package shardkv

import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "6.824/shardctrler"
import "log"
import "sync/atomic"
import "bytes"
import "fmt"
import "time"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) registerWaitCh(cid int, seq_num int, ch chan Result) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.notifyCh[cid] = WaitCh{
		Seq_num: seq_num,
		Ch:      ch,
	}
}

func (kv *ShardKV) unregisterWaitCh(cid int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.notifyCh[cid]
	if ok {
		delete(kv.notifyCh, cid)
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client_id int
	Seq_num   int
	Key       string
	Value     string
	Opt       OPTYPE
}

type ShardKV struct {
	sm           *shardctrler.Clerk
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead          int32
	lastSeenTable map[int]Pair
	//notifyCh      map[int](chan Result)
	notifyCh    map[int]WaitCh
	lastApplied int
	state       map[string]string

	currentConfig shardctrler.Config
	configLock    sync.Mutex

	// TODO: decides whether we want to store the currentConfig
	// TODO: decides whether we want to encode the currentConfig in the snapshot
}

func (kv *ShardKV) testIfNeedSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}

	stateSize := kv.rf.GetStateSize()

	if stateSize >= kv.maxraftstate {
		return true
	}
	return false
}

// TODO: This check is not good, needs to be changed.
// TODO: Consider the situation, we see fast configuration change.
// now we are responsible for a shard, but we have not install its snapshot
func (kv *ShardKV) checkIfResponsible(key string) bool {
	kv.configLock.Lock()
	defer kv.configLock.Unlock()

	shard := key2shard(key)
	if kv.currentConfig.Shards[shard] == kv.gid {
		return true
	}
	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	//TODO: Check if we can serve this request, based on the current configuration
	//TODO: This check should held the lock, as there should be another thread tries to update it

	if !kv.checkIfResponsible(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	result := kv.checkIfDuplicate(args.Client_id, args.Seq_num)
	if result != nil {
		t := result.(Result)
		reply.Value = t.Value
		reply.Err = t.Err
		return
	}
	// If not, extract the arguments, generate a Op operation.
	op := Op{
		Client_id: args.Client_id,
		Seq_num:   args.Seq_num,
		Key:       args.Key,
		Opt:       GET,
	}
	// Call Start()
	_, _, isLeader := kv.rf.Start(op)
	// Handle error case in start
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// Wait on a channel
	waitCh := make(chan Result)
	kv.registerWaitCh(args.Client_id, args.Seq_num, waitCh)
	// Yes, we need timeout here, otherwise, if we ends up in the minority
	// and the leader believes that it is the leader, then we cannot progress
	// We need timeout to pass test in TestOnePartition3A
	select {
	case res := <-waitCh:
		if res.Opt != GET {
			panic("Get rpc get put result")
		}
		DPrintf("server[%d] get(%s)=%s", kv.me, args.Key, res.Value)
		reply.Value = res.Value
		reply.Err = res.Err
		kv.unregisterWaitCh(args.Client_id)
		return
	case <-time.After(500 * time.Millisecond):
		DPrintf("server[%d] fails to reach majority", kv.me)
		reply.Err = TimeOut
		kv.unregisterWaitCh(args.Client_id)
		return
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.checkIfResponsible(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	result := kv.checkIfDuplicate(args.Client_id, args.Seq_num)
	if result != nil {
		t := result.(Result)
		reply.Err = t.Err
		return
	}
	// If not, extract the arguments, generate a Op operation.
	op := Op{
		Client_id: args.Client_id,
		Seq_num:   args.Seq_num,
		Key:       args.Key,
		Value:     args.Value,
	}
	if args.Op == "Put" {
		op.Opt = PUT
	} else {
		op.Opt = PUTAPPEND
	}
	// Call Start()
	_, _, isLeader := kv.rf.Start(op)
	// Handle error case in start
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if op.Opt == PUT {
		DPrintf("server[%d] put[%s]:%s", kv.me, op.Key, op.Value)
	} else {
		DPrintf("server[%d] append[%s]:%s", kv.me, op.Key, op.Value)
	}
	// Wait on a channel
	waitCh := make(chan Result)
	kv.registerWaitCh(args.Client_id, args.Seq_num, waitCh)
	select {
	case res := <-waitCh:
		if res.Opt != PUT && res.Opt != PUTAPPEND {
			panic("put rpc get get result")
		}
		reply.Err = res.Err
		kv.unregisterWaitCh(args.Client_id)
		return
	case <-time.After(500 * time.Millisecond):
		DPrintf("server[%d] fails to reach majority", kv.me)
		reply.Err = TimeOut
		kv.unregisterWaitCh(args.Client_id)
		return
	}
}

func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastApplied)
	e.Encode(kv.state)
	e.Encode(kv.lastSeenTable)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) decodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		DPrintf("server[%d]: receive null snapshot", kv.me)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastViewedTable map[int]Pair
	var lastApplied int
	var state map[string]string

	if d.Decode(&lastApplied) != nil ||
		d.Decode(&state) != nil ||
		d.Decode(&lastViewedTable) != nil {
		DPrintf("server[%d]: decode snapshot errors", kv.me)
	} else {
		kv.lastApplied = lastApplied
		kv.lastSeenTable = lastViewedTable
		kv.state = state
	}
}

// The op must be a verified operation
func (kv *ShardKV) applyOp(op Op) Result {
	r := Result{}
	r.Opt = op.Opt
	switch op.Opt {
	case GET:
		value, ok := kv.state[op.Key]
		if !ok {
			r.Err = ErrNoKey
			r.Value = ""
		} else {
			r.Err = OK
			r.Value = value
		}
	case PUTAPPEND:
		value, ok := kv.state[op.Key]
		if !ok {
			// New key
			kv.state[op.Key] = op.Value
			r.Err = ErrNoKey
		} else {
			kv.state[op.Key] = value + op.Value
			r.Err = OK
		}
	case PUT:
		kv.state[op.Key] = op.Value
		r.Err = OK
	}
	return r
}

func (kv *ShardKV) checkIfDuplicate(cid, seq_num int) interface{} {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	pair, ok := kv.lastSeenTable[cid]
	if !ok {
		// This is the first request from the client
		return nil
	}
	if seq_num == pair.Seq_num {
		return pair.Result
	} else if seq_num < pair.Seq_num {
		panic("Unexpected seq_num")
	}
	DPrintf("server[%d] encounters a duplicate request cid:%d seq:%d", kv.me, cid, seq_num)
	return nil
}

func (kv *ShardKV) monitorApplyCh() {
	for kv.killed() == false {
		// Keep reading from the applyCh
		msg := <-kv.applyCh

		if !msg.CommandValid {
			// Test if it is a snapshot
			if !msg.SnapshotValid {
				panic("Unexpected applyMsg")
			}

			DPrintf("snapshot[%d] received snapshot from raft with index:%d, compared to lastApplied:%v", kv.me, msg.SnapshotIndex, kv.lastApplied)
			// Only apply the snapshot if we see a bigger index
			if msg.SnapshotIndex <= kv.lastApplied {
				continue
			}
			kv.mu.Lock()
			DPrintf("snapshot[%d] before->lastApplied:%d, state:%v, lastSeenTable:%v", kv.me, kv.lastApplied, kv.state, kv.lastSeenTable)
			kv.decodeSnapshot(msg.Snapshot)
			DPrintf("snapshot[%d] decode finished", kv.me)
			DPrintf("snapshot[%d] after->lastApplied:%d, state:%v, lastSeenTable:%v", kv.me, kv.lastApplied, kv.state, kv.lastSeenTable)
			kv.mu.Unlock()
			// Send command back to raft module
			kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
			continue
		}

		// We might have already exectued it
		if msg.CommandIndex <= kv.lastApplied {
			continue
		}

		if msg.CommandIndex != kv.lastApplied+1 {
			fmt.Printf("server[%d] expected:%d, get:%d", kv.me, kv.lastApplied+1, msg.CommandIndex)
			panic("Unexpected command order")
		}

		// Let's test for snapshot here
		if kv.testIfNeedSnapshot() {
			// Call snapshot function, need to first encode it
			// as lastSeenTable is only modified in this thread, it is safe to read it without holding the lock
			DPrintf("snapshot[%d] sends snapshot to raft", kv.me)
			snapshot := kv.encodeSnapshot()
			kv.rf.Snapshot(kv.lastApplied, snapshot)
		}

		// Is a valid command, get the op from it
		op := msg.Command.(Op)
		DPrintf("server[%d] op received in applyCh:%v", kv.me, op)

		// Increase the lastApplied
		kv.lastApplied = msg.CommandIndex
		// Duplicate log?
		if kv.checkIfDuplicate(op.Client_id, op.Seq_num) != nil {
			DPrintf("server[%d] in monitorApplyCh", kv.me)
			continue
		}

		// Not duplicate, apply the op and record the result
		res := kv.applyOp(op)
		pair := Pair{
			Seq_num: op.Seq_num,
			Result:  res,
		}

		kv.mu.Lock()
		kv.lastSeenTable[op.Client_id] = pair
		// When we finally tries to send the command back to the client, is it still the command the client is expecting?
		waitCh, ok := kv.notifyCh[op.Client_id]
		if ok && waitCh.Seq_num == op.Seq_num {
			select {
			case kv.notifyCh[op.Client_id].Ch <- res:
			case <-time.After(250 * time.Millisecond):
				kv.mu.Unlock()
				continue
			}
		}
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) updateConfig() {
	for kv.killed() == false {
		c := kv.sm.Query(-1)
		kv.configLock.Lock()
		if c.Num > kv.currentConfig.Num {
			kv.currentConfig = c
		}
		kv.configLock.Unlock()
		time.Sleep(80 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Result{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sm = shardctrler.MakeClerk(ctrlers)

	kv.lastSeenTable = make(map[int]Pair)
	kv.notifyCh = make(map[int]WaitCh)
	kv.lastApplied = 0
	kv.state = make(map[string]string)

	kv.currentConfig = kv.sm.Query(-1)

	go kv.monitorApplyCh()
	go kv.updateConfig()
	return kv
}
