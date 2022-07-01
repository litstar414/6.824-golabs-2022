package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
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

type Result struct {
	Value string
	Err   Err
	Opt   OPTYPE
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastSeenTable map[int]Pair
	notifyCh      map[int](chan Result)
	lastApplied   int
	state         map[string]string
}

// Invoked on a single thread, so no lock required
// The op must be a verified operation
func (kv *KVServer) applyOp(op Op) Result {
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

func (kv *KVServer) monitorApplyCh() {
	for kv.killed() == false {
		// Keep reading from the applyCh
		msg := <-kv.applyCh

		if !msg.CommandValid {
			// TODO: temporally ignore here
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

		// Is a valid command, get the op from it
		op := msg.Command.(Op)
		DPrintf("server[%d] op received in applyCh:%v", kv.me, op)

		// Duplicate log?
		if kv.checkIfDuplicate(op.Client_id, op.Seq_num) != nil {
			DPrintf("server[%d] in monitorApplyCh", kv.me)
			kv.lastApplied = msg.CommandIndex
			continue
		}

		// Not duplicate, apply the op and record the result
		res := kv.applyOp(op)
		pair := Pair{
			Seq_num: op.Seq_num,
			Result:  res,
		}

		// Increase the lastApplied
		kv.lastApplied = msg.CommandIndex

		kv.mu.Lock()
		kv.lastSeenTable[op.Client_id] = pair
		// When we finally tries to send the command back to the client, is it still the command the client is expecting?
		// Consider the case, the original client sends the request to the minority leader, and register a command
		_, ok := kv.notifyCh[op.Client_id]
		if ok {
			select {
			case kv.notifyCh[op.Client_id] <- res:
			case <-time.After(250 * time.Millisecond):
				kv.mu.Unlock()
				continue
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) registerWaitCh(cid int, ch chan Result) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.notifyCh[cid] = ch
}

func (kv *KVServer) unregisterWaitCh(cid int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.notifyCh[cid]
	if ok {
		delete(kv.notifyCh, cid)
	}
}

// Get rpc handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// First check if the request is duplicate
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
	// TODO: decide whether we need these two arguments
	_, _, isLeader := kv.rf.Start(op)
	// Handle error case in start
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// Wait on a channel
	waitCh := make(chan Result)
	kv.registerWaitCh(args.Client_id, waitCh)
	// Yes, we need timeout here, otherwise, if we ends up in the minority
	// and the leader believes that it is the leader, then we cannot progress
	// We need timeout to pass test in TestOnePartition3A
	select {
	case res := <-waitCh:
		if res.Opt != GET {
			fmt.Printf("Bad Opt:%d", res.Opt)
			panic("Get rpc get Other result")
		}
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

// PutAppend handler
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("server[%d] put command received:%v", kv.me, args)
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
	// TODO: decide whether we need these two arguments
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
	kv.registerWaitCh(args.Client_id, waitCh)
	//DPrintf("client[%d] wait channel registerred", op.Client_id)
	select {
	case res := <-waitCh:
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

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// If duplicate, return the result.
// Otherwise, return nil
func (kv *KVServer) checkIfDuplicate(cid, seq_num int) interface{} {
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

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.lastSeenTable = make(map[int]Pair)
	kv.notifyCh = make(map[int](chan Result))
	kv.lastApplied = 0
	kv.state = make(map[string]string)

	go kv.monitorApplyCh()
	return kv
}
