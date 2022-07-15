package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type status int

const (
	POLLING      status = 1 //indicate that the shard is needed to be pulled
	POLLED       status = 2 //indicate that the polling of this shard has completed and the group can now begin to provide services
	NOTAVAILABLE status = 3 //indicate that the current shard is not available and we are waiting for the group leader to poll it
	SERVICE      status = 4 //indicate that the current shard can provide service
)

type OpOperation int

const (
	NormalOp    OpOperation = 1
	NewConfig   OpOperation = 2
	InsertShard OpOperation = 3
	DeleteShard OpOperation = 4
)

const CONFIG_INTERVAL = 80

//TODO: Add currentConfig to the snapshot
//TODO: Add shardStatus to the snapshot
//TODO: Add shardTable to the snapshot
//TODO: change the design of the snapshot
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

type Shard struct {
	State map[string]string
	// The last seen request from the client related to this shard
	LastSeenTable map[int]Pair
	ConfigNum     int
}

func generateBlankShard(configN int) Shard {
	s := Shard{
		State:         make(map[string]string),
		LastSeenTable: make(map[int]Pair),
		ConfigNum:     configN,
	}
	return s
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OperationType OpOperation
	// Field for NewConfig
	NewConfig shardctrler.Config
	NewShard  Shard
	NewShardN int

	Client_id int
	Seq_num   int
	Key       string
	Value     string
	Opt       OPTYPE
}

type ShardKV struct {
	sm           *shardctrler.Clerk
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32
	//lastSeenTable map[int]Pair
	notifyCh    map[int]WaitCh
	lastApplied int
	//state       map[string]string

	currentConfig shardctrler.Config
	lastConfig    shardctrler.Config

	shardStatus map[int]status
	shardTable  map[int]Shard
	restart     bool
}

// Read lock should be held for this function
func (kv *ShardKV) testIfCanReConfig() bool {
	for _, v := range kv.shardStatus {
		if v != SERVICE {
			return false
		}
	}
	return true
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
func (kv *ShardKV) checkIfResponsible(key string) Err {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	shard := key2shard(key)
	if kv.currentConfig.Shards[shard] == kv.gid && kv.shardStatus[shard] == SERVICE {
		return OK
	}
	if kv.currentConfig.Shards[shard] != kv.gid {
		return ErrWrongGroup
	}

	return ErrWait
}

func copyShard(s Shard) Shard {
	shard := Shard{
		State:         make(map[string]string),
		LastSeenTable: make(map[int]Pair),
		ConfigNum:     s.ConfigNum,
	}

	for k, v := range s.State {
		shard.State[k] = v
	}

	for k, v := range s.LastSeenTable {
		shard.LastSeenTable[k] = v
	}
	return shard
}

func (kv *ShardKV) checkShardStatus(shard int) status {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.shardStatus[shard]
}

func makeNilShard() Shard {
	return Shard{
		ConfigNum:     -1,
		LastSeenTable: make(map[int]Pair),
		State:         make(map[string]string),
	}
}

//TODO: decide whether it is safe to not polling from the leader
func (kv *ShardKV) PollRequest(args *PollArgs, reply *PollReply) {
	DPrintf("server[%d:%d] pollrequest get called for shard in config%d:%d", kv.gid, kv.me, args.Shard, args.ConfigN)
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	reply.Shard = makeNilShard()

	if args.ConfigN > kv.currentConfig.Num {
		DPrintf("server[%d:%d] pollrequest not updated config number, args.ConfigN:%d, my configN:%d", kv.gid, kv.me, args.ConfigN, kv.currentConfig.Num)
		reply.Err = ErrWait
		return
	}

	shard, ok := kv.shardTable[args.Shard]
	// TODO: will later become work with deleteShard
	if !ok {
		DPrintf("server[%d:%d] receives a stale pollrequest with request info: %v", kv.gid, kv.me, *args)
		reply.Err = TimeOut
		return
	}
	if shard.ConfigNum != args.ConfigN-1 {
		DPrintf("server[%d:%d] receives a stale pollrequest", kv.gid, kv.me)
		reply.Err = TimeOut
		return
	}

	// Success
	reply.Err = OK
	reply.Shard = copyShard(shard)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	//TODO: re-check the implementation of the check
	//TODO: we need to seperate ErrWrongGroup and ErrWait
	//TODO: For ErrWait, it indicates that we have that shard, but shard not available
	if err := kv.checkIfResponsible(args.Key); err != OK {
		reply.Err = err
		return
	}

	result := kv.checkIfDuplicate(args.Key, args.Client_id, args.Seq_num)
	if result != nil {
		t := result.(Result)
		reply.Value = t.Value
		reply.Err = t.Err
		return
	}
	// If not, extract the arguments, generate a Op operation.
	op := Op{
		OperationType: NormalOp,
		Client_id:     args.Client_id,
		Seq_num:       args.Seq_num,
		Key:           args.Key,
		Opt:           GET,
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
		reply.Value = res.Value
		reply.Err = res.Err
		kv.unregisterWaitCh(args.Client_id)
		return
	case <-time.After(500 * time.Millisecond):
		reply.Err = TimeOut
		kv.unregisterWaitCh(args.Client_id)
		return
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if err := kv.checkIfResponsible(args.Key); err != OK {
		reply.Err = err
		return
	}

	result := kv.checkIfDuplicate(args.Key, args.Client_id, args.Seq_num)
	if result != nil {
		t := result.(Result)
		reply.Err = t.Err
		return
	}
	// If not, extract the arguments, generate a Op operation.
	op := Op{
		OperationType: NormalOp,
		Client_id:     args.Client_id,
		Seq_num:       args.Seq_num,
		Key:           args.Key,
		Value:         args.Value,
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
		//DPrintf("server[%d] put[%s]:%s", kv.me, op.Key, op.Value)
	} else {
		//DPrintf("server[%d] append[%s]:%s", kv.me, op.Key, op.Value)
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
		//DPrintf("server[%d] fails to reach majority", kv.me)
		reply.Err = TimeOut
		kv.unregisterWaitCh(args.Client_id)
		return
	}
}

//TODO:change the snapshot
func (kv *ShardKV) encodeSnapshot() []byte {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastApplied)
	e.Encode(kv.shardStatus)
	e.Encode(kv.shardTable)
	e.Encode(kv.lastConfig)
	e.Encode(kv.currentConfig)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) decodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		//DPrintf("server[%d]: receive null snapshot", kv.me)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var shardTable map[int]Shard
	var shardStatus map[int]status
	var lastConfig shardctrler.Config
	var currentConfig shardctrler.Config

	if d.Decode(&lastApplied) != nil ||
		d.Decode(&shardStatus) != nil ||
		d.Decode(&shardTable) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&currentConfig) != nil {
		DPrintf("server[%d]: decode snapshot errors", kv.me)
	} else {
		DPrintf("server[%d:%d] applied snapshot with shardStatus:%v, currentConfig:%v", kv.gid, kv.me, shardStatus, currentConfig)
		kv.lastApplied = lastApplied
		if kv.restart {
			kv.restart = false
			for k, v := range shardStatus {
				if v == SERVICE {
					kv.shardStatus[k] = SERVICE
				} else if v == POLLING {
					kv.shardStatus[k] = NOTAVAILABLE
				} else {
					kv.shardStatus[k] = v
				}
			}
		} else {
			kv.shardStatus = shardStatus
		}
		kv.shardTable = shardTable
		kv.lastConfig = lastConfig
		kv.currentConfig = currentConfig
	}
}

//TODO: Decide whether we want to use the lock
// The op must be a verified operation
func (kv *ShardKV) applyOp(op Op) Result {
	r := Result{}
	r.Opt = op.Opt
	shard := key2shard(op.Key)
	switch op.Opt {
	case GET:
		value, ok := kv.shardTable[shard].State[op.Key]
		if !ok {
			r.Err = ErrNoKey
			r.Value = ""
		} else {
			r.Err = OK
			r.Value = value
		}
	case PUTAPPEND:
		value, ok := kv.shardTable[shard].State[op.Key]
		if !ok {
			// New key
			kv.shardTable[shard].State[op.Key] = op.Value
			r.Err = ErrNoKey
		} else {
			kv.shardTable[shard].State[op.Key] = value + op.Value
			r.Err = OK
		}
	case PUT:
		kv.shardTable[shard].State[op.Key] = op.Value
		r.Err = OK
	}
	return r
}

func (kv *ShardKV) checkIfDuplicate(key string, cid, seq_num int) interface{} {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(key)
	pair, ok := kv.shardTable[shard].LastSeenTable[cid]
	if !ok {
		// This is the first request from the client
		return nil
	}
	if seq_num == pair.Seq_num {
		return pair.Result
	} else if seq_num < pair.Seq_num {
		panic("Unexpected seq_num")
	}
	//DPrintf("server[%d] encounters a duplicate request cid:%d seq:%d", kv.me, cid, seq_num)
	return nil
}

func (kv *ShardKV) monitorApplyCh() {
	for kv.killed() == false {
		// Keep reading from the applyCh
		msg := <-kv.applyCh

		if !msg.CommandValid {
			// Test if it is a snapshot
			kv.handleSnapshotMsg(msg)
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
			//DPrintf("snapshot[%d] sends snapshot to raft", kv.me)
			snapshot := kv.encodeSnapshot()
			kv.rf.Snapshot(kv.lastApplied, snapshot)
		}

		// Is a valid command, get the op from it
		op := msg.Command.(Op)
		//DPrintf("server[%d] op received in applyCh:%v", kv.me, op)
		// Increase the lastApplied
		kv.lastApplied = msg.CommandIndex
		switch op.OperationType {
		case NormalOp:
			kv.handleNormalOp(op)
			continue
		case NewConfig:
			kv.handleNewConfig(op)
			continue
		case InsertShard:
			DPrintf("server[%d:%d] received insertshard", kv.gid, kv.me)
			kv.handleInsertShard(op)
			continue
		}
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

// If this function fails at any means, change the shard status to NOTAVAILABLE if it is not service
func (kv *ShardKV) pollShard(shard, configN int, servers []string) {
	for kv.killed() == false {
		_, isleader := kv.rf.GetState()
		if !isleader {
			// RETURN
			kv.mu.Lock()
			if kv.shardStatus[shard] != SERVICE {
				kv.shardStatus[shard] = NOTAVAILABLE
			}
			kv.mu.Unlock()
			return
		}

		args := PollArgs{
			Shard:   shard,
			ConfigN: configN,
		}
		reply := PollReply{}

		DPrintf("server[%d:%d] begins to poll shard %d", kv.gid, kv.me, shard)
		// I am the leader, poll the shard!
		for {
			// Keep polling until the shardstatus is correct
			if kv.checkShardStatus(shard) == SERVICE {
				return
			}
			for si := 0; si < len(servers); si++ {
				if kv.checkShardStatus(shard) == SERVICE {
					return
				}
				srv := kv.make_end(servers[si])
				ok := srv.Call("ShardKV.PollRequest", &args, &reply)
				if ok {
					if reply.Err == OK {
						DPrintf("server[%d:%d] remote rpc pollrequest successes", kv.gid, kv.me)
						// We have polled a shard successfully
						// Call start
						op := Op{
							OperationType: InsertShard,
							NewShard:      copyShard(reply.Shard),
							NewShardN:     shard,
						}
						_, _, leader := kv.rf.Start(op)
						kv.mu.Lock()
						if !leader {
							if kv.shardStatus[shard] != SERVICE {
								kv.shardStatus[shard] = NOTAVAILABLE
							}
						}
						kv.mu.Unlock()
						time.Sleep(100 * time.Millisecond)
					} else if reply.Err == ErrWait {
						// The remote config is not updated
						time.Sleep(100 * time.Millisecond)
					} else {
						return
					}
				} else {
					DPrintf("server[%d:%d] remote rpc pollrequest fails", kv.gid, kv.me)
				}
			}
		}
	}
}

func (kv *ShardKV) monitorShards() {
	for kv.killed() == false {
		// Only the leader is responsible for polling the shards
		_, leader := kv.rf.GetState()
		if !leader {
			time.Sleep(CONFIG_INTERVAL * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		for k, v := range kv.shardStatus {
			if v == NOTAVAILABLE {
				// Spawn a new thread to poll it, this thread should ensure that it will poll the shard for at least once
				gid := kv.lastConfig.Shards[k]
				go kv.pollShard(k, kv.currentConfig.Num, kv.lastConfig.Groups[gid])
				kv.shardStatus[k] = POLLING
			}
		}
		kv.mu.Unlock()
		time.Sleep(RETRY_INTERVAL * time.Millisecond)
	}
}

func (kv *ShardKV) updateConfig() {
	for kv.killed() == false {
		// Only the leader is responsible for updating the config
		_, leader := kv.rf.GetState()
		if !leader {
			time.Sleep(CONFIG_INTERVAL * time.Millisecond)
			continue
		}

		kv.mu.Lock()
		// Check if we are now doing some reconfiguration
		if !kv.testIfCanReConfig() {
			kv.mu.Unlock()
			time.Sleep(CONFIG_INTERVAL * time.Millisecond)
			continue
		}
		c := kv.sm.Query(kv.currentConfig.Num + 1)
		if c.Num == kv.currentConfig.Num+1 {
			// We have successfully see a new config
			// change currentConfig and lastConfig correspondingly
			// Generate an reconfiguration op and send it to the applyCh
			op := Op{
				OperationType: NewConfig,
				NewConfig:     c,
			}
			_, _, isLeader := kv.rf.Start(op)
			if !isLeader {
				kv.mu.Unlock()
				time.Sleep(CONFIG_INTERVAL * time.Millisecond)
				continue
			}
		}
		kv.mu.Unlock()
		time.Sleep(CONFIG_INTERVAL * time.Millisecond)
	}
}

//Add here

// Read Lock required
func (kv *ShardKV) checkIfCanService(shard int) bool {
	if _, ok := kv.shardStatus[shard]; ok {
		if kv.shardStatus[shard] == SERVICE {
			return true
		}
	}
	return false
}

// TODO: Problem here
func (kv *ShardKV) handleNormalOp(op Op) {
	// We should first check if we are responsible for this request
	shard := key2shard(op.Key)
	err := kv.checkIfResponsible(op.Key)
	res := Result{}
	res.Opt = op.Opt
	if err == OK {
		if kv.checkIfDuplicate(op.Key, op.Client_id, op.Seq_num) != nil {
			return
		}

		DPrintf("server[%d:%d] normal op received key:%v value:%v opt:%v", kv.gid, kv.me, op.Key, op.Value, op.Opt)
		res = kv.applyOp(op)
		pair := Pair{
			Seq_num: op.Seq_num,
			Result:  res,
		}
		kv.mu.Lock()
		kv.shardTable[shard].LastSeenTable[op.Client_id] = pair
	} else if err == ErrWrongGroup {
		res.Err = ErrWrongGroup
		kv.mu.Lock()
	} else {
		// ErrWait
		res.Err = ErrWait
		kv.mu.Lock()
	}
	waitCh, ok := kv.notifyCh[op.Client_id]
	if ok && waitCh.Seq_num == op.Seq_num {
		select {
		case kv.notifyCh[op.Client_id].Ch <- res:
		case <-time.After(250 * time.Millisecond):
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
}

// Return the gained shards, no-change shards, and the lost shards
func (kv *ShardKV) calculateShardChanges(previousConfig shardctrler.Config, after shardctrler.Config) ([]int, []int, []int) {
	gainedShards := make([]int, 0)
	maintainedShards := make([]int, 0)
	lostShards := make([]int, 0)
	// My gid:
	for i := 0; i < shardctrler.NShards; i++ {
		if after.Shards[i] == kv.gid {
			if previousConfig.Shards[i] == kv.gid {
				maintainedShards = append(maintainedShards, i)
			} else {
				gainedShards = append(gainedShards, i)
			}
		} else {
			if previousConfig.Shards[i] == kv.gid {
				lostShards = append(lostShards, i)
			}
		}
	}
	return gainedShards, maintainedShards, lostShards
}

func (kv *ShardKV) handleInsertShard(op Op) {
	// We have to check again about the shard status
	shard := op.NewShard
	shardN := op.NewShardN

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Do we already get this shard?
	if kv.shardStatus[shardN] == SERVICE {
		return
	}

	// Is it for the currentConfig?
	if shard.ConfigNum+1 != kv.currentConfig.Num {
		DPrintf("server[%d:%d] receives a stale shard", kv.gid, kv.me)
		return
	}

	// install the shard
	myshard := copyShard(shard)
	myshard.ConfigNum = kv.currentConfig.Num
	kv.shardTable[shardN] = myshard
	kv.shardStatus[shardN] = SERVICE
	DPrintf("server[%d:%d] install shard %d in config %d", kv.gid, kv.me, shardN, kv.currentConfig.Num)
}

// No lock acquired for invoking this function
func (kv *ShardKV) handleNewConfig(op Op) {
	c := op.NewConfig
	// We only try to apply new configs we have seen
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.currentConfig.Num >= c.Num {
		// This is a duplicate config, we don't need to do anything
		return
	}
	if c.Num != kv.currentConfig.Num+1 {
		fmt.Printf("Unexpected config number, expected:%d, got:%d\n", kv.currentConfig.Num+1, c.Num)
	}

	DPrintf("server[%d:%d] new config received", kv.gid, kv.me)
	if !kv.testIfCanReConfig() {
		fmt.Printf("Unexpected reconfig, shard status:%v", kv.shardStatus)
		panic("The last reconfiguration has not finished yet")
	}

	kv.lastConfig = kv.currentConfig
	kv.currentConfig = c

	// Update shard table and delete table
	gainedShards, maintainedShards, deleteShards := kv.calculateShardChanges(kv.lastConfig, kv.currentConfig)

	for _, v := range deleteShards {
		delete(kv.shardStatus, v)
	}

	for _, v := range maintainedShards {
		if entry, ok := kv.shardTable[v]; ok {
			entry.ConfigNum = kv.currentConfig.Num
			kv.shardTable[v] = entry
		}
	}

	for _, v := range gainedShards {
		if kv.lastConfig.Num == 0 {
			kv.shardStatus[v] = SERVICE
			// Generate shards
			kv.shardTable[v] = generateBlankShard(kv.currentConfig.Num)
		} else {
			kv.shardStatus[v] = NOTAVAILABLE
		}
	}

	DPrintf("server[%d:%d] after reconfig(%d):%v", kv.gid, kv.me, c.Num, kv.shardStatus)
}

func (kv *ShardKV) handleSnapshotMsg(msg raft.ApplyMsg) {
	if !msg.SnapshotValid {
		panic("Unexpected applyMsg")
	}

	//DPrintf("snapshot[%d] received snapshot from raft with index:%d, compared to lastApplied:%v", kv.me, msg.SnapshotIndex, kv.lastApplied)
	// Only apply the snapshot if we see a bigger index
	if msg.SnapshotIndex <= kv.lastApplied {
		return
	}
	kv.mu.Lock()
	kv.decodeSnapshot(msg.Snapshot)
	//DPrintf("snapshot[%d] decode finished", kv.me)
	kv.mu.Unlock()
	// Send command back to raft module
	kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
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
	labgob.Register(Shard{})
	labgob.Register(Pair{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(Task{})

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

	//kv.lastSeenTable = make(map[int]Pair)
	kv.notifyCh = make(map[int]WaitCh)
	kv.lastApplied = 0
	//kv.state = make(map[string]string)

	kv.currentConfig = kv.sm.Query(0)
	kv.lastConfig = kv.sm.Query(0)
	kv.shardStatus = make(map[int]status)
	kv.shardTable = make(map[int]Shard)
	kv.restart = true

	go kv.monitorApplyCh()
	go kv.updateConfig()
	go kv.monitorShards()
	return kv
}
