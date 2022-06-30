package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
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
}

func (kv *KVServer) registerWaitCh(cid int, ch chan Result) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.notifyCh[cid] = ch
}

// Get rpc handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// First check if the request is duplicate
	result := kv.checkIfDuplicate(args.Client_id, args.Seq_num)
	if result != nil {
		t := result.(GetReply)
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
	res := <-waitCh
	// Return
	reply.Value = res.Value
	reply.Err = res.Err
}

// PutAppend handler
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	result := kv.checkIfDuplicate(args.Client_id, args.Seq_num)
	if result != nil {
		t := result.(PutAppendReply)
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
	// Wait on a channel
	waitCh := make(chan Result)
	kv.registerWaitCh(args.Client_id, waitCh)
	res := <-waitCh
	// Return
	reply.Err = res.Err
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
	return kv
}
