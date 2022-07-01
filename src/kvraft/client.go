package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type OPTYPE int

const (
	GET       OPTYPE = 1
	PUTAPPEND OPTYPE = 2
	PUT       OPTYPE = 3
)

/*
Some important notes:
The clerk is attached to the application, all the calls to the get/put/append function
are not happening in parallel.  These functions are not rpcs
*/

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	// Record the latest leaderId we have seen
	leaderId int
	// client_id is the id of the clerk, used with seq_num to identify a request
	client_id int
	// Monotonically increasing, assign to all the operations that are not
	// idempotent so that they are executed exactly-once
	seq_num int
}

const RETRY_INTERVAL = 150

// Generate a random number
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// Create a clerk which can communicate with client
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.client_id = int(nrand())
	ck.seq_num = 0
	return ck
}

func (ck *Clerk) sendCommands(fname string, args interface{}, opt OPTYPE) interface{} {
	var ok bool
	var err Err
	var result interface{}

	for {
		if opt == GET {
			a := args.(GetArgs)
			r := GetReply{}
			DPrintf("client[%d]: call with arg:%v", ck.client_id, a)
			ok = ck.servers[ck.leaderId].Call(fname, &a, &r)
			err = r.Err
			result = r
		} else if opt == PUTAPPEND {
			a := args.(PutAppendArgs)
			r := PutAppendReply{}
			DPrintf("client[%d]: call with arg:%v", ck.client_id, a)
			ok = ck.servers[ck.leaderId].Call(fname, &a, &r)
			err = r.Err
			result = r
		}

		if ok == true && (err == OK || err == ErrNoKey) {
			return result
		}
		if !ok {
			DPrintf("client[%d]: call fails", ck.client_id)
		} else {
			DPrintf("client[%d]: Encounter error:%s", ck.client_id, err)
		}

		DPrintf("client[%d] original lid:%d", ck.client_id, ck.leaderId)

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		DPrintf("client[%d] unsuccess operation, try server:%d", ck.client_id, ck.leaderId)
		time.Sleep(RETRY_INTERVAL * time.Millisecond)
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// Generate GetArgs
	// Idealy, we don't need to assign seq_num for get requests
	args := GetArgs{
		Key:       key,
		Client_id: ck.client_id,
		Seq_num:   ck.seq_num,
	}
	ck.seq_num += 1

	r := ck.sendCommands("KVServer.Get", args, GET)
	reply := r.(GetReply)

	DPrintf("client[%d] get(%s)=%s", ck.client_id, key, reply.Value)
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		Client_id: ck.client_id,
		Seq_num:   ck.seq_num,
	}

	ck.seq_num += 1
	ck.sendCommands("KVServer.PutAppend", args, PUTAPPEND)

	if op == "Put" {
		DPrintf("client[%d] put(%s)=%s", ck.client_id, key, value)
	} else {
		DPrintf("client[%d] append(%s)=%s", ck.client_id, key, value)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
