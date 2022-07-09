package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

type OPTYPE int

const (
	GET       OPTYPE = 1
	PUTAPPEND OPTYPE = 2
	PUT       OPTYPE = 3
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	client_id int
	seq_num   int
}

const RETRY_INTERVAL = 100

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.client_id = int(nrand())
	ck.seq_num = 0
	return ck
}

func (ck *Clerk) sendCommands(fname string, args interface{}, opt OPTYPE, key string) interface{} {
	var err Err
	var result interface{}
	//WARNING: ok reuse

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				if opt == GET {
					a := args.(GetArgs)
					r := GetReply{}
					ok = srv.Call(fname, &a, &r)
					err = r.Err
					result = r
				} else if opt == PUTAPPEND {
					a := args.(PutAppendArgs)
					r := PutAppendReply{}
					ok = srv.Call(fname, &a, &r)
					err = r.Err
					result = r
				}
				if ok && (err == OK || err == ErrNoKey) {
					return result
				}

				if ok && err == ErrWrongGroup {
					break
				}

				// ErrWrongLeader
				if ok && err == ErrWrongLeader {
					time.Sleep(RETRY_INTERVAL * time.Millisecond)
				}
				// Not ok, retry immediately
			}
		}
		time.Sleep(RETRY_INTERVAL * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.Client_id = ck.client_id
	args.Seq_num = ck.seq_num

	ck.seq_num += 1

	r := ck.sendCommands("ShardKV.Get", args, GET, key)
	reply := r.(GetReply)

	DPrintf("client[%d] get(%s)=%s", ck.client_id, key, reply.Value)
	return reply.Value
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		Client_id: ck.client_id,
		Seq_num:   ck.seq_num,
	}

	ck.seq_num += 1
	ck.sendCommands("ShardKV.PutAppend", args, PUTAPPEND, key)

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
