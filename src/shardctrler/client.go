package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId  int
	client_id int
	seq_num   int
}

type OPTYPE int

const (
	JOIN  OPTYPE = 1
	LEAVE OPTYPE = 2
	MOVE  OPTYPE = 3
	QUERY OPTYPE = 4
)

const RETRY_INTERVAL = 100

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leaderId = 0
	ck.client_id = int(nrand())
	ck.seq_num = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{
		Num:       num,
		Client_id: ck.client_id,
		Seq_num:   ck.seq_num,
	}

	ck.seq_num += 1
	r := ck.sendCommands("ShardCtrler.Query", args, QUERY)
	reply := r.(QueryReply)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.Client_id = ck.client_id
	args.Seq_num = ck.seq_num

	ck.seq_num += 1

	ck.sendCommands("ShardCtrler.Join", args, JOIN)
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.Client_id = ck.client_id
	args.Seq_num = ck.seq_num

	ck.seq_num += 1
	ck.sendCommands("ShardCtrler.Leave", args, LEAVE)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.Client_id = ck.client_id
	args.Seq_num = ck.seq_num

	ck.seq_num += 1

	ck.sendCommands("ShardCtrler.Move", args, MOVE)
}

func (ck *Clerk) sendCommands(fname string, args interface{}, opt OPTYPE) interface{} {
	var ok bool
	var err Err
	var result interface{}

	for {
		if opt == JOIN {
			a := args.(JoinArgs)
			r := JoinReply{}
			ok = ck.servers[ck.leaderId].Call(fname, &a, &r)
			err = r.Err
			result = r
		} else if opt == LEAVE {
			a := args.(LeaveArgs)
			r := LeaveReply{}
			ok = ck.servers[ck.leaderId].Call(fname, &a, &r)
			err = r.Err
			result = r
		} else if opt == MOVE {
			a := args.(MoveArgs)
			r := MoveReply{}
			ok = ck.servers[ck.leaderId].Call(fname, &a, &r)
			err = r.Err
			result = r
		} else if opt == QUERY {
			a := args.(QueryArgs)
			r := QueryReply{}
			ok = ck.servers[ck.leaderId].Call(fname, &a, &r)
			err = r.Err
			result = r
		}

		if ok == true && err == OK {
			return result
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(RETRY_INTERVAL * time.Millisecond)
	}
}
