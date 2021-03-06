package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	TimeOut        = "TImeOut"
	ErrWait        = "Wait"
)

type Result struct {
	Value string
	Err   Err
	Opt   OPTYPE
}

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client_id int
	Seq_num   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Client_id int
	Seq_num   int
}

type GetReply struct {
	Err   Err
	Value string
}

type PollArgs struct {
	Shard   int
	ConfigN int
}

type PollReply struct {
	Err   Err
	Shard Shard
}

type Pair struct {
	Seq_num int
	Result  interface{}
}

type WaitCh struct {
	Ch      chan Result
	Seq_num int
}
