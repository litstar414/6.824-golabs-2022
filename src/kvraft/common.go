package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	TimeOut        = "TimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	Client_id int
	Seq_num   int
	// otherwise RPC will break.
}

type Pair struct {
	Seq_num int
	Result  interface{}
}

type PutAppendReply struct {
	Err Err
}

type WaitCh struct {
	Ch      chan Result
	Seq_num int
}

type GetArgs struct {
	Key       string
	Client_id int
	Seq_num   int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
