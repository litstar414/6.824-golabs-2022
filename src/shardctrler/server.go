package shardctrler

import (
	"bytes"
	"fmt"
	"log"
	"sort"
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

const SNAP_THREASH int = 3

func (sc *ShardCtrler) registerWaitCh(cid int, seq_num int, ch chan Result) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.notifyCh[cid] = WaitCh{
		Seq_num: seq_num,
		Ch:      ch,
	}
}

func (sc *ShardCtrler) unregisterWaitCh(cid int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, ok := sc.notifyCh[cid]
	if ok {
		delete(sc.notifyCh, cid)
	}
}

func (sc *ShardCtrler) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.lastApplied)
	e.Encode(sc.configs)
	e.Encode(sc.lastSeenTable)
	data := w.Bytes()
	return data
}
func (sc *ShardCtrler) checkIfDuplicate(cid, seq_num int) interface{} {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	pair, ok := sc.lastSeenTable[cid]
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

func printConfig(c *Config) {
	// map from gid -> shards
	t := make(map[int]int)
	unused := 0
	for i := 0; i < NShards; i++ {
		if c.Shards[i] == 0 {
			unused += 1
		} else {
			gid := c.Shards[i]
			if _, ok := t[gid]; ok {
				t[gid] = t[gid] + 1
			} else {
				t[gid] = 1
			}
		}
	}
	for k := range c.Groups {
		if _, ok := t[k]; !ok {
			// we add it
			t[k] = 0
		}
	}

	keystring := ""
	valuestring := ""
	count := 0
	for k, v := range t {
		keystring += fmt.Sprint(k)
		valuestring += fmt.Sprint(v)
		count += 1
		if count == len(t) {
			break
		}
		keystring += "\t"
		valuestring += "\t"
	}
	if Debug {
		fmt.Printf("From gid -> assigned shards\n")
		fmt.Printf("====================================\n")
		fmt.Println(keystring)
		fmt.Println(valuestring)
		fmt.Printf("Unassigned shards:%d\n", unused)
		fmt.Printf("====================================\n")
	}
}

func removeServer(gid int, config *Config) {
	// First reset Shards to zero
	for i := 0; i < NShards; i++ {
		if config.Shards[i] == gid {
			config.Shards[i] = 0
		}
	}

	// Now delete it
	delete(config.Groups, gid)
}

// Return smallestIndex, biggestIndex
func findSmallestBiggest(distribution map[int]int, gids []int) (int, int) {
	smallestIndex := -1
	smallestValue := 100000

	biggestIndex := -1
	biggestValue := -1

	for _, k := range gids {
		// k -> gid
		if distribution[k] > biggestValue {
			biggestValue = distribution[k]
			biggestIndex = k
		}
		if distribution[k] < smallestValue {
			smallestValue = distribution[k]
			smallestIndex = k
		}
	}
	return smallestIndex, biggestIndex
}

func findSmallestAvailableNum(servers map[int][]string) int {
	i := 0
	for ; i < len(servers); i++ {
		if _, ok := servers[i]; !ok {
			return i
		}
	}

	return i
}

func reconfiguration(JoinedServers map[int][]string, LeftServers []int, origConfig Config) Config {
	c := copyConfig(origConfig)
	c.Num += 1
	if JoinedServers != nil {
		for k, v := range JoinedServers {
			// If the current GID is in the servers, we need to assign a new one
			if _, ok := c.Groups[k]; ok {
				c.Groups[findSmallestAvailableNum(c.Groups)] = v
			} else {
				c.Groups[k] = v
			}
		}
	}
	if LeftServers != nil {
		for i := 0; i < len(LeftServers); i++ {
			removeServer(LeftServers[i], &c)
		}
	}

	if len(c.Groups) == 0 {
		DPrintf("Zero groups now, return")
		return c
	}

	DPrintf("Before rebalance:")
	printConfig(&c)

	// Now we need to rebalance it
	t := make(map[int]int)
	unassigned := make([]int, 0)
	// Deterministic step
	for i := 0; i < NShards; i++ {
		if c.Shards[i] == 0 {
			unassigned = append(unassigned, i)
		} else {
			gid := c.Shards[i]
			if _, ok := t[gid]; ok {
				t[gid] = t[gid] + 1
			} else {
				t[gid] = 1
			}
		}
	}

	// Not deterministic
	gids := make([]int, 0)
	for k, _ := range c.Groups {
		gids = append(gids, k)
	}
	sort.Ints(gids)

	for i, k := range gids {
		if _, ok := t[k]; !ok {
			t[k] = 0
		}
		if i == 0 {
			t[k] += len(unassigned)
			for i := 0; i < len(unassigned); i++ {
				c.Shards[unassigned[i]] = k
			}
			unassigned = unassigned[0:0]
		}
	}

	if len(c.Groups) == 1 {
		return c
	}

	totalGroups := len(c.Groups)
	// We have "plusone" groups need to take NShards / totalGroups + 1 shards
	plusOne := NShards % totalGroups
	// We have "normal" groups need to take NShards / totalGroups shards
	//normal := totalGroups - plusOne

	if plusOne == 0 {
		plusOne = 0
	} else {
		plusOne = 1
	}

	for {
		smallestIndex, biggestIndex := findSmallestBiggest(t, gids)
		diff := t[biggestIndex] - t[smallestIndex]
		if diff == plusOne {
			break
		} else {
			// Move one from server biggestIndex to smallestIndex
			moveFromGtoG(biggestIndex, smallestIndex, &c)
			t[biggestIndex] -= 1
			t[smallestIndex] += 1
		}
	}

	DPrintf("After reconfiguration:")
	printConfig(&c)

	return c
}

// Move one shard from group 1 to group 2
func moveFromGtoG(g1, g2 int, config *Config) {
	// Find a shard that belongs to g1
	for i := 0; i < NShards; i++ {
		if config.Shards[i] == g1 {
			moveTo(i, g2, config)
			return
		}
	}
}

func findUnbalancedServer(distribution map[int]int, threashold int) int {
	// Firstly find a server with less than threashold, it cannot, find one with less than threashold + 1
	smallestIndex := 0
	smallestValue := 100000
	for k, v := range distribution {
		if v < threashold {
			return k
		}
		if v < smallestValue {
			smallestValue = v
			smallestIndex = k
		}
	}
	return smallestIndex
}

func moveTo(shard int, gid int, origConfig *Config) {
	origConfig.Shards[shard] = gid
}

func copySlice(strings []string) []string {
	t := make([]string, len(strings))
	for i := 0; i < len(strings); i++ {
		t[i] = strings[i]
	}
	return t
}

//This function will not change the config number.
//The invoker should change it by themselves
func copyConfig(config Config) Config {
	c := Config{}
	c.Num = config.Num

	var shards [NShards]int
	for i := 0; i < NShards; i++ {
		shards[i] = config.Shards[i]
	}

	groups := make(map[int][]string)
	for k, v := range config.Groups {
		groups[k] = copySlice(v)
	}

	c.Shards = shards
	c.Groups = groups
	return c
}

func (sc *ShardCtrler) applyOp(op Op) Result {
	r := Result{}
	r.Opt = op.Opt
	r.Err = OK
	lastConfig := sc.configs[len(sc.configs)-1]
	switch op.Opt {
	case JOIN:
		c := reconfiguration(op.JoinServers, nil, lastConfig)
		sc.configs = append(sc.configs, c)
	case LEAVE:
		c := reconfiguration(nil, op.LeaveGroups, lastConfig)
		sc.configs = append(sc.configs, c)
	case QUERY:
		if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) {
			r.Value = lastConfig
		} else {
			r.Value = sc.configs[op.QueryNum]
		}
	case MOVE:
		c := copyConfig(lastConfig)
		c.Num += 1
		moveTo(op.FromShard, op.ToGroup, &c)
		sc.configs = append(sc.configs, c)
	}
	return r
}

func (sc *ShardCtrler) testIfNeedSnapshot() bool {
	if sc.maxraftstate == -1 {
		return false
	}

	stateSize := sc.rf.GetStateSize()

	if stateSize >= sc.maxraftstate || sc.maxraftstate-stateSize <= SNAP_THREASH {
		return true
	}
	return false
}

func (sc *ShardCtrler) decodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		//DPrintf("server[%d]: receive null snapshot", sc.me)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastViewedTable map[int]Pair
	var lastApplied int
	var configs []Config

	if d.Decode(&lastApplied) != nil ||
		d.Decode(&configs) != nil ||
		d.Decode(&lastViewedTable) != nil {
		//DPrintf("server[%d]: decode snapshot errors", sc.me)
	} else {
		sc.lastApplied = lastApplied
		sc.lastSeenTable = lastViewedTable
		sc.configs = configs
	}
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	maxraftstate int
	dead         int32

	// "state" of the machine
	configs       []Config // indexed by config num
	lastSeenTable map[int]Pair
	notifyCh      map[int]WaitCh
	lastApplied   int
}

type Op struct {
	// Your data here.
	Client_id   int
	Seq_num     int
	JoinServers map[int][]string
	LeaveGroups []int
	FromShard   int
	ToGroup     int
	QueryNum    int
	Opt         OPTYPE
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	result := sc.checkIfDuplicate(args.Client_id, args.Seq_num)
	if result != nil {
		t := result.(Result)
		reply.Err = t.Err
		return
	}
	// If not, extract the arguments, generate a Op operation.
	op := Op{
		Client_id:   args.Client_id,
		Seq_num:     args.Seq_num,
		JoinServers: args.Servers,
		Opt:         JOIN,
	}
	// Call Start()
	_, _, isLeader := sc.rf.Start(op)
	// Handle error case in start
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// Wait on a channel
	waitCh := make(chan Result)
	sc.registerWaitCh(args.Client_id, args.Seq_num, waitCh)
	// Yes, we need timeout here, otherwise, if we ends up in the minority
	// and the leader believes that it is the leader, then we cannot progress
	// We need timeout to pass test in TestOnePartition3A
	select {
	case res := <-waitCh:
		if res.Opt != JOIN {
			panic("Incorrect result obtained")
		}
		reply.Err = res.Err
		sc.unregisterWaitCh(args.Client_id)
		return
	case <-time.After(500 * time.Millisecond):
		reply.Err = TimeOut
		sc.unregisterWaitCh(args.Client_id)
		return
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	result := sc.checkIfDuplicate(args.Client_id, args.Seq_num)
	if result != nil {
		t := result.(Result)
		reply.Err = t.Err
		return
	}
	// If not, extract the arguments, generate a Op operation.
	op := Op{
		Client_id:   args.Client_id,
		Seq_num:     args.Seq_num,
		LeaveGroups: args.GIDs,
		Opt:         LEAVE,
	}
	// Call Start()
	_, _, isLeader := sc.rf.Start(op)
	// Handle error case in start
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// Wait on a channel
	waitCh := make(chan Result)
	sc.registerWaitCh(args.Client_id, args.Seq_num, waitCh)
	// Yes, we need timeout here, otherwise, if we ends up in the minority
	// and the leader believes that it is the leader, then we cannot progress
	// We need timeout to pass test in TestOnePartition3A
	select {
	case res := <-waitCh:
		if res.Opt != LEAVE {
			panic("Incorrect result obtained")
		}
		reply.Err = res.Err
		sc.unregisterWaitCh(args.Client_id)
		return
	case <-time.After(500 * time.Millisecond):
		reply.Err = TimeOut
		sc.unregisterWaitCh(args.Client_id)
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// Your code here.
	result := sc.checkIfDuplicate(args.Client_id, args.Seq_num)
	if result != nil {
		t := result.(Result)
		reply.Err = t.Err
		return
	}
	// If not, extract the arguments, generate a Op operation.
	op := Op{
		Client_id: args.Client_id,
		Seq_num:   args.Seq_num,
		FromShard: args.Shard,
		ToGroup:   args.GID,
		Opt:       MOVE,
	}
	// Call Start()
	_, _, isLeader := sc.rf.Start(op)
	// Handle error case in start
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// Wait on a channel
	waitCh := make(chan Result)
	sc.registerWaitCh(args.Client_id, args.Seq_num, waitCh)
	// Yes, we need timeout here, otherwise, if we ends up in the minority
	// and the leader believes that it is the leader, then we cannot progress
	// We need timeout to pass test in TestOnePartition3A
	select {
	case res := <-waitCh:
		if res.Opt != MOVE {
			panic("Incorrect result obtained")
		}
		reply.Err = res.Err
		sc.unregisterWaitCh(args.Client_id)
		return
	case <-time.After(500 * time.Millisecond):
		reply.Err = TimeOut
		sc.unregisterWaitCh(args.Client_id)
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	result := sc.checkIfDuplicate(args.Client_id, args.Seq_num)
	if result != nil {
		t := result.(Result)
		reply.Err = t.Err
		reply.Config = t.Value
		return
	}
	// If not, extract the arguments, generate a Op operation.
	op := Op{
		Client_id: args.Client_id,
		Seq_num:   args.Seq_num,
		QueryNum:  args.Num,
		Opt:       QUERY,
	}
	// Call Start()
	_, _, isLeader := sc.rf.Start(op)
	// Handle error case in start
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// Wait on a channel
	waitCh := make(chan Result)
	sc.registerWaitCh(args.Client_id, args.Seq_num, waitCh)
	// Yes, we need timeout here, otherwise, if we ends up in the minority
	// and the leader believes that it is the leader, then we cannot progress
	// We need timeout to pass test in TestOnePartition3A
	select {
	case res := <-waitCh:
		if res.Opt != QUERY {
			panic("Incorrect result obtained")
		}
		reply.Err = res.Err
		reply.Config = res.Value
		sc.unregisterWaitCh(args.Client_id)
		return
	case <-time.After(500 * time.Millisecond):
		reply.Err = TimeOut
		sc.unregisterWaitCh(args.Client_id)
		return
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.maxraftstate = 10

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}

	labgob.Register(Op{})
	labgob.Register(Result{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastSeenTable = make(map[int]Pair)
	sc.notifyCh = make(map[int]WaitCh)
	sc.lastApplied = 0

	go sc.monitorApplyCh()
	return sc
}

func (sc *ShardCtrler) monitorApplyCh() {
	for !sc.killed() {
		// Keep reading from the applyCh
		msg := <-sc.applyCh

		if !msg.CommandValid {
			// Test if it is a snapshot
			if !msg.SnapshotValid {
				panic("Unexpected applyMsg")
			}

			// Only apply the snapshot if we see a bigger index
			if msg.SnapshotIndex <= sc.lastApplied {
				continue
			}
			sc.mu.Lock()
			sc.decodeSnapshot(msg.Snapshot)
			sc.mu.Unlock()
			// Send command back to raft module
			sc.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
			continue
		}

		// We might have already exectued it
		if msg.CommandIndex <= sc.lastApplied {
			continue
		}

		if msg.CommandIndex != sc.lastApplied+1 {
			panic("Unexpected command order")
		}

		// Let's test for snapshot here
		if sc.testIfNeedSnapshot() {
			// Call snapshot function, need to first encode it
			// as lastSeenTable is only modified in this thread, it is safe to read it without holding the lock
			//DPrintf("snapshot[%d] sends snapshot to raft", sc.me)
			snapshot := sc.encodeSnapshot()
			sc.rf.Snapshot(sc.lastApplied, snapshot)
		}

		// Is a valid command, get the op from it
		op := msg.Command.(Op)
		DPrintf("server[%d] op received in applyCh:%v", sc.me, op)

		// Increase the lastApplied
		sc.lastApplied = msg.CommandIndex
		// Duplicate log?
		if sc.checkIfDuplicate(op.Client_id, op.Seq_num) != nil {
			continue
		}

		// Not duplicate, apply the op and record the result
		res := sc.applyOp(op)
		pair := Pair{
			Seq_num: op.Seq_num,
			Result:  res,
		}

		sc.mu.Lock()
		sc.lastSeenTable[op.Client_id] = pair
		// When we finally tries to send the command back to the client, is it still the command the client is expecting?
		waitCh, ok := sc.notifyCh[op.Client_id]
		if ok && waitCh.Seq_num == op.Seq_num {
			select {
			case sc.notifyCh[op.Client_id].Ch <- res:
			case <-time.After(250 * time.Millisecond):
				sc.mu.Unlock()
				continue
			}
		}
		sc.mu.Unlock()
	}
}
