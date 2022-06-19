package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

type state int

const (
	Follower  state = 0
	Candidate state = 1
	Leader    state = 2
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int
var debugLock sync.Mutex

func debugInit() {
	debugLock.Lock()
	defer debugLock.Unlock()
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func MyDebug(topic logTopic, format string, a ...interface{}) {
	debugLock.Lock()
	defer debugLock.Unlock()

	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Each log entry stores a state machine command along with the
// term number when the entry was received by the leader
// Each log also has an integer index identifying its position in the log.
type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	// -1 indicates that not voted in this round.
	votedFor int

	log []LogEntry
	// volatile states on all servers
	commitIndex int
	lastApplied int

	// leader states that are reinitialized after election.
	nextIndex  []int
	matchIndex []int

	state     state
	lastReset time.Time
}

// Invoke under lock
func (rf *Raft) broadcastAE() {
	if rf.state != Leader {
		return
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// otherwise, send messages
		go func(server int, term int) {
			// held the lock
			args := AppendEntryArgs{
				Term:     term,
				LeaderId: rf.me,
			}
			reply := AppendEntryReply{}
			MyDebug(dLeader, "S%d Leader sends HB to server %d in term %d", rf.me, server, term)
			ok := rf.sendAppendEntry(server, &args, &reply)
			if ok {
				// handle the result
				// Always check the term
				MyDebug(dTrace, "S%d tries to acquire the lock in MHAE", rf.me)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				defer MyDebug(dTrace, "S%d release the lock in MHAE", rf.me)

				if rf.checkTerm(reply.Term) {
					return
				}
				if reply.Term != term {
					// Stale information
					return
				}
				// We always expect a success from peers in 2A
			}
		}(i, rf.currentTerm)
	}
}

// Invoke this function should held the lock
func (rf *Raft) broadcastRV() {
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		// TODO: implement LastLogIndex and LastLogTerm
	}

	done := false
	count := 1
	var lock sync.Mutex

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// Send rpc to other servers
		go func(server int) {
			reply := RequestVoteReply{}
			MyDebug(dTimer, "S%d sends request to S%d", rf.me, server)
			ok := rf.sendRequestVote(server, &args, &reply)
			MyDebug(dTimer, "S%d received a result from S%d", rf.me, server)
			if ok {
				// Check the term
				MyDebug(dTrace, "S%d tries to acquire the lock in MHRV", rf.me)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				defer MyDebug(dTrace, "S%d release the lock in MHRV", rf.me)

				lock.Lock()
				defer lock.Unlock()

				if rf.checkTerm(reply.Term) {
					// We failed to become the leader
					done = true
					return
				}
				if done {
					// We have already become the leader or failed
					return
				}
				if reply.VoteGranted {
					count += 1
					MyDebug(dVote, "S%d Candidate, received vote from S%d in term %d, total count %d", rf.me, server, rf.currentTerm, count)
					if count > len(rf.peers)/2 {
						if rf.state == Candidate && rf.currentTerm == args.Term {
							// This is a valid reply, we become as the leader
							MyDebug(dLeader, "S%d Leader, has become leader in term %d", rf.me, rf.currentTerm)
							rf.state = Leader
							done = true
							rf.leaderInitialize()
						}
					}
				}
			}
		}(i)
	}

}

// To invoke this function, lock should be held
// If term gets updated(gets bigger), the votedFor will be updated
func (rf *Raft) checkTerm(term int) bool {
	if term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = term
		if rf.state != Follower {
			MyDebug(dTimer, "S%d sees a bigger term %d, revert to follower", rf.me, term)
			rf.state = Follower
			rf.lastReset = time.Now()
			// This instruction causes problem
			//rf.resetTimer <- 1
		}
		return true
	}
	return false
}

// Invoke this function should held the lock
// Before return from this function, the lock should be held
func (rf *Raft) converToCandidate() {
	rf.state = Candidate
	rf.lastReset = time.Now()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.broadcastRV()
}

// Invoke this function should held the lock
// Before return from this function, the lock should be held
func (rf *Raft) leaderInitialize() {
	rf.broadcastAE()
	rf.lastReset = time.Now()
	// re-initialize nextIndex and matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

// Let's say the leader sends the heartbeats at interval 120ms
// And the timeout time is 300-500ms
const LOWEST_INTERVAL = 500

// Return a value in range [LOWEST_INTERVAL, LOWEST_INTERVAL + 200)
func (rf *Raft) getRandomValue() int32 {
	if rf.state == Leader {
		return 120
	}
	// Generate a new seed each time this is accessed
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Int31n(200) + LOWEST_INTERVAL
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int

	Entries      []LogEntry
	LeaderCommit int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

// AEhandler
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	MyDebug(dTrace, "S%d tries to acquire lock in AE handler", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer MyDebug(dTrace, "S%d release the lock in AE handler", rf.me)

	rf.checkTerm(args.Term)
	if args.Term < rf.currentTerm {
		MyDebug(dTimer, "S%d received a stale appendEntires from old leader", rf.me)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// If u are candidate, then after receiving messages from new leader, convert to follower
	if rf.state == Candidate {
		MyDebug(dTimer, "S%d Candidate, received AE, convert back to follower", rf.me)
		rf.state = Follower
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.lastReset = time.Now()
	} else {
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.lastReset = time.Now()
		MyDebug(dTimer, "S%d Follower, received AE, reset timer", rf.me)
	}

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	MyDebug(dTrace, "S%d tries to acquire lock in RV handler", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer MyDebug(dTrace, "S%d release the lock in RV handler", rf.me)

	rf.checkTerm(args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		MyDebug(dVote, "S%d reject vote request from S%d in term %d", rf.me, args.CandidateId, rf.currentTerm)
		reply.VoteGranted = false
		return
	}

	// TODO: compare logs

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		MyDebug(dVote, "S%d Follower votes for S%d in term %d", rf.me, args.CandidateId, rf.currentTerm)
		// the resetTimer channel blocks here
		rf.lastReset = time.Now()
		return
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	MyDebug(dTrace, "S%d tries to acquire the lock in Start", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer MyDebug(dTrace, "S%d release the lock in Start", rf.me)

	// First check if me is the leader or not
	if rf.state != Leader {
		// The index and term only has meaning when it's the leader
		return -1, -1, false
	}
	// Generate the LogEntry
	log := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   len(rf.log),
	}

	// Append to its own log
	rf.log = append(rf.log, log)

	// Reset the timer for leader as we are about to send messages
	rf.lastReset = time.Now()
	rf.broadcastAE()

	// Return immediately from the function, don't wait for results
	return len(rf.log) - 1, rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const SLEEP_INTERVAL = 5

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// If we are the leader, we can simply ignore this function, otherwise we do other things.
func (rf *Raft) ticker() {
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().

	// The sleep time should based on the identity of the server

	for rf.killed() == false {

		time.Sleep(SLEEP_INTERVAL * time.Millisecond)
		MyDebug(dTrace, "S%d tries to acquire lock in main thread", rf.me)
		rf.mu.Lock()
		switch rf.state {
		case Follower:
			if time.Since(rf.lastReset).Milliseconds() > int64(rf.getRandomValue()) {
				MyDebug(dTimer, "S%d Follower, timesout become Candidate in term %d", rf.me, rf.currentTerm+1)
				rf.converToCandidate()
			}
		case Candidate:
			if time.Since(rf.lastReset).Milliseconds() > int64(rf.getRandomValue()) {
				MyDebug(dTimer, "S%d Candidate, timesout Candidate again in term %d", rf.me, rf.currentTerm+1)
				rf.converToCandidate()
			}
		case Leader:
			if time.Since(rf.lastReset).Milliseconds() > 120 {
				rf.lastReset = time.Now()
				MyDebug(dTimer, "S%d Leader, sends heartbeats in term %d", rf.me, rf.currentTerm)
				rf.broadcastAE()
			}
		}
		MyDebug(dTrace, "S%d releases the lock in main thread", rf.me)
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	debugInit()
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	// We use lazy allocation -> only allocate this space when needed
	rf.nextIndex = nil
	rf.matchIndex = nil
	// Use append to add new entries into the log
	rf.log = make([]LogEntry, 0)
	rf.state = Follower

	rf.lastReset = time.Now()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
