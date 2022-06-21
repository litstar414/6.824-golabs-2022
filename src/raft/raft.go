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

type LogEntry struct {
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

	state      state
	resetTimer chan int
	lastReset  time.Time
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// The sleep time should based on the identity of the server
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

	rf.resetTimer = make(chan int)
	rf.lastReset = time.Now()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
