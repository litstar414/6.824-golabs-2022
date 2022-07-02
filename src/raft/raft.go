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
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	Command interface{}
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
	// Start index for this term
	termStartIndex int
	// The log start index (for 2D)
	lastIncludedIndex int
	lastIncludedTerm  int

	state     state
	lastReset time.Time
	applyCh   chan ApplyMsg

	cond *sync.Cond
}

// To invoke this function, lock should be held
// If term gets updated(gets bigger), the votedFor will be updated
func (rf *Raft) checkTerm(term int) bool {
	if term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = term
		data := rf.persist()
		rf.persister.SaveRaftState(data)
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
	data := rf.persist()
	rf.persister.SaveRaftState(data)
	rf.broadcastRV()
}

// Do we need lock here?
func (rf *Raft) GetStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// Invoke this function should held the lock
// Before return from this function, the lock should be held
func (rf *Raft) leaderInitialize() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.termStartIndex = len(rf.log) + rf.lastIncludedIndex
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
		rf.matchIndex[i] = 0
	}
	MyDebug(dCommit, "S%d after leader initialize nextIndex:%v", rf.me, rf.nextIndex)
	rf.broadcastAE()
	rf.lastReset = time.Now()
	go rf.updateLeaderCommit(rf.currentTerm)
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
// Invoke under lock
func (rf *Raft) persist() []byte {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	for i := range rf.log {
		if i == 0 {
			continue
		}
		e.Encode(rf.log[i])
	}
	data := w.Bytes()
	//rf.persister.SaveRaftState(data)
	MyDebug(dPersist, "S%d stores state->currentTerm:%d votedFor:%d log:%v lastIncludedIndex:%v",
		rf.me, rf.currentTerm, rf.votedFor, rf.log, rf.lastIncludedIndex)

	return data
}

//
// restore previously persisted state.
//
// We should probably run this code after initialization
func (rf *Raft) readPersist(data []byte) (int, int) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		MyDebug(dPersist, "S%d no previous information found from persist", rf.me)
		return 0, 0
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		// error occurs
		MyDebug(dPersist, "S%d reads from persister failed", rf.me)
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
	}

	// Tries to decode the log from the decoder
	var entry LogEntry
	var e error
	for e = d.Decode(&entry); e == nil; e = d.Decode(&entry) {
		// No error occurs
		rf.log = append(rf.log, entry)
	}

	// Error occurs
	if e == io.EOF {
		MyDebug(dPersist, "S%d restore finished with state currentTerm:%d votedFor:%d rf.log:%v lastIncludedIndex:%v",
			rf.me, rf.currentTerm, rf.votedFor, rf.log, rf.lastIncludedIndex)
	} else {
		// Error occurs
		MyDebug(dPersist, "S%d reads from persister failed with error:%v", rf.me, e)
	}
	return lastIncludedIndex, lastIncludedTerm
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
	MyDebug(dTrace, "S%d tries to acquire the lock in start", rf.me)
	rf.mu.Lock()
	MyDebug(dTrace, "S%d acquired the lock in start", rf.me)
	defer MyDebug(dTrace, "S%d releases the lock in start", rf.me)
	defer rf.mu.Unlock()

	term = rf.currentTerm
	// Check leader
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}

	// Append entry to local log
	c := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}

	rf.log = append(rf.log, c)
	data := rf.persist()
	rf.persister.SaveRaftState(data)
	index = rf.lastIncludedIndex + len(rf.log) - 1

	MyDebug(dLog, "S%d Leader new command received:%v in term %d", rf.me, command, rf.currentTerm)
	// Start synchronize
	rf.broadcastAE()

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
		MyDebug(dTrace, "S%d acquired lock in main thread", rf.me)
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
		rf.mu.Unlock()
		MyDebug(dTrace, "S%d releases the lock in main thread", rf.me)

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// The sleep time should based on the identity of the server
	}
}

func (rf *Raft) sendCommands(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		// Wait for update
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait()
		}

		lastApplied := rf.lastApplied

		Entries := make([]LogEntry, 0)
		// There is a race conditon, in the initial snapshot, the lastApplied is set to 0, but lastIncludedIndex is set to its original value
		Entries = append(Entries, rf.log[rf.lastApplied+1-rf.lastIncludedIndex:rf.commitIndex+1-rf.lastIncludedIndex]...)

		rf.mu.Unlock()
		for i := range Entries {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      Entries[i].Command,
				CommandIndex: lastApplied + 1,
			}
			applyCh <- msg
			MyDebug(dCommit, "S%d applies %d with index%d", rf.me, msg.Command, msg.CommandIndex)
			lastApplied += 1
		}

		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, lastApplied)
		rf.mu.Unlock()
	}

}

func (rf *Raft) updateLeaderCommit(term int) {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		MyDebug(dTrace, "S%d tries to acquire lock in updateLeaderCommit thread", rf.me)
		rf.mu.Lock()
		MyDebug(dTrace, "S%d acquired lock in updateLeaderCommit thread", rf.me)
		if rf.currentTerm != term {
			rf.mu.Unlock()
			MyDebug(dTrace, "S%d releases the lock in updateLeaderCommit thread", rf.me)
			return
		}
		// Still leader, try to update leaderCommit
		// We must select an entry that has the same term with us.
		// commitIndex = 0
		if rf.commitIndex >= rf.termStartIndex {
			// Then try leaderCommit + 1
			if rf.checkIfCommit(rf.commitIndex + 1) {
				rf.commitIndex += 1
				rf.cond.Signal()
			}
		} else {
			// Try termStartIndex
			// What if termStartIndex not exist
			if rf.checkIfCommit(rf.termStartIndex) {
				rf.commitIndex = rf.termStartIndex
				rf.cond.Signal()
			}
		}
		rf.mu.Unlock()
		MyDebug(dTrace, "S%d releases the lock in updateLeaderCommit thread", rf.me)
	}
}

// Check if index has committed or not.
// Invoke under lock
func (rf *Raft) checkIfCommit(index int) bool {
	majority := len(rf.peers)/2 + 1
	count := 0
	for i := range rf.matchIndex {
		if i != rf.me {
			if rf.matchIndex[i] >= index {
				count += 1
			}
		} else {
			// self
			if len(rf.log)-1+rf.lastIncludedIndex >= index {
				count += 1
			}
		}
	}
	if count >= majority {
		return true
	}
	return false
}

// Invoke on lock
func (rf *Raft) ResetLog(ori []LogEntry, left, right int) {
	temp := make([]LogEntry, 0)
	temp = append(temp, LogEntry{Command: -1, Term: 0})
	if ori != nil {
		if left == -1 {
			left = 0
		}
		if right == -1 {
			right = len(ori)
		}
		if right >= left {
			temp = append(temp, ori[left:right]...)
		}
	}
	rf.log = temp
}

// Invoke under lock
func (rf *Raft) findRelatedTerm(index int) int {
	var term int
	if index == rf.lastIncludedIndex {
		term = rf.lastIncludedTerm
	} else if index < rf.lastIncludedIndex {
		panic("Invalid index found in snapshot")
	} else {
		term = rf.log[index-rf.lastIncludedIndex].Term
	}
	return term
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	// We use lazy allocation -> only allocate this space when needed
	rf.nextIndex = nil
	rf.matchIndex = nil
	// Use append to add new entries into the log
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{
		Command: -1,
		Term:    0,
	})
	rf.state = Follower
	// Set the default value for lastIncludedIndex
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.lastReset = time.Now()
	rf.cond = sync.NewCond(&rf.mu)

	MyDebug(dSnap, "S%d restarts", rf.me)

	// 2C reads from persist storage if there are any
	// initialize from state persisted before a crash
	lastIncludedIndex, lastIncludedTerm := rf.readPersist(persister.ReadRaftState())
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastApplied = lastIncludedIndex
	// Potentially set lastApplied and commitIndex to 0

	rf.readPersistSnapshot(rf.persister.ReadSnapshot(), applyCh, lastIncludedIndex, lastIncludedTerm)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendCommands(applyCh)
	//go rf.sendCommandToQ()
	//go rf.applyFromQ(applyCh)

	return rf
}

// restore previously persisted snapshot
// Can only be called in the start of make
func (rf *Raft) readPersistSnapshot(data []byte, applyCh chan ApplyMsg, lastIncludedIndex, lastIncludedTerm int) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		MyDebug(dSnap, "S%d no previous information found for snapshot", rf.me)
		return
	}
	go func() {
		msg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      data,
			SnapshotTerm:  lastIncludedTerm,
			SnapshotIndex: lastIncludedIndex,
		}
		// Reset the sentIndex
		applyCh <- msg
		MyDebug(dSnap, "S%d reboots and sends a snapshot to service", rf.me)
	}()
}
