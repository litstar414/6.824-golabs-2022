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
	nextIndex []int
	// If matchIndex[i] >= 1, then log from [1, matchIndex[i]] are committed
	// Otherwise, no log are committed
	matchIndex []int

	state     state
	lastReset time.Time

	termStartIndex int
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
		// We should prepare all the arguments here.
		var entries []LogEntry
		prevLogIndex := 0
		prevLogTerm := 0
		leaderCommit := -1

		// When we delete elements in the log, we must also change its length.
		// What if nextIndex[i] is 0?
		if rf.nextIndex[i] > len(rf.log) {
			// we dont have anything new to send out.
			entries = nil
		} else {
			// Generate a slice which contains the log that needs to be appended
			// XXX: This may be dangerous as we are using slice and the slice may change
			entries = rf.log[rf.nextIndex[i]-1:]
		}

		// Setup prevLogIndex and prevLogTerm
		// If we only have one element, then set them to 0
		if rf.nextIndex[i] != 1 {
			prevLogIndex = rf.log[rf.nextIndex[i]-2].Index
			prevLogTerm = rf.log[rf.nextIndex[i]-2].Term
		}

		leaderCommit = rf.commitIndex

		// Next step
		go func(server int, term int, entries []LogEntry, prevLogIndex, prevLogTerm, leaderCommit int) {
			// held the lock
			args := AppendEntryArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			reply := AppendEntryReply{}
			if args.Entries == nil {

				MyDebug(dLeader, "S%d Leader sends HB to server %d in term %d", rf.me, server, term)
			} else {
				MyDebug(dLog, "S%d Leader sends AE to server %d in term %d %v", rf.me, server, term, entries)
			}
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
				if reply.Success {
					// If successful: update nextIndex and matchIndex for follower
					// However, we need to take care if this message is a heartbeat message
					MyDebug(dLog, "S%d Leader received success from previous AE for server %d in term %d", rf.me, server, term)
					if args.Entries == nil {
						// This success simply means that prevLogIndex and prevLogTerm matches.
						rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex)
						// Don't update nextIndex
					} else {
						//rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						// Match index simply won't go back in the same term
						rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
						// XXX: Update only if we does not observe a change in the nextIndex[server]
						if rf.nextIndex[server] == args.PrevLogIndex+1 {
							// We do not want to go back
							rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
						}
					}
				} else {
					// Decrement nextIndex and retry
					// Ensure we does not see any changes in the nextIndex
					if rf.nextIndex[server] == args.PrevLogIndex+1 {
            MyDebug(dLog, "S%d Leader received suggested(%d) Term:%d Next:%d", rf.me, server, reply.ConflictTerm, reply.Next)
						if reply.Next == -1 {
							rf.nextIndex[server] -= 1
            MyDebug(dLog, "S%d Leader received suggested(%d) -1 to %d", rf.me, server, rf.nextIndex[server])
						} else {
							if reply.ConflictTerm == -1 {
								rf.nextIndex[server] = reply.Next
                MyDebug(dLog, "S%d Leader received suggested(%d) Next to %d", rf.me, server, rf.nextIndex[server])
							} else {
								// Do we have term in our log?
								temp := args.PrevLogIndex
								for ; rf.log[temp-1].Term > reply.ConflictTerm && temp > 1; temp-- {
								}
								if rf.log[temp-1].Term == reply.ConflictTerm {
									rf.nextIndex[server] = temp
                  MyDebug(dLog, "S%d Leader received suggested(%d) temp to %d", rf.me, server, temp)
								} else {
                  MyDebug(dLog, "S%d Leader received suggested(%d) next2 to %d", rf.me, server, reply.Next)
									rf.nextIndex[server] = reply.Next
								}
							}
						}
					}
				}
			}
		}(i, rf.currentTerm, entries, prevLogIndex, prevLogTerm, leaderCommit)
	}
}

// Invoke this function should held the lock
func (rf *Raft) broadcastRV() {
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		// Might be -1
		LastLogIndex: len(rf.log),
	}
	if args.LastLogIndex == 0 {
		args.LastLogTerm = 0
	} else {
		args.LastLogTerm = rf.log[args.LastLogIndex-1].Term
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
			MyDebug(dTimer, "S%d sends request to S%d with arg:%v", rf.me, server, args)
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
			if len(rf.log) >= index {
				count += 1
			}
		}
	}
	if count >= majority {
		return true
	}
	return false
}

func (rf *Raft) updateLeaderCommit() {
	for rf.killed() == false {
		time.Sleep(30 * time.Millisecond)
		MyDebug(dTrace, "S%d tries to acquire lock in updateLeaderCommit thread", rf.me)
		rf.mu.Lock()
		if rf.state != Leader {
			MyDebug(dTrace, "S%d releases the lock in main thread", rf.me)
			rf.mu.Unlock()
			return
		}
		// Still leader, try to update leaderCommit
		// We must select an entry that has the same term with us.
		if rf.commitIndex >= rf.termStartIndex {
			// Then try leaderCommit + 1
			if rf.checkIfCommit(rf.commitIndex + 1) {
				rf.commitIndex += 1
			}
		} else {
			// Try termStartIndex
			if rf.checkIfCommit(rf.termStartIndex) {
				rf.commitIndex = rf.termStartIndex
			}
		}
		MyDebug(dLeader, "S%d Leader updates its commitIndex to %d", rf.me, rf.commitIndex)
		rf.mu.Unlock()
	}
}

// Invoke this function should held the lock
// Before return from this function, the lock should be held
func (rf *Raft) leaderInitialize() {
	// re-initialize nextIndex and matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.termStartIndex = len(rf.log) + 1
	MyDebug(dLog, "S%d Leader initialize, with log:%v", rf.me, rf.log)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}
	rf.broadcastAE()
	rf.lastReset = time.Now()

	// Spawn a new go routine to update leaderCommit
	go rf.updateLeaderCommit()
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
	// Default to -1 which indicates that the leader should handler it by itself
	Next int
	// Default to -1
	ConflictTerm int
}

// Invoke under lock
func (rf *Raft) updateCommitIndex(leaderCommit int) {
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = min(leaderCommit, len(rf.log)+1)
		MyDebug(dLog, "S%d update commitIndex to %d with leaderCommit:%d", rf.me, rf.commitIndex, leaderCommit)
	}
}

// Invoke under lock
func (rf *Raft) SuggestNext(term, index int) int {
	for index > 1 && rf.log[index-1].Term >= term {
		index -= 1
	}
	return index
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
		reply.Next = -1
		reply.ConflictTerm = -1
		return
	}
	// If u are candidate, then after receiving messages from new leader, convert to follower
	if rf.state == Candidate {
		MyDebug(dTimer, "S%d Candidate, received AE, convert back to follower", rf.me)
		rf.state = Follower
	} else {
		MyDebug(dTimer, "S%d Follower, received AE, reset timer", rf.me)
	}

	// Reset the timer as we now receive a valid AE(has term not smaller than us)
	rf.lastReset = time.Now()
	reply.Term = rf.currentTerm
	reply.Next = -1
	reply.ConflictTerm = -1

	// Reply false if log doesn't contain an entry at prevLogIndex whose
	// term matches prevLogTerm
	MyDebug(dLog2, "S%d Follower's log:%v", rf.me, rf.log)
	if args.PrevLogIndex != 0 {
		if len(rf.log) < args.PrevLogIndex {
			MyDebug(dLog, "S%d Follower, does not contain an entry at index %d", rf.me, args.PrevLogIndex)
			reply.Success = false
			reply.Next = len(rf.log)
			return
		}
	}

	// PrevLogIndex may be 0
	/* if args.PrevLogIndex == 0 {*/
	/*MyDebug(dLog, "S%d Follower, delete log to zero", rf.me)*/
	/*// Append any new entry not already in the log*/
	/*rf.log = rf.log[0:0]*/
	/*if args.Entries != nil {*/
	/*rf.log = append(rf.log, args.Entries...)*/
	/*MyDebug(dLog, "S%d Follower, new entry appended:%v", rf.me, rf.log)*/
	/*}*/
	/*rf.updateCommitIndex(args.LeaderCommit)*/
	/*reply.Success = true*/
	/*} else {*/
	// Test if there are conflicts(same index but different terms)
	if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		// Delete the existing entry and all that follow it.
		t := rf.log[args.PrevLogIndex-1].Term
		rf.log = rf.log[0 : args.PrevLogIndex-1]
		MyDebug(dLog, "S%d Follower, conflicts detected, delete log, new log:%v", rf.me, rf.log)
		// Should return directly
		reply.Term = rf.currentTerm
		reply.Next = rf.SuggestNext(t, args.PrevLogIndex-1)
		reply.ConflictTerm = t
		reply.Success = false
	} else {
		// There is a match
		if args.Entries != nil {
			// We must take care not to delete right logs, as this might be a stale request
			for i := range args.Entries {
				if len(rf.log) < args.PrevLogIndex+1+i {
					// There are no entries after this one.
					// We need to simply append the remaining entries
					rf.log = append(rf.log, args.Entries[i:]...)
          break
				} else {
					// Compare the log
					if rf.log[args.PrevLogIndex+i].Term == args.Entries[i].Term {
						continue
					} else {
						// Does not match...
						// Delete from the unmatched point.
						rf.log = rf.log[0 : args.PrevLogIndex+i]
						rf.log = append(rf.log, args.Entries[i:]...)
            break
					}
				}
			}

			MyDebug(dLog, "S%d Follower, match detected, new log appended:%v", rf.me, rf.log)
		}
		rf.updateCommitIndex(args.LeaderCommit)
		reply.Success = true
	}
	//}
}

// Test if t1, i1 is at least up to date compared to t2 i2
func upToDate(t1, i1, t2, i2 int) bool {
	if t1 > t2 {
		return true
	} else if t1 < t2 {
		return false
	} else {
		//t1 = t2
		if i1 >= i2 {
			return true
		}
		return false
	}
}

//REhandler
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

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		//Check if log is at least up-to-date as the candidate
		MyDebug(dLog, "S%d Follower my log:%v", rf.me, rf.log)
		lastLogIndex := len(rf.log)
		lastLogTerm := 0
		if lastLogIndex != 0 {
			lastLogTerm = rf.log[lastLogIndex-1].Term
		}
		MyDebug(dLog, "S%d log comparsion, my lastLogTerm:%d, lastLogIndex:%d", rf.me, lastLogTerm, lastLogIndex)
		if upToDate(args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex) {
			// Vote
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			MyDebug(dVote, "S%d Follower votes for S%d in term %d", rf.me, args.CandidateId, rf.currentTerm)
			rf.lastReset = time.Now()
			return
		} else {
			// Not vote
			MyDebug(dVote, "S%d Follower reject vote for S%d in term %d, not updated log", rf.me, args.CandidateId, rf.currentTerm)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
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
	MyDebug(dLog, "S%d receives new command in Start", rf.me)
	// Generate the LogEntry
	log := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   len(rf.log) + 1,
	}

	// Append to its own log
	rf.log = append(rf.log, log)

	// Reset the timer for leader as we are about to send messages
	rf.lastReset = time.Now()
	rf.broadcastAE()

	// Return immediately from the function, don't wait for results
	return len(rf.log), rf.currentTerm, true
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

func (rf *Raft) sendCommandToCh(applyCh chan ApplyMsg) {

	sentIndex := 0
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		MyDebug(dTrace, "S%d tries to acquire lock in Ch thread", rf.me)
		rf.mu.Lock()

		for sentIndex < rf.commitIndex {
			temp := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[sentIndex].Command,
				CommandIndex: sentIndex + 1,
			}
			sentIndex += 1
			applyCh <- temp
			MyDebug(dLog, "S%d sends message to the applyCh %v", rf.me, temp.Command)
		}

		rf.mu.Unlock()
		MyDebug(dTrace, "S%d tries to release the lock in Ch thread", rf.me)
	}
}

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
	go rf.sendCommandToCh(applyCh)

	return rf
}

func min(a, b int) int {
	if a <= b {
		return a
	}

	return b
}

func max(a, b int) int {
	if a >= b {
		return a
	}

	return b
}
