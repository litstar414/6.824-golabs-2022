package raft

import (
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
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

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := len(rf.log) - 1 + rf.lastIncludedIndex
		lastLogTerm := rf.findRelatedTerm(lastLogIndex)
		if upToDate(args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex) {
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.lastReset = time.Now()
			rf.persist()
			return
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
		return
	}
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

// Invoke this function should held the lock
func (rf *Raft) broadcastRV() {
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1 + rf.lastIncludedIndex,
	}
	args.LastLogTerm = rf.findRelatedTerm(args.LastLogIndex)

	count := 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// Send rpc to other servers
		go func(server, term int) {
			reply := RequestVoteReply{}
			MyDebug(dVote, "S%d sends request to S%d", rf.me, server)
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				// Check the term
				MyDebug(dTrace, "S%d tries to acquire the lock in MHRV", rf.me)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				defer MyDebug(dTrace, "S%d release the lock in MHRV", rf.me)

				if rf.checkTerm(reply.Term) {
					//reply.Term is greater than our term, back to follower
					return
				}

				// Stale information
				if term != rf.currentTerm {
					return
				}
				if reply.VoteGranted && rf.state != Leader {
					count += 1
					MyDebug(dVote, "S%d Candidate, received vote from S%d in term %d, total count %d", rf.me, server, rf.currentTerm, count)
					if count > len(rf.peers)/2 {
						// This is a valid reply, we become as the leader
						MyDebug(dVote, "S%d Leader, has become leader in term %d", rf.me, rf.currentTerm)
						rf.state = Leader
						// When we update our term, other thread will not handle the stale request
						rf.leaderInitialize()
					}
				}
			}
		}(i, rf.currentTerm)
	}
}
