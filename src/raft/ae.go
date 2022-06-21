package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
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
type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// Handle the reply for previous AE
// NO LOCK
func (rf *Raft) handleAEReply(reply *AppendEntryReply, term int) {
	MyDebug(dTrace, "S%d tries to acquire the lock in MHAE", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer MyDebug(dTrace, "S%d release the lock in MHAE", rf.me)

	//TODO: implement
	if rf.checkTerm(reply.Term) {
		return
	}
	if reply.Term != term {
		// Stale information
		return
	}
}

// This function will send AE and handle the reply
// Do not assume lock on invoke
func (rf *Raft) handleAE(server, term int) {
	// held the lock
	args := AppendEntryArgs{
		Term:     term,
		LeaderId: rf.me,
	}
	reply := AppendEntryReply{}
	MyDebug(dLeader, "S%d Leader sends HB to server %d in term %d", rf.me, server, term)
	ok := rf.sendAppendEntry(server, &args, &reply)
	if ok {
		rf.handleAEReply(&reply, term)
	}

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
		go rf.handleAE(i, rf.currentTerm)
	}
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

