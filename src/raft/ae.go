package raft

import (
	"time"
)

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

	SuggestNext  int
	ConflictTerm int
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// Handle the reply for previous AE
// NO LOCK
func (rf *Raft) handleAEReply(server int, args *AppendEntryArgs, reply *AppendEntryReply, term int, prevLogIndex int) {
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
		MyDebug(dLeader, "S%d leader, received success from previous AE in term %d", rf.me, rf.currentTerm)
		// Update nextIndex and matchIndex
		if prevLogIndex+1 == rf.nextIndex[server] {
			// We can update
			rf.nextIndex[server] = len(args.Entries) + 1 + prevLogIndex
		}
		rf.matchIndex[server] = max(rf.matchIndex[server], len(args.Entries)+prevLogIndex)
		MyDebug(dLeader, "S%d leader, update matchIndex(%d) to %d", rf.me, server, rf.matchIndex[server])
	} else {
		if reply.SuggestNext == -1 {
			rf.nextIndex[server] -= 1
		} else {
			rf.nextIndex[server] = reply.SuggestNext
		}
	}

}

// This function will send AE and handle the reply
// Do not assume lock on invoke
func (rf *Raft) handleAE(server, term, LeaderId, PrevLogIndex, PrevLogTerm int,
	Entries []LogEntry, LeaderCommit int) {
	// held the lock
	args := AppendEntryArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: PrevLogIndex,
		PrevLogTerm:  PrevLogTerm,
		Entries:      Entries,
		LeaderCommit: LeaderCommit,
	}
	reply := AppendEntryReply{}
	ok := rf.sendAppendEntry(server, &args, &reply)
	if ok {
		rf.handleAEReply(server, &args, &reply, term, PrevLogIndex)
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

		//otherwise, send messages
		//Prepare the arguments
		PrevLogIndex := rf.nextIndex[i] - 1
		if PrevLogIndex < rf.lastIncludedIndex {
			// TODO: PrevLogIndex may in the snapshot, in that case, we should send the snapshot
			// TODO: send snapshot
			MyDebug(dSnap, "S%d sends snapshot for server %d with PrevLogIndex:%v lastIncludedIndex:%v",
				rf.me, PrevLogIndex, rf.lastIncludedIndex)
			continue
		}
		PrevLogTerm := rf.findRelatedTerm(PrevLogIndex)
		// Prepare Entries, as we may cut this while sending the rpc, create a new one
		Entries := append([]LogEntry{}, rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:]...)
		MyDebug(dLeader, "S%d Leader sends AE to server %d in term %d with entries:%v", rf.me, i, rf.currentTerm, Entries)
		go rf.handleAE(i, rf.currentTerm, rf.me, PrevLogIndex, PrevLogTerm, Entries, rf.commitIndex)
	}
}

// AEhandler
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	MyDebug(dTrace, "S%d tries to acquire lock in AE handler", rf.me)
	rf.mu.Lock()
	defer MyDebug(dTrace, "S%d releases the lock in AE handler", rf.me)
	defer rf.mu.Unlock()

	rf.checkTerm(args.Term)
	// Reply false if term < currentTerm
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
	} else {
		MyDebug(dTimer, "S%d Follower, received AE, reset timer", rf.me)
	}

	reply.Term = rf.currentTerm
	rf.lastReset = time.Now()
	reply.SuggestNext = -1
	reply.ConflictTerm = -1
	// Reply false if log does not contain an entry at prevLogIndex
	if rf.lastIncludedIndex+len(rf.log)-1 < args.PrevLogIndex {
		reply.Success = false
		reply.SuggestNext = len(rf.log) + rf.lastIncludedIndex
		return
	}

	// As we are about to compare log with index PrevLogIndex, we need to make sure
	// that we have that entry in our log
	if args.PrevLogIndex <= rf.lastIncludedIndex {
		// As the logs prior to this has already committed, it must be a match
		rf.tryAppendNewEntries(args.Entries, args.PrevLogIndex)
		reply.Success = true
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.lastIncludedIndex+len(rf.log)-1)
			rf.cond.Signal()
		}
		return
	}

	// Conflict not match term for prevLogIndex
	if rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		// Conflict, delete the existing entry and all that follow it
		t := rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
		reply.Success = false
		reply.ConflictTerm = t
		reply.SuggestNext = rf.findFirstIndexWithTerm(reply.ConflictTerm, args.PrevLogIndex)
		rf.ResetLog(rf.log, 1, args.PrevLogIndex-rf.lastIncludedIndex)
		rf.persist()
		return
	}

	// No conflict on prevLogIndex
	rf.tryAppendNewEntries(args.Entries, args.PrevLogIndex)
	reply.Success = true

	// Update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1+rf.lastIncludedIndex)
		rf.cond.Signal()
	}
	return
}

// Invoke with lock
// Find the first log entry with term t
// Or the last entry with term number t - 1
func (rf *Raft) findFirstIndexWithTerm(t, startIndex int) int {
	for ; startIndex > rf.commitIndex && startIndex > rf.lastIncludedIndex && rf.log[startIndex-rf.lastIncludedIndex].Term == t; startIndex-- {

	}
	return startIndex + 1
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

//Invoke under lock
func (rf *Raft) tryAppendNewEntries(Entries []LogEntry, PrevLogIndex int) {
	for i := range Entries {
		if PrevLogIndex+i+1 <= rf.lastIncludedIndex {
			continue
		}
		// First test if my log has this entry or not
		if len(rf.log)-1+rf.lastIncludedIndex < PrevLogIndex+i+1 {
			// Append all the entries following it.
			rf.log = append(rf.log, Entries[i:]...)
			MyDebug(dLog, "S%d appends entries %v to its log", rf.me, Entries[i:])
			MyDebug(dLog, "S%d new log:%v", rf.me, rf.log)
			rf.persist()
			break
		}

		// Compare the term
		index := PrevLogIndex + i + 1
		if rf.log[index-rf.lastIncludedIndex].Term == Entries[i].Term {
			continue
		} else {
			// Delete and append
			rf.ResetLog(rf.log, 1, index-rf.lastIncludedIndex)
			rf.log = append(rf.log, Entries[i:]...)
			rf.persist()
			MyDebug(dLog, "S%d appends entries %v to its log", rf.me, Entries[i:])
			MyDebug(dLog, "S%d new log:%v", rf.me, rf.log)
			break
		}
	}

}
