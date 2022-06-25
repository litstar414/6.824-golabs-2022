package raft

import "time"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// Called from the leader to a follower
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	MyDebug(dTrace, "S%d tries to acquire lock in ISS handler", rf.me)
	rf.mu.Lock()
	rf.mu.Unlock()
	defer MyDebug(dTrace, "S%d release the lock in ISS handler", rf.me)

	rf.checkTerm(args.Term)
	// Reply immediately if term < rf.currentTerm
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// Should reset the timer
	rf.lastReset = time.Now()

	// If existing log entry has same index and term as snapshot's last included entry,
	// retain log entries following it and reply

	// The log entry might already in the snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	// Send the snapshot to applyCh
	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Snapshot,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

	// Save the state to disk
	// The return to this function tells the leader that I as a follower have all the information
	// up to this snapshot, so we must save the snapshot before return
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Snapshot)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
// This function is generally called from the leader to send a snapshot to the
// follower or a new-entered server, and the server has to decide whether they
// want to use it
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	// Based on the state of the snapshot, trim our log
	// lastIncludedIndex > rf.commitIndex
	if len(rf.log)-1+rf.lastIncludedIndex < lastIncludedIndex {
		// discard the entire log
		rf.ResetLog(nil, -1, -1)
	} else if rf.log[lastIncludedIndex-rf.lastIncludedIndex].Term == lastIncludedTerm {
		rf.ResetLog(rf.log, lastIncludedIndex-rf.lastIncludedIndex+1, -1)
	} else {
		rf.ResetLog(nil, -1, -1)
	}
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
	return true

	// check the term and index
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// Acquire the lock
	rf.mu.Lock()
	MyDebug(dTrace, "S%d acquired the lock in Snapshot", rf.me)
	defer MyDebug(dTrace, "S%d releases the lock in Snapshot", rf.me)
	defer rf.mu.Unlock()

	// Try to trim the log
	if index <= rf.lastIncludedIndex {
		// Stale snapshot
		MyDebug(dSnap, "S%d receives a stale snapshot", rf.me)
		return
	}

	// Before trimming, store the lastIncludedTerm
	rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex].Term
	// Now try to trim the log
	rf.trimLog(index)
	rf.lastIncludedIndex = index
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
}

// Invoke on lock
func (rf *Raft) trimLog(index int) {
	deleteLength := index - rf.lastIncludedIndex

	// Do we have the next element?
	if len(rf.log)-1 > deleteLength {
		// Safe operation
		rf.ResetLog(rf.log, deleteLength+1, -1)
	} else {
		// Just delete all the logs
		rf.ResetLog(nil, -1, -1)
	}
}

// Does not require lock(rf.mu)
func (rf *Raft) handleIS(server, term, leaderId, lastIncludedIndex, lastIncludedTerm int, snapshot []byte) {
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          leaderId,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Snapshot:          snapshot,
	}
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if ok {
		rf.handleISReply(server, &args, &reply, term)
	}
}

func (rf *Raft) handleISReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, term int) {
	MyDebug(dTrace, "S%d tries to acquire the lock in ISRP", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer MyDebug(dTrace, "S%d release the lock in ISRP", rf.me)

	if rf.checkTerm(reply.Term) {
		return
	}

	if reply.Term != term {
		return
	}

	// Update matchIndex and nextIndex
	rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex+1)
	rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
}
