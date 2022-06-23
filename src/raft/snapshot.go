package raft

import ()

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
// This function is generally called from the leader to send a snapshot to the
// follower or a new-entered server, and the server has to decide whether they
// want to use it
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
	// Acquire the lock
	rf.mu.Lock()
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
}

// Invoke on lock
func (rf *Raft) trimLog(index int) {
	deleteLength := index - rf.lastIncludedIndex

	// Do we have the next element?
	if len(rf.log)-1 > deleteLength {
		// Safe operation
		rf.log = append(rf.log[0:1], rf.log[deleteLength+1:]...)
	} else {
		// Just delete all the logs
		rf.log = rf.log[0:1]
	}
}
