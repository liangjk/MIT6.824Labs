package raft

import "sync/atomic"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	Assert(index < rf.lastApplied, "Snapshot includes command not applied: %v, lastApplied: %v", index, rf.lastApplied)
	discardIndex := index - rf.startIndex
	if discardIndex < 0 {
		rf.mu.Unlock()
		return
	}
	rf.startIndex = index
	newLogs := []Log{{Term: rf.logs[discardIndex].Term}}
	rf.logs = append(newLogs, rf.logs[discardIndex+1:]...)
	rf.snapshot = snapshot
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) sendSnapshot(peer, term, mIndex int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm != term {
			return
		}
		if reply.Term > term {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = Follower
			return
		}
		if mIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = mIndex
			rf.commitCond.Signal()
		}
		go rf.sendLog(peer, term)
	}
}

type InstallSnapshotArgs struct {
	Term                int
	LastIndex, LastTerm int
	Data                []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	rf.state = Follower
	atomic.StoreInt32(&rf.missedHeartbeat, 0)
	discardIndex := args.LastIndex - rf.startIndex
	if discardIndex <= 0 {
		return
	}
	rf.snapshot = args.Data
	rf.startIndex = args.LastIndex
	newLogs := []Log{{Term: args.LastTerm}}
	if discardIndex < len(rf.logs) && rf.logs[discardIndex].Term == args.LastTerm {
		rf.logs = append(newLogs, rf.logs[discardIndex+1:]...)
	} else {
		rf.logs = newLogs
	}
	if rf.commitIndex < args.LastIndex+1 {
		rf.commitIndex = args.LastIndex + 1
	}
	rf.lastApplied = args.LastIndex + 1
	rf.snapshotApplying = true
	rf.applyCond.Signal()
	rf.persist()
}
