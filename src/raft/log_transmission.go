package raft

import (
	"sync/atomic"
	"time"
)

type AppendEntriesArgs struct {
	Term                      int
	PrevLogTerm, PrevLogIndex int
	Entries                   []Log
	LeaderCommit              int
}

type AppendEntriesReply struct {
	Term                int
	Success             bool
	XTerm, XIndex, XLen int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	rf.state = Follower
	atomic.StoreInt32(&rf.missedHeartbeat, 0)
	if args.PrevLogIndex < rf.startIndex {
		discardIndex := rf.startIndex - args.PrevLogIndex
		if discardIndex >= len(args.Entries) {
			reply.Success = true
			return
		}
		args.PrevLogTerm = args.Entries[discardIndex-1].Term
		args.PrevLogIndex = rf.startIndex
		args.Entries = args.Entries[discardIndex:]
	}
	if args.PrevLogIndex >= rf.startIndex+len(rf.logs) {
		myIndex := len(rf.logs) - 1
		for myIndex > 0 && rf.logs[myIndex].Term == rf.logs[myIndex-1].Term {
			myIndex--
		}
		reply.Success = false
		reply.XIndex = myIndex + rf.startIndex
		reply.XTerm = rf.logs[myIndex].Term
		reply.XLen = len(rf.logs) + rf.startIndex
		return
	}
	if rf.logs[args.PrevLogIndex-rf.startIndex].Term != args.PrevLogTerm {
		reply.Success = false
		myIndex := args.PrevLogIndex - rf.startIndex
		for myIndex > 0 && rf.logs[myIndex].Term == rf.logs[myIndex-1].Term {
			myIndex--
		}
		reply.XIndex = myIndex + rf.startIndex
		reply.XTerm = rf.logs[myIndex].Term
		reply.XLen = args.PrevLogIndex
		return
	}
	myIndex := args.PrevLogIndex - rf.startIndex + 1
	argsIndex := 0
	modified := false
	for argsIndex < len(args.Entries) {
		if myIndex >= len(rf.logs) || rf.logs[myIndex].Term != args.Entries[argsIndex].Term {
			rf.logs = append(rf.logs[:myIndex], args.Entries[argsIndex:]...)
			modified = true
			break
		}
		myIndex++
		argsIndex++
	}
	if modified {
		rf.persistL()
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}
	reply.Success = true
}

func (rf *Raft) sendLog(peer, term int) {
	rf.mu.Lock()
	if rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	for {
		if rf.nextIndex[peer] <= rf.startIndex {
			rf.nextIndex[peer] = rf.startIndex + 1
			snapshotArgs := InstallSnapshotArgs{rf.currentTerm, rf.startIndex, rf.logs[0].Term, rf.snapshot}
			go rf.sendSnapshot(peer, term, rf.startIndex+1, &snapshotArgs)
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{Term: rf.currentTerm, Entries: rf.logs[rf.nextIndex[peer]-rf.startIndex:], LeaderCommit: rf.commitIndex}
		args.PrevLogIndex = rf.nextIndex[peer] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex-rf.startIndex].Term
		tempSave := rf.nextIndex[peer]
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
		if ok && !rf.killed() {
			rf.mu.Lock()
			if rf.currentTerm != term || rf.nextIndex[peer] != tempSave {
				rf.mu.Unlock()
				return
			}
			if reply.Success {
				rf.nextIndex[peer] += len(args.Entries)
				if rf.nextIndex[peer] > rf.matchIndex[peer] {
					rf.matchIndex[peer] = rf.nextIndex[peer]
					rf.commitCond.Signal()
				}
				rf.mu.Unlock()
				return
			}
			if reply.Term > args.Term {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				rf.mu.Unlock()
				return
			}
			nIndex := reply.XIndex
			for nIndex > rf.startIndex && rf.logs[nIndex-rf.startIndex-1].Term >= reply.XTerm {
				nIndex--
			}
			if nIndex < rf.startIndex {
				nIndex = rf.startIndex
			}
			for nIndex-rf.startIndex < len(rf.logs) && nIndex < reply.XLen && rf.logs[nIndex-rf.startIndex].Term == reply.XTerm {
				nIndex++
			}
			rf.nextIndex[peer] = nIndex
		} else {
			return
		}
	}
}

func (rf *Raft) heartbeat(term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		since := time.Now().UnixMilli() - rf.lastMsg
		rf.mu.Unlock()
		atomic.StoreInt32(&rf.missedHeartbeat, 0)
		if since*2 > HeartbeatRate {
			for i := range rf.peers {
				if i != rf.me {
					go rf.sendLog(i, term)
				}
			}
		}
		time.Sleep(time.Millisecond * HeartbeatRate)
	}
}
