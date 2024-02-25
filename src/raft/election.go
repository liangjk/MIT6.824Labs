package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term, CandidateId, LastLogTerm, LastLogIndex int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term    int
	Granted bool
}

func (rf *Raft) getLastLog() (term, index int) {
	logLen := len(rf.logs) - 1
	index = rf.startIndex + logLen
	term = rf.logs[logLen].Term
	return
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Granted = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	} else if rf.votedFor != args.CandidateId && rf.votedFor != -1 {
		reply.Term = rf.currentTerm
		reply.Granted = false
		return
	}
	reply.Term = rf.currentTerm
	lastLogTerm, lastLogIndex := rf.getLastLog()
	if args.LastLogTerm < lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		reply.Granted = false
		return
	}
	rf.votedFor = args.CandidateId
	reply.Granted = true
	atomic.StoreInt32(&rf.missedHeartbeat, 0)
	rf.persist()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	lastLogTerm, lastLogIndex := rf.getLastLog()
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogTerm, lastLogIndex}
	rf.mu.Unlock()

	const (
		Success = -1
		Fail    = 0
	)
	numServers := len(rf.peers)
	results := make(chan int, numServers)
	votes := 1
	elected := false
	for i := 0; i < numServers; i++ {
		if i == rf.me {
			continue
		}
		peer := rf.peers[i]
		go func() {
			reply := RequestVoteReply{}
			ok := peer.Call("Raft.RequestVote", &args, &reply)
			if ok {
				if reply.Granted {
					results <- Success
				} else if reply.Term > args.Term {
					results <- reply.Term
				} else {
					results <- Fail
				}
			} else {
				results <- Fail
			}
		}()
	}
	for i := 1; i < numServers; i++ {
		res := <-results
		if res == Success {
			votes++
			if votes*2 > numServers {
				elected = true
				break
			}
		} else if res != Fail {
			rf.mu.Lock()
			if res > rf.currentTerm {
				rf.currentTerm = res
				rf.votedFor = -1
				rf.state = Follower
			}
			rf.mu.Unlock()
			return
		}
	}

	if elected {
		rf.mu.Lock()
		DPrintf("Server %v won election in term: %v", rf.me, rf.currentTerm)
		if rf.currentTerm == args.Term && rf.state == Candidate {
			rf.state = Leader
			for i := range rf.peers {
				rf.nextIndex[i] = lastLogIndex + 1
				rf.matchIndex[i] = 1
			}
			rf.newOp = 0
			go rf.heartbeat(rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		if atomic.LoadInt32(&rf.missedHeartbeat) > ElectionThreshold {
			go rf.startElection()
			atomic.StoreInt32(&rf.missedHeartbeat, 0)
		} else {
			atomic.AddInt32(&rf.missedHeartbeat, 1)
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
