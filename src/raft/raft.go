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
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower          = 0
	Candidate         = 1
	Leader            = 2
	ElectionThreshold = 5
	HeartbeatRate     = 300
)

type Log struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm              int
	votedFor                 int
	commitIndex, lastApplied int
	nextIndex, matchIndex    []int
	state                    int

	logs []Log

	missedHeartbeat int32
	lastMsg         int64

	applyCh               chan ApplyMsg
	done                  chan bool
	snapshotApplying      bool
	commitCond, applyCond *sync.Cond
	newOp                 int

	startIndex int
	snapshot   []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state != Leader {
		index = -1
		isLeader = false
		return
	}
	index = rf.startIndex + len(rf.logs)
	isLeader = true
	rf.logs = append(rf.logs, Log{term, command})
	rf.persistL()
	if rf.newOp == 0 {
		rf.newOp = index
		rf.commitCond.Broadcast()
		go rf.committer(rf.currentTerm)
	}
	rf.lastMsg = time.Now().UnixMilli()
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendLog(i, term)
		}
	}
	return
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer close(rf.applyCh)
	for !rf.killed() {
		if rf.snapshotApplying {
			msg := ApplyMsg{SnapshotValid: true, Snapshot: rf.snapshot, SnapshotTerm: rf.logs[0].Term, SnapshotIndex: rf.startIndex}
			rf.snapshotApplying = false
			select {
			case rf.applyCh <- msg:
			default:
				rf.mu.Unlock()
				select {
				case rf.applyCh <- msg:
				case <-rf.done:
					return
				}
				rf.mu.Lock()
			}
		} else if rf.lastApplied < rf.commitIndex {
			var batch []ApplyMsg
			for rf.lastApplied < rf.commitIndex {
				msg := ApplyMsg{CommandValid: true, Command: rf.logs[rf.lastApplied-rf.startIndex].Command, CommandIndex: rf.lastApplied}
				batch = append(batch, msg)
				rf.lastApplied++
			}
			rf.mu.Unlock()
			for len(batch) > 0 {
				select {
				case rf.applyCh <- batch[0]:
					batch = batch[1:]
				case <-rf.done:
					return
				}
			}
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) testIndexL(index int) bool {
	matched := 1
	for i := range rf.peers {
		if rf.matchIndex[i] > index && i != rf.me {
			matched++
			if matched*2 > len(rf.peers) {
				return true
			}
		}
	}
	return false
}

func (rf *Raft) committer(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newCommit := false
	for !rf.killed() {
		if rf.currentTerm != term {
			return
		}
		if newCommit {
			if rf.testIndexL(rf.commitIndex) {
				lBound := rf.commitIndex + 1
				rBound := rf.startIndex + len(rf.logs)
				for rBound > lBound {
					mid := (lBound + rBound) >> 1
					if rf.testIndexL(mid) {
						lBound = mid + 1
					} else {
						rBound = mid
					}
				}
				rf.commitIndex = lBound
				rf.applyCond.Signal()
			}
			rf.commitCond.Wait()
		} else if rf.testIndexL(rf.newOp) {
			newCommit = true
			rf.commitIndex = rf.newOp + 1
			rf.applyCond.Signal()
		} else {
			rf.commitCond.Wait()
		}
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	select {
	case rf.done <- true:
	default:
	}
	rf.commitCond.Broadcast()
	rf.applyCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	numPeers := len(peers)
	rf.nextIndex = make([]int, numPeers)
	rf.matchIndex = make([]int, numPeers)

	rf.commitIndex = 1
	rf.lastApplied = 1

	rf.commitCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.done = make(chan bool)

	// initialize from state persisted before a crash
	if !rf.readPersist(persister.ReadRaftState()) {
		rf.currentTerm = 0
		rf.logs = make([]Log, 1)
		rf.votedFor = -1
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
