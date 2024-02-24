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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	HeartbeatRate     = 200
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
	startIndex, snapshotTerm int
	commitIndex, lastApplied int
	nextIndex, matchIndex    []int
	state                    int

	logs []Log

	missedHeartbeat int32

	applyCh               chan ApplyMsg
	commitCond, applyCond *sync.Cond
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term, CandidateId, LastLogTerm, LastLogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term    int
	Granted bool
}

func (rf *Raft) getLastLog() (term, index int) {
	logLen := len(rf.logs) - 1
	index = rf.startIndex + logLen
	term = rf.snapshotTerm
	if logLen >= 0 {
		term = rf.logs[logLen].Term
	}
	return
}

// example RequestVote RPC handler.
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
	rf.persist()
}

type AppendEntriesArgs struct {
	Term                      int
	PrevLogTerm, PrevLogIndex int
	Entries                   []Log
	LeaderCommit              int
}

type AppendEntriesReply struct {
	Term                     int
	Success                  bool
	ConfilctTerm, StartIndex int
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
	} else {
		Assert(rf.state != Leader, "Leader received AppendEntries in same term!!! Server: %v Term: %v", rf.me, rf.currentTerm)
	}
	reply.Term = rf.currentTerm
	rf.state = Follower
	atomic.StoreInt32(&rf.missedHeartbeat, 0)
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	var argsIndex int
	myIndex := args.PrevLogIndex + 1
	for argsIndex = 0; argsIndex < len(args.Entries); argsIndex++ {
		if myIndex >= len(rf.logs) || rf.logs[myIndex].Term != args.Entries[argsIndex].Term {
			break
		}
		myIndex++
	}
	rf.logs = append(rf.logs[:myIndex], args.Entries[argsIndex:]...)
	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}
	reply.Success = true
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
	index = len(rf.logs)
	isLeader = true
	rf.logs = append(rf.logs, Log{term, command})
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendLog(i, term)
		}
	}
	// DPrintf("Leader %v start command %v at index %v", rf.me, command, index)
	return
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	for !rf.killed() {
		for rf.lastApplied < rf.commitIndex {
			msg := ApplyMsg{CommandValid: true, Command: rf.logs[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
			select {
			case rf.applyCh <- msg:
			default:
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
			}
			// DPrintf("Server %v apply msg %v", rf.me, rf.lastApplied)
			rf.lastApplied++
		}
		rf.applyCond.Wait()
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendLog(peer, term int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderCommit: rf.commitIndex}
	peerRpc := rf.peers[peer]
	for {
		args.Entries = rf.logs[rf.nextIndex[peer]:]
		args.PrevLogIndex = rf.nextIndex[peer] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		ok := peerRpc.Call("Raft.AppendEntries", &args, &reply)
		if ok {
			rf.mu.Lock()
			if rf.state != Leader || rf.currentTerm != term || rf.nextIndex[peer] != args.PrevLogIndex+1 {
				rf.mu.Unlock()
				return
			}
			if reply.Success {
				rf.nextIndex[peer] += len(args.Entries)
				rf.matchIndex[peer] = rf.nextIndex[peer]
				rf.mu.Unlock()
				rf.commitCond.Signal()
				return
			}
			if reply.Term > args.Term {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				rf.mu.Unlock()
				return
			}
			rf.nextIndex[peer]--
		} else {
			return
		}
	}

}

func (rf *Raft) heartbeat(term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendLog(i, term)
			}
		}
		time.Sleep(time.Millisecond * HeartbeatRate)
	}
}

func (rf *Raft) checkCommit(term int) {
	rf.mu.Lock()
	for !rf.killed() {
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		for {
			matched := 1
			for i := range rf.peers {
				if rf.matchIndex[i] > rf.commitIndex && i != rf.me {
					matched++
					if matched*2 > len(rf.peers) {
						break
					}
				}
			}
			if matched*2 > len(rf.peers) {
				rf.commitIndex++
				rf.applyCond.Signal()
			} else {
				break
			}
		}
		rf.commitCond.Wait()
	}
	rf.mu.Unlock()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	// DPrintf("Election %v Term %v", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.state = Candidate
	lastLogTerm, lastLogIndex := rf.getLastLog()
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogTerm, lastLogIndex}
	rf.mu.Unlock()

	numServers := len(rf.peers)
	results := make(chan int, numServers)

	const (
		Success = -1
		Fail    = 0
	)

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

	for i := 0; i < numServers-1; i++ {
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
		DPrintf("Win! %v Term: %v", rf.me, rf.currentTerm)
		rf.mu.Lock()
		if rf.currentTerm == args.Term && rf.state == Candidate {
			rf.state = Leader
			for i := range rf.peers {
				rf.nextIndex[i] = lastLogIndex + 1
				rf.matchIndex[i] = 1
			}
			go rf.heartbeat(rf.currentTerm)
			go rf.checkCommit(rf.currentTerm)
		}
		rf.mu.Unlock()
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		if atomic.LoadInt32(&rf.missedHeartbeat) > ElectionThreshold {
			go rf.startElection()
		}
		atomic.AddInt32(&rf.missedHeartbeat, 1)
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
	rf.logs = make([]Log, 1)

	rf.commitIndex = 1
	rf.lastApplied = 1

	rf.commitCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.votedFor = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.apply()

	return rf
}
