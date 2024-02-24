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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	snapshotApplying      int
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.startIndex)
	raftstate := w.Bytes()
	if rf.startIndex > 0 {
		rf.persister.Save(raftstate, rf.snapshot)
	} else {
		rf.persister.Save(raftstate, nil)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) bool {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return false
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []Log
	var startIndex int
	err := d.Decode(&currentTerm)
	if err != nil {
		DPrintf("Read Persist currentTerm error: %v", err)
		return false
	}
	err = d.Decode(&votedFor)
	if err != nil {
		DPrintf("Read Persist votedFor error: %v", err)
		return false
	}
	err = d.Decode(&logs)
	if err != nil {
		DPrintf("Read Persist logs error: %v", err)
		return false
	}
	err = d.Decode(&startIndex)
	if err != nil {
		DPrintf("Read Persist startIndex error: %v", err)
		return false
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.startIndex = startIndex
	if startIndex > 0 {
		rf.snapshot = rf.persister.ReadSnapshot()
		rf.commitIndex = startIndex + 1
		rf.lastApplied = startIndex + 1
	}
	return true
}

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
		return
	}
	rf.startIndex = index
	newLogs := []Log{{Term: rf.logs[discardIndex].Term}}
	rf.logs = append(newLogs, rf.logs[discardIndex+1:]...)
	rf.snapshot = snapshot
	rf.persist()
	rf.mu.Unlock()
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
	term = rf.logs[logLen].Term
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
		rf.persist()
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.snapshotApplying == 0 {
			rf.applyCond.Signal()
		}
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
	index = rf.startIndex + len(rf.logs)
	isLeader = true
	rf.logs = append(rf.logs, Log{term, command})
	rf.persist()
	if rf.newOp == 0 {
		rf.newOp = index
		go rf.checkCommit(rf.currentTerm)
	}
	rf.lastMsg = time.Now().UnixMilli()
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendLog(i, term)
		}
	}
	return
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	for !rf.killed() {
		for rf.lastApplied < rf.commitIndex && rf.snapshotApplying == 0 {
			msg := ApplyMsg{CommandValid: true, Command: rf.logs[rf.lastApplied-rf.startIndex].Command, CommandIndex: rf.lastApplied}
			rf.lastApplied++
			select {
			case rf.applyCh <- msg:
			default:
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
			}
		}
		rf.applyCond.Wait()
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendSnapshot(peer, term, mIndex int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader || rf.currentTerm != term {
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

func (rf *Raft) sendLog(peer, term int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderCommit: rf.commitIndex}
	for {
		if rf.nextIndex[peer] <= rf.startIndex {
			rf.nextIndex[peer] = rf.startIndex + 1
			snapshotArgs := InstallSnapshotArgs{rf.currentTerm, rf.startIndex, rf.logs[0].Term, rf.snapshot}
			go rf.sendSnapshot(peer, term, rf.startIndex+1, &snapshotArgs)
			rf.mu.Unlock()
			return
		}
		args.Entries = rf.logs[rf.nextIndex[peer]-rf.startIndex:]
		args.PrevLogIndex = rf.nextIndex[peer] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex-rf.startIndex].Term
		tempSave := rf.nextIndex[peer]
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
		if ok {
			rf.mu.Lock()
			if rf.state != Leader || rf.currentTerm != term || rf.nextIndex[peer] != tempSave {
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
		if rf.state != Leader || rf.currentTerm != term {
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

func (rf *Raft) checkCommit(term int) {
	rf.mu.Lock()
	newCommit := false
	for !rf.killed() {
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		if newCommit {
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
					if rf.snapshotApplying == 0 {
						rf.applyCond.Signal()
					}
				} else {
					break
				}
			}
			rf.commitCond.Wait()
		} else {
			matched := 1
			for i := range rf.peers {
				if rf.matchIndex[i] > rf.newOp && i != rf.me {
					matched++
					if matched*2 > len(rf.peers) {
						newCommit = true
						break
					}
				}
			}
			if !newCommit {
				rf.commitCond.Wait()
			}
		}
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
		DPrintf("Server %v won election in term: %v", rf.me, rf.currentTerm)
		rf.mu.Lock()
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
	msg := ApplyMsg{SnapshotValid: true, Snapshot: rf.snapshot, SnapshotTerm: args.LastTerm, SnapshotIndex: args.LastIndex}
	rf.snapshotApplying++
	go func() {
		rf.applyCh <- msg
		rf.mu.Lock()
		rf.snapshotApplying--
		rf.mu.Unlock()
		rf.applyCond.Signal()
	}()
	rf.persist()
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

	rf.commitIndex = 1
	rf.lastApplied = 1

	rf.commitCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	if !rf.readPersist(persister.ReadRaftState()) {
		rf.currentTerm = 0
		rf.logs = make([]Log, 1)
		rf.votedFor = -1
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.apply()

	return rf
}
