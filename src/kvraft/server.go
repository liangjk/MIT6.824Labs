package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Opcode int

const (
	GET Opcode = iota
	PUT
	APPEND
)

const (
	tickerIntv        = 1
	SnapshotThreshold = 0.9
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Code       Opcode
	Key, Value string
	Cid        int32
	Seq        int64
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	persister *raft.Persister
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data        map[string]string
	clientSeq   map[int32]int64
	clientReply map[int32]string

	wait    map[int32]*sync.Cond
	nowTerm int
}

func (kv *KVServer) getWaitL(cid int32) (ret *sync.Cond) {
	ret = kv.wait[cid]
	if ret == nil {
		ret = sync.NewCond(&kv.mu)
		kv.wait[cid] = ret
	}
	return
}

func (kv *KVServer) checkTermL(term int) {
	if term > kv.nowTerm {
		kv.nowTerm = term
		for _, cond := range kv.wait {
			cond.Broadcast()
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Seq <= kv.clientSeq[args.Cid] {
		reply.Err = OK
		reply.Value = kv.clientReply[args.Cid]
		return
	}
	op := Op{GET, args.Key, "", args.Cid, args.Seq}
	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.checkTermL(term)
	for {
		kv.getWaitL(args.Cid).Wait()
		if term != kv.nowTerm || kv.killed() {
			reply.Err = ErrWrongLeader
			return
		}
		if args.Seq <= kv.clientSeq[args.Cid] {
			reply.Err = OK
			reply.Value = kv.clientReply[args.Cid]
			return
		}
	}
}

// unlike in lab 2, neither Put nor Append should return a value.
// this is already reflected in the PutAppendReply struct.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Seq <= kv.clientSeq[args.Cid] {
		reply.Err = OK
		return
	}
	op := Op{args.Op, args.Key, args.Value, args.Cid, args.Seq}
	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.checkTermL(term)
	for {
		kv.getWaitL(args.Cid).Wait()
		if term != kv.nowTerm || kv.killed() {
			reply.Err = ErrWrongLeader
			return
		}
		if args.Seq <= kv.clientSeq[args.Cid] {
			reply.Err = OK
			return
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) generateSnapshotL() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.clientSeq)
	e.Encode(kv.clientReply)
	return w.Bytes()
}

func (kv *KVServer) applyOp(op *Op, index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.Seq <= kv.clientSeq[op.Cid] {
		return
	}
	switch op.Code {
	case GET:
		kv.clientReply[op.Cid] = kv.data[op.Key]
	case PUT:
		kv.data[op.Key] = op.Value
	case APPEND:
		kv.data[op.Key] += op.Value
	}
	kv.clientSeq[op.Cid] = op.Seq
	kv.getWaitL(op.Cid).Broadcast()
	if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= int(float32(kv.maxraftstate)*SnapshotThreshold) {
		snapshot := kv.generateSnapshotL()
		go kv.rf.Snapshot(index, snapshot)
	}
}

func (kv *KVServer) applyMsg(msg *raft.ApplyMsg) {
	if msg.CommandValid {
		op, ok := msg.Command.(Op)
		if ok {
			kv.applyOp(&op, msg.CommandIndex)
			return
		}
	} else if msg.SnapshotValid {
		kv.mu.Lock()
		ok := kv.applySnapshotL(msg.Snapshot)
		kv.checkTermL(msg.SnapshotTerm)
		kv.mu.Unlock()
		if ok {
			return
		}
	}
	DPrintf("Unknown apply message: %v\n", *msg)
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		kv.applyMsg(&msg)
	}
}

func (kv *KVServer) ticker() {
	for {
		kv.mu.Lock()
		if kv.killed() {
			for _, cond := range kv.wait {
				cond.Broadcast()
			}
			kv.mu.Unlock()
			return
		}
		term, _ := kv.rf.GetState()
		kv.checkTermL(term)
		kv.mu.Unlock()
		time.Sleep(time.Second * tickerIntv)
	}
}

func (kv *KVServer) applySnapshotL(snapshot []byte) bool {
	if snapshot == nil || len(snapshot) < 1 {
		return false
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var seq map[int32]int64
	var reply map[int32]string
	err := d.Decode(&data)
	if err != nil {
		DPrintf("Apply Snapshot data error:%v, Snapshot data:%v\n", err, snapshot)
		return false
	}
	err = d.Decode(&seq)
	if err != nil {
		DPrintf("Apply Snapshot clientSeq error:%v, Snapshot data:%v\n", err, snapshot)
		return false
	}
	err = d.Decode(&reply)
	if err != nil {
		DPrintf("Apply Snapshot clientReply error:%v, Snapshot data:%v\n", err, snapshot)
		return false
	}
	kv.data = data
	kv.clientSeq = seq
	kv.clientReply = reply
	return true
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	// You may need initialization code here.
	if !kv.applySnapshotL(persister.ReadSnapshot()) {
		kv.data = make(map[string]string)
		kv.clientSeq = make(map[int32]int64)
		kv.clientReply = make(map[int32]string)
	}

	kv.wait = make(map[int32]*sync.Cond)
	kv.nowTerm = 0

	go kv.applier()
	go kv.ticker()

	return kv
}
