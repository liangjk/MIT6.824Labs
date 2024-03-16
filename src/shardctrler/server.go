package shardctrler

import (
	"bytes"
	"log"
	"sort"
	"sync"
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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	clientSeq map[int32]int64
	wait      map[int32]*sync.Cond
	nowTerm   int

	doneCh chan bool

	configs []Config // indexed by config num
	create  [NShards]int
}

type Opcode int

const (
	JOIN Opcode = iota + 1
	LEAVE
	MOVE
	QUERY
	CREATE
)

type Op struct {
	// Your data here.
	Code    Opcode
	Content []byte
	Cid     int32
	Seq     int64
}

func (sc *ShardCtrler) operateL(op *Op) bool {
	_, term, isLeader := sc.rf.Start(*op)
	if !isLeader {
		return false
	}
	sc.checkTermL(term)
	for {
		sc.getWaitL(op.Cid).Wait()
		if term != sc.nowTerm {
			return false
		}
		select {
		case <-sc.doneCh:
			return false
		default:
		}
		if op.Seq <= sc.clientSeq[op.Cid] {
			return true
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *OpReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if args.Seq <= sc.clientSeq[args.Cid] {
		reply.Ok = true
		return
	}
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(args.Servers)
	op := Op{JOIN, buf.Bytes(), args.Cid, args.Seq}
	reply.Ok = sc.operateL(&op)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *OpReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if args.Seq <= sc.clientSeq[args.Cid] {
		reply.Ok = true
		return
	}
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(args.GIDs)
	op := Op{LEAVE, buf.Bytes(), args.Cid, args.Seq}
	reply.Ok = sc.operateL(&op)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *OpReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if args.Seq <= sc.clientSeq[args.Cid] {
		reply.Ok = true
		return
	}
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(args.Shard)
	encoder.Encode(args.GID)
	op := Op{MOVE, buf.Bytes(), args.Cid, args.Seq}
	reply.Ok = sc.operateL(&op)
}

func (sc *ShardCtrler) getConfigL(index int, config *Config) {
	nowLen := len(sc.configs)
	var ref *Config
	if index < 0 || index >= nowLen {
		ref = &sc.configs[nowLen-1]
	} else {
		ref = &sc.configs[index]
	}
	config.Num = ref.Num
	config.Shards = ref.Shards
	config.Groups = make(map[int][]string, len(ref.Groups))
	for k, v := range ref.Groups {
		config.Groups[k] = v
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if args.Seq <= sc.clientSeq[args.Cid] {
		reply.Ok = true
		sc.getConfigL(args.Num, &reply.Config)
		return
	}
	op := Op{QUERY, nil, args.Cid, args.Seq}
	if sc.operateL(&op) {
		reply.Ok = true
		sc.getConfigL(args.Num, &reply.Config)
		return
	}
	reply.Ok = false
}

func (sc *ShardCtrler) Create(args *CreateArgs, reply *CreateReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.create[args.Shard] != 0 {
		reply.Ok = true
		reply.Create = args.Num == sc.create[args.Shard]
		return
	}
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(args.Shard)
	encoder.Encode(args.Num)
	op := Op{CREATE, buf.Bytes(), args.Cid, args.Seq}
	reply.Ok = sc.operateL(&op)
	reply.Create = args.Num == sc.create[args.Shard]
}

func (cfg *Config) reConfig() {
	liveCnt := len(cfg.Groups)
	if liveCnt == 0 {
		for sd := range cfg.Shards {
			cfg.Shards[sd] = 0
		}
	} else {
		max := NShards / liveCnt
		limit := NShards % liveCnt
		liveGroups := make([]int, 0, liveCnt)
		for gid := range cfg.Groups {
			liveGroups = append(liveGroups, gid)
		}
		sort.Ints(liveGroups)
		realloc := make([]int, 0, NShards)
		alloc := make(map[int]int, liveCnt)
		for sd, gid := range cfg.Shards {
			if cfg.Groups[gid] == nil || alloc[gid] > max {
				realloc = append(realloc, sd)
			} else if alloc[gid] == max {
				if limit > 0 {
					limit--
					alloc[gid] = alloc[gid] + 1
				} else {
					realloc = append(realloc, sd)
				}
			} else {
				alloc[gid] = alloc[gid] + 1
			}
		}
		for _, gid := range liveGroups {
			nowLen := alloc[gid]
			for nowLen < max {
				cfg.Shards[realloc[0]] = gid
				nowLen++
				realloc = realloc[1:]
			}
			if limit > 0 && nowLen == max {
				limit--
				cfg.Shards[realloc[0]] = gid
				realloc = realloc[1:]
			}
		}
	}
}

func (sc *ShardCtrler) applyJoin(op *Op) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if op.Seq <= sc.clientSeq[op.Cid] {
		return true
	}
	buf := bytes.NewBuffer(op.Content)
	decoder := labgob.NewDecoder(buf)
	var Servers map[int][]string
	if decoder.Decode(&Servers) != nil {
		return false
	}
	var newConfig Config
	sc.getConfigL(-1, &newConfig)
	newConfig.Num++

	for gid, val := range Servers {
		newConfig.Groups[gid] = val
	}
	newConfig.reConfig()

	sc.configs = append(sc.configs, newConfig)
	sc.clientSeq[op.Cid] = op.Seq
	sc.getWaitL(op.Cid).Broadcast()
	return true
}

func (sc *ShardCtrler) applyLeave(op *Op) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if op.Seq <= sc.clientSeq[op.Cid] {
		return true
	}
	buf := bytes.NewBuffer(op.Content)
	decoder := labgob.NewDecoder(buf)
	var GIDs []int
	if decoder.Decode(&GIDs) != nil {
		return false
	}
	var newConfig Config
	sc.getConfigL(-1, &newConfig)
	newConfig.Num++

	for _, lvgid := range GIDs {
		delete(newConfig.Groups, lvgid)
	}
	newConfig.reConfig()

	sc.configs = append(sc.configs, newConfig)
	sc.clientSeq[op.Cid] = op.Seq
	sc.getWaitL(op.Cid).Broadcast()
	return true
}

func (sc *ShardCtrler) applyMove(op *Op) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if op.Seq <= sc.clientSeq[op.Cid] {
		return true
	}
	buf := bytes.NewBuffer(op.Content)
	decoder := labgob.NewDecoder(buf)
	var Shard, GID int
	if decoder.Decode(&Shard) != nil {
		return false
	}
	if decoder.Decode(&GID) != nil {
		return false
	}
	var newConfig Config
	sc.getConfigL(-1, &newConfig)
	newConfig.Num++
	newConfig.Shards[Shard] = GID
	sc.configs = append(sc.configs, newConfig)
	sc.clientSeq[op.Cid] = op.Seq
	sc.getWaitL(op.Cid).Broadcast()
	return true
}

func (sc *ShardCtrler) applyQuery(op *Op) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if op.Seq > sc.clientSeq[op.Cid] {
		sc.clientSeq[op.Cid] = op.Seq
		sc.getWaitL(op.Cid).Broadcast()
	}
	return true
}

func (sc *ShardCtrler) applyCreate(op *Op) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if op.Seq <= sc.clientSeq[op.Cid] {
		return true
	}
	buf := bytes.NewBuffer(op.Content)
	decoder := labgob.NewDecoder(buf)
	var Shard, Num int
	if decoder.Decode(&Shard) != nil {
		return false
	}
	if decoder.Decode(&Num) != nil {
		return false
	}
	if sc.create[Shard] == 0 {
		sc.create[Shard] = Num
	}
	sc.clientSeq[op.Cid] = op.Seq
	sc.getWaitL(op.Cid).Broadcast()
	return true
}

func (sc *ShardCtrler) getWaitL(cid int32) (ret *sync.Cond) {
	ret = sc.wait[cid]
	if ret == nil {
		ret = sync.NewCond(&sc.mu)
		sc.wait[cid] = ret
	}
	return
}

func (sc *ShardCtrler) checkTermL(term int) {
	if term > sc.nowTerm {
		sc.nowTerm = term
		for _, cond := range sc.wait {
			cond.Broadcast()
		}
	}
}

func (sc *ShardCtrler) applyMsg(msg *raft.ApplyMsg) {
	if msg.CommandValid {
		op, ok := msg.Command.(Op)
		if ok {
			success := false
			switch op.Code {
			case JOIN:
				success = sc.applyJoin(&op)
			case LEAVE:
				success = sc.applyLeave(&op)
			case MOVE:
				success = sc.applyMove(&op)
			case QUERY:
				success = sc.applyQuery(&op)
			case CREATE:
				success = sc.applyCreate(&op)
			}
			if !success {
				DPrintf("Unknown operation: %v\n", op)
			}
			return
		}
	}
	DPrintf("Unknown apply message: %v\n", *msg)
}

func (sc *ShardCtrler) applier() {
	for {
		select {
		case msg, ok := <-sc.applyCh:
			if ok {
				sc.applyMsg(&msg)
			} else {
				return
			}
		case <-sc.doneCh:
			return
		}
	}
}

const tickerIntv = 1

func (sc *ShardCtrler) ticker() {
	const d = time.Second * tickerIntv
	timer := time.NewTimer(d)
	for {
		select {
		case <-sc.doneCh:
			timer.Stop()
			sc.mu.Lock()
			for _, cond := range sc.wait {
				cond.Broadcast()
			}
			sc.mu.Unlock()
			return
		case <-timer.C:
			sc.mu.Lock()
			term, _ := sc.rf.GetState()
			sc.checkTermL(term)
			sc.mu.Unlock()
		}
		timer.Reset(d)
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	close(sc.doneCh)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientSeq = make(map[int32]int64)
	sc.wait = make(map[int32]*sync.Cond)
	sc.nowTerm = 0
	sc.doneCh = make(chan bool)

	go sc.applier()
	go sc.ticker()

	return sc
}
