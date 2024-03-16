package shardkv

import (
	"log"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Code       OpCode
	Key, Value string
	Shard      int
	Cid        int32
	Seq        int64
}

type ShardData struct {
	KV          map[string]string
	ClientSeq   map[int32]int64
	ClientReply map[int32]string
}

type ShardKV struct {
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big

	make_end func(string) *labrpc.ClientEnd
	gid      int
	seq      [shardctrler.NShards]int64

	cfgclerk, crtclerk *shardctrler.Clerk
	cmu                sync.Mutex
	config             shardctrler.Config

	shardmu [shardctrler.NShards]sync.Mutex
	kvs     [shardctrler.NShards]*ShardData
	wait    [shardctrler.NShards]map[int32]*sync.Cond

	gmu         [shardctrler.NShards]sync.Mutex
	gseq        [shardctrler.NShards]map[int]int64
	ginstalling [shardctrler.NShards]bool
	gwait       [shardctrler.NShards]*sync.Cond

	nowTerm int32

	doneCh chan bool
}

func (kv *ShardKV) wakeup() {
	for i := 0; i < shardctrler.NShards; i++ {
		go func(shard int) {
			kv.shardmu[shard].Lock()
			if kv.kvs[shard] != nil {
				for _, cond := range kv.wait[shard] {
					cond.Broadcast()
				}
			}
			kv.shardmu[shard].Unlock()
		}(i)
		go func(shard int) {
			kv.gmu[shard].Lock()
			kv.gwait[shard].Broadcast()
			kv.gmu[shard].Unlock()
		}(i)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	close(kv.doneCh)
	kv.wakeup()
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(InstallOp{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.cfgclerk = shardctrler.MakeClerk(ctrlers)
	kv.crtclerk = shardctrler.MakeClerk(ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.gseq[i] = make(map[int]int64)
		kv.gwait[i] = sync.NewCond(&kv.gmu[i])
	}

	kv.doneCh = make(chan bool)

	go kv.leaderChecker()
	go kv.ctrlerTicker()
	go kv.applier()
	return kv
}
