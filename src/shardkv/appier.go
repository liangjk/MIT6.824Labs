package shardkv

import (
	"sync"

	"6.5840/raft"
	"6.5840/shardctrler"
)

func (kv *ShardKV) removeShardL(shard int) {
	kv.kvs[shard] = nil
	for _, cond := range kv.wait[shard] {
		cond.Broadcast()
	}
	kv.wait[shard] = nil
}

func (kv *ShardKV) applyConfig(cfg *shardctrler.Config) {
	kv.cmu.Lock()
	kv.config = *cfg
	kv.cmu.Unlock()
	for i, gid := range cfg.Shards {
		kv.shardmu[i].Lock()
		sdd := kv.kvs[i]
		if sdd != nil {
			if gid != kv.gid {
				kv.removeShardL(i)
				kv.seq[i]++
				go kv.sendShard(cfg.Groups[gid], i, sdd, kv.seq[i])
			}
		} else if gid == kv.gid {
			if kv.crtclerk.Create(i, cfg.Num) {
				// DPrintf("Group:%v Server:%v creating shard:%v\n", kv.gid, kv.me, shard)
				kv.kvs[i] = &ShardData{make(map[string]string), make(map[int32]int64), make(map[int32]string)}
				kv.wait[i] = make(map[int32]*sync.Cond)
			}
		}
		kv.shardmu[i].Unlock()
	}
}

func (kv *ShardKV) applyMsg(msg *raft.ApplyMsg) {
	// DPrintf("Group:%v Server:%v apply:%v\n", kv.gid, kv.me, *msg)
	if msg.CommandValid {
		switch msg.Command.(type) {
		case Op:
			op := msg.Command.(Op)
			kv.applyOp(&op)
		case shardctrler.Config:
			op := msg.Command.(shardctrler.Config)
			kv.applyConfig(&op)
		case InstallOp:
			op := msg.Command.(InstallOp)
			kv.applyInstallOp(&op)
		default:
			DPrintf("Unknown Command:%v\n", msg.Command)
		}
		return
	}
	if msg.SnapshotValid {
		return
	}
	DPrintf("Unknown Message:%v\n", *msg)
}

func (kv *ShardKV) applier() {
	for {
		select {
		case msg, ok := <-kv.applyCh:
			if ok {
				kv.applyMsg(&msg)
			} else {
				return
			}
		case <-kv.doneCh:
			return
		}
	}
}
