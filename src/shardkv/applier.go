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
	for i, gid := range cfg.Shards {
		kv.shardmu[i].Lock()
		sdd := kv.kvs[i]
		var sdkv *SendData
		if sdd != nil {
			if gid != kv.gid {
				kv.removeShardL(i)
				kv.seq[i]++
				sdkv = &SendData{cfg.Groups[gid], *sdd, kv.seq[i]}
			}
		} else if gid == kv.gid {
			if kv.crtclerk.Create(i, cfg.Num) {
				kv.kvs[i] = &ShardData{make(map[string]string), make(map[int32]int64), make(map[int32]string)}
				kv.wait[i] = make(map[int32]*sync.Cond)
			}
		}
		kv.shardmu[i].Unlock()
		if sdkv != nil {
			kv.sdmu[i].Lock()
			kv.sdkvs[i] = sdkv
			kv.sdmu[i].Unlock()
		}
	}
}

const snapshotThreshold = 0.9

func (kv *ShardKV) applyMsg(msg *raft.ApplyMsg) {
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
		if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= int(float32(kv.maxraftstate)*snapshotThreshold) {
			kv.snapshot(msg.CommandIndex)
		}
		return
	}
	if msg.SnapshotValid {
		if !kv.applySnapshot(msg.Snapshot) {
			DPrintf("Unable to decode snapshot:%v\n", msg.Snapshot)
		}
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
