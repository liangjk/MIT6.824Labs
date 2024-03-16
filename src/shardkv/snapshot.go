package shardkv

import (
	"bytes"
	"sync"

	"6.5840/labgob"
	"6.5840/shardctrler"
)

func (kv *ShardKV) snapshot(index int) {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(kv.tmseq)
	encoder.Encode(kv.seq)
	list := make([]int, 0, shardctrler.NShards)
	for i, sdd := range kv.kvs {
		if sdd != nil {
			list = append(list, i)
		}
	}
	encoder.Encode(list)
	for _, sd := range list {
		encoder.Encode(*kv.kvs[sd])
	}
	list = make([]int, 0, shardctrler.NShards)
	sdlist := make([]*SendData, 0, shardctrler.NShards)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.sdmu[i].Lock()
		if kv.sdkvs[i] != nil {
			list = append(list, i)
			sdlist = append(sdlist, kv.sdkvs[i])
		}
		kv.sdmu[i].Unlock()
	}
	encoder.Encode(list)
	for _, sdd := range sdlist {
		encoder.Encode(*sdd)
	}
	kv.rf.Snapshot(index, buf.Bytes())
}

func (kv *ShardKV) applySnapshot(snapshot []byte) bool {
	if snapshot == nil || len(snapshot) < 1 {
		return false
	}
	buf := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buf)
	var tmseq [shardctrler.NShards]map[int]int64
	var seq [shardctrler.NShards]int64

	var list []int
	var kvs [shardctrler.NShards]*ShardData
	var sdkvs [shardctrler.NShards]*SendData
	if decoder.Decode(&tmseq) != nil {
		return false
	}
	if decoder.Decode(&seq) != nil {
		return false
	}

	if decoder.Decode(&list) != nil {
		return false
	}
	for _, sd := range list {
		var sdd ShardData
		if decoder.Decode(&sdd) != nil {
			return false
		}
		kvs[sd] = &sdd
	}
	if decoder.Decode(&list) != nil {
		return false
	}
	for _, sd := range list {
		var sdd SendData
		if decoder.Decode(&sdd) != nil {
			return false
		}
		sdkvs[sd] = &sdd
	}

	for i := 0; i < shardctrler.NShards; i++ {
		kv.seq[i] = seq[i]

		kv.tmmu[i].Lock()
		kv.tmseq[i] = tmseq[i]
		kv.tmwait[i].Broadcast()
		kv.tmmu[i].Unlock()

		kv.shardmu[i].Lock()
		if kvs[i] != nil {
			if kv.kvs[i] == nil {
				kv.wait[i] = make(map[int32]*sync.Cond)
			} else {
				for _, cond := range kv.wait[i] {
					cond.Broadcast()
				}
			}
			kv.kvs[i] = kvs[i]
		} else if kv.kvs[i] != nil {
			kv.removeShardL(i)
		}
		kv.shardmu[i].Unlock()

		kv.sdmu[i].Lock()
		kv.sdkvs[i] = sdkvs[i]
		kv.sdmu[i].Unlock()
	}

	return true
}
