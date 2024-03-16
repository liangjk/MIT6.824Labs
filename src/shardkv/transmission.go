package shardkv

import (
	"sync"
	"sync/atomic"
	"time"
)

type InstallOp InstallShardArgs

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	kv.tmmu[args.Shard].Lock()
	defer kv.tmmu[args.Shard].Unlock()
	if kv.tmseq[args.Shard][args.Gid] >= args.Seq {
		reply.Err = OK
		return
	}
	if kv.installing[args.Shard] {
		reply.Err = ErrWait
		return
	}
	op := InstallOp(*args)
	_, tm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	term := int32(tm)
	kv.checkTerm(term)
	kv.installing[args.Shard] = true
	defer func() { kv.installing[args.Shard] = false }()
	for {
		kv.tmwait[args.Shard].Wait()
		if term != atomic.LoadInt32(&kv.nowTerm) {
			reply.Err = ErrWrongLeader
			return
		}
		select {
		case <-kv.doneCh:
			reply.Err = ErrWrongLeader
			return
		default:
		}
		if kv.tmseq[args.Shard][args.Gid] >= args.Seq {
			reply.Err = OK
			return
		}
	}
}

func (data *ShardData) clone() *ShardData {
	ret := &ShardData{make(map[string]string, len(data.KV)), make(map[int32]int64, len(data.ClientSeq)), make(map[int32]string, len(data.ClientReply))}
	for k, v := range data.KV {
		ret.KV[k] = v
	}
	for k, v := range data.ClientSeq {
		ret.ClientSeq[k] = v
	}
	for k, v := range data.ClientReply {
		ret.ClientReply[k] = v
	}
	return ret
}

func (kv *ShardKV) applyInstallOp(op *InstallOp) {
	kv.tmmu[op.Shard].Lock()
	defer kv.tmmu[op.Shard].Unlock()
	if kv.tmseq[op.Shard][op.Gid] >= op.Seq {
		return
	}
	kv.shardmu[op.Shard].Lock()
	if kv.kvs[op.Shard] != nil {
		DPrintf("Install overwriting shard:%v\n", op.Shard)
	}
	kv.kvs[op.Shard] = op.Data.clone()
	kv.wait[op.Shard] = make(map[int32]*sync.Cond)
	kv.shardmu[op.Shard].Unlock()
	kv.tmseq[op.Shard][op.Gid] = op.Seq
	kv.tmwait[op.Shard].Broadcast()
}

func (kv *ShardKV) sendShard(servers []string, shard int, data *ShardData, seq int64) {
	args := InstallShardArgs{shard, kv.gid, seq, *data}
	for {
		for _, server := range servers {
			srv := kv.make_end(server)
		retry:
			reply := InstallShardReply{}
			ok := srv.Call("ShardKV.InstallShard", &args, &reply)
			if ok {
				switch reply.Err {
				case OK:
					return
				case ErrWait:
					time.Sleep(time.Millisecond * installMs)
					goto retry
				}
			}
		}
		time.Sleep(time.Millisecond * 100)
		select {
		case <-kv.doneCh:
			return
		default:
		}
	}
}