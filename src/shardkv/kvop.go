package shardkv

import (
	"sync"
	"sync/atomic"
)

func (kv *ShardKV) getWaitL(shard int, cid int32) (ret *sync.Cond) {
	ret = kv.wait[shard][cid]
	if ret == nil {
		ret = sync.NewCond(&kv.shardmu[shard])
		kv.wait[shard][cid] = ret
	}
	return
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.shardmu[args.Shard].Lock()
	defer kv.shardmu[args.Shard].Unlock()
	sdd := kv.kvs[args.Shard]
	if sdd == nil {
		reply.Err = ErrWrongGroup
		return
	}
	if args.Seq <= sdd.ClientSeq[args.Cid] {
		reply.Err = OK
		reply.Value = sdd.ClientReply[args.Cid]
		return
	}
	op := Op{GET, args.Key, "", args.Shard, args.Cid, args.Seq}
	_, tm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	term := int32(tm)
	kv.checkTerm(term)
	for {
		kv.getWaitL(args.Shard, args.Cid).Wait()
		select {
		case <-kv.doneCh:
			reply.Err = ErrWrongGroup
			return
		default:
		}
		sdd = kv.kvs[args.Shard]
		if sdd == nil {
			reply.Err = ErrWrongGroup
			return
		}
		if term != atomic.LoadInt32(&kv.nowTerm) {
			reply.Err = ErrWrongLeader
			return
		}
		if args.Seq <= sdd.ClientSeq[args.Cid] {
			reply.Err = OK
			reply.Value = sdd.ClientReply[args.Cid]
			return
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.shardmu[args.Shard].Lock()
	defer kv.shardmu[args.Shard].Unlock()
	sdd := kv.kvs[args.Shard]
	if sdd == nil {
		reply.Err = ErrWrongGroup
		return
	}
	if args.Seq <= sdd.ClientSeq[args.Cid] {
		reply.Err = OK
		return
	}
	op := Op{args.Op, args.Key, args.Value, args.Shard, args.Cid, args.Seq}
	_, tm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	term := int32(tm)
	kv.checkTerm(term)
	for {
		kv.getWaitL(args.Shard, args.Cid).Wait()
		select {
		case <-kv.doneCh:
			reply.Err = ErrWrongGroup
			return
		default:
		}
		sdd = kv.kvs[args.Shard]
		if sdd == nil {
			reply.Err = ErrWrongGroup
			return
		}
		if term != atomic.LoadInt32(&kv.nowTerm) {
			reply.Err = ErrWrongLeader
			return
		}
		if args.Seq <= sdd.ClientSeq[args.Cid] {
			reply.Err = OK
			return
		}
	}
}

func (kv *ShardKV) applyOp(op *Op) {
	kv.shardmu[op.Shard].Lock()
	defer kv.shardmu[op.Shard].Unlock()
	sdd := kv.kvs[op.Shard]
	if sdd == nil {
		return
	}
	if sdd.ClientSeq[op.Cid] >= op.Seq {
		return
	}
	switch op.Code {
	case GET:
		sdd.ClientReply[op.Cid] = sdd.KV[op.Key]
	case PUT:
		sdd.KV[op.Key] = op.Value
	case APPEND:
		sdd.KV[op.Key] = sdd.KV[op.Key] + op.Value
	}
	sdd.ClientSeq[op.Cid] = op.Seq
	kv.getWaitL(op.Shard, op.Cid).Broadcast()
}
