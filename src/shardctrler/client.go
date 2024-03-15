package shardctrler

//
// Shardctrler clerk.
//

import (
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	cid    int32
	seq    int64
	leader int
}

// func nrand() int64 {
// 	max := big.NewInt(int64(1) << 62)
// 	bigx, _ := rand.Int(rand.Reader, max)
// 	x := bigx.Int64()
// 	return x
// }

var assignedCid int32 = -1

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.cid = atomic.AddInt32(&assignedCid, 1)
	ck.seq = 0
	ck.leader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.Cid = ck.cid
	ck.seq++
	args.Seq = ck.seq
		serverCnt := len(ck.servers)
	for {
		// try each known server.
		for i := 0; i < serverCnt; i++ {
			var reply QueryReply
			ok := ck.servers[ck.leader].Call("ShardCtrler.Query", args, &reply)
			if ok && reply.Ok {
				return reply.Config
			}
			ck.leader++
			if ck.leader >= serverCnt {
				ck.leader = 0
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.Cid = ck.cid
	ck.seq++
	args.Seq = ck.seq
		serverCnt := len(ck.servers)
	for {
		// try each known server.
		for i := 0; i < serverCnt; i++ {
			var reply OpReply
			ok := ck.servers[ck.leader].Call("ShardCtrler.Join", args, &reply)
			if ok && reply.Ok {
				return
			}
			ck.leader++
			if ck.leader >= serverCnt {
				ck.leader = 0
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.Cid = ck.cid
	ck.seq++
	args.Seq = ck.seq
	serverCnt := len(ck.servers)
	for {
		// try each known server.
		for i := 0; i < serverCnt; i++ {
			var reply OpReply
			ok := ck.servers[ck.leader].Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.Ok {
				return
			}
			ck.leader++
			if ck.leader >= serverCnt {
				ck.leader = 0
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.Cid = ck.cid
	ck.seq++
	args.Seq = ck.seq
	serverCnt := len(ck.servers)
	for {
		// try each known server.
		for i := 0; i < serverCnt; i++ {
			var reply OpReply
			ok := ck.servers[ck.leader].Call("ShardCtrler.Move", args, &reply)
			if ok && reply.Ok {
				return
			}
			ck.leader++
			if ck.leader >= serverCnt {
				ck.leader = 0
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
