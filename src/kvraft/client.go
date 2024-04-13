package kvraft

import (
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.cid = atomic.AddInt32(&assignedCid, 1)
	ck.seq = 0
	ck.leader = 0
	return ck
}

const electionWaitSec = 1

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seq++
	args := GetArgs{key, ck.cid, ck.seq}
	retry := 0
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			return reply.Value
		}
		ck.leader++
		if ck.leader >= len(ck.servers) {
			ck.leader = 0
		}
		retry++
		if retry >= len(ck.servers) {
			time.Sleep(time.Second * electionWaitSec)
			retry = 0
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op Opcode) {
	// You will have to modify this function.
	ck.seq++
	args := PutAppendArgs{op, key, value, ck.cid, ck.seq}
	retry := 0
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			return
		}
		ck.leader++
		if ck.leader >= len(ck.servers) {
			ck.leader = 0
		}
		retry++
		if retry >= len(ck.servers) {
			time.Sleep(time.Second * electionWaitSec)
			retry = 0
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
