package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//
type ErrCode int

const (
	OK ErrCode = iota + 1
	ErrWrongGroup
	ErrWrongLeader
	ErrWait
)

type OpCode int

const (
	GET OpCode = iota
	PUT
	APPEND
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Op    OpCode // "Put" or "Append"
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Shard int
	Cid   int32
	Seq   int64
}

type PutAppendReply struct {
	Err ErrCode
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Shard int
	Cid   int32
	Seq   int64
}

type GetReply struct {
	Err   ErrCode
	Value string
}

type SendShardArgs struct {
	Shard, Gid int
	Seq        int64
	Data       ShardData
}

type SendShardReply struct {
	Err ErrCode
}
