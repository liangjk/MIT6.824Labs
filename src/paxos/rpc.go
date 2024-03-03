package paxos

type PrepareArgs struct {
	Seq     int
	Prepare int
}

const (
	Ok          = -1
	Fail        = 0
	LowPrepare  = 1
	HasDecided  = 2
	LateRequest = 3
)

type PrepareReply struct {
	Code  int
	AcPr  int
	Value interface{}
}

type AcceptArgs struct {
	Seq    int
	Accept int
	Value  interface{}
}

type AcceptReply struct {
	Reply int
}

type DecideArgs struct {
	Seq   int
	Value interface{}
}

type DecideReply struct{}

type DoneArgs struct{}

type DoneReply struct {
	Done, Decided int
}
