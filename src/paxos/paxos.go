package paxos

import (
	"sync"
	"sync/atomic"

	"6.5840/labrpc"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Instance struct {
	mu              sync.Mutex
	prepare, accept int
	value           interface{}
	status          Fate
}

type Paxos struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	me        int                 // index into peers[]
	persister *Persister
	dead      int32

	instances     []*Instance
	startIndex    int
	done, decided int
}

func (px *Paxos) decide(seq int, inst *Instance, value interface{}) {
	inst.mu.Lock()
	if inst.status != Decided {
		inst.status = Decided
		if value != nil {
			inst.value = value
		}
		px.persistInstanceL(seq, inst)
	}
	inst.mu.Unlock()
	px.mu.Lock()
	if seq > px.decided {
		px.decided = seq
	}
	px.mu.Unlock()
}

func (px *Paxos) getInstanceL(seq int) *Instance {
	if seq < px.startIndex {
		return nil
	}
	for i := len(px.instances); px.startIndex+i <= seq; i++ {
		newInst := &Instance{status: Pending}
		px.instances = append(px.instances, newInst)
	}
	return px.instances[seq-px.startIndex]
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()
	inst := px.getInstanceL(seq)
	if inst != nil {
		go px.proposer(seq, v, inst)
	}
}

// the application on this machine is done with
// all instances <= seq.
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	if seq >= px.done {
		px.done = seq + 1
	}
	px.mu.Unlock()
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.startIndex + len(px.instances) - 1
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.startIndex
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	if seq < px.startIndex {
		px.mu.Unlock()
		return Forgotten, nil
	} else if seq >= px.startIndex+len(px.instances) {
		px.mu.Unlock()
		return Pending, nil
	}
	inst := px.instances[seq-px.startIndex]
	px.mu.Unlock()
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if inst.status == Decided {
		px.persistInstanceL(seq, inst)
		return Decided, inst.value
	}
	return Pending, nil
}

// tell the peer to shut itself down.
// for testing.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
}

// has this peer been asked to shut down?
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.persister = persister

	px.instances = make([]*Instance, 0)
	px.readPersist(persister)

	go px.ticker()

	return px
}
