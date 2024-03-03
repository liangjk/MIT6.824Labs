package paxos

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) {
	px.mu.Lock()
	inst := px.getInstanceL(args.Seq)
	if inst == nil {
		px.mu.Unlock()
		return
	}
	px.mu.Unlock()
	go px.decide(args.Seq, inst, args.Value)
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) {
	px.mu.Lock()
	inst := px.getInstanceL(args.Seq)
	if inst == nil {
		px.mu.Unlock()
		reply.Reply = Fail
		return
	}
	px.mu.Unlock()
	inst.mu.Lock()
	if inst.status != Decided && args.Accept >= inst.prepare {
		inst.prepare = args.Accept
		inst.accept = args.Accept
		inst.value = args.Value
		reply.Reply = Ok
		px.persistInstanceL(args.Seq, inst)
	} else {
		reply.Reply = inst.prepare
	}
	inst.mu.Unlock()
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) {
	px.mu.Lock()
	inst := px.getInstanceL(args.Seq)
	if inst == nil {
		px.mu.Unlock()
		reply.Code = LateRequest
		return
	}
	px.mu.Unlock()
	inst.mu.Lock()
	if inst.status == Decided {
		reply.Code = HasDecided
		reply.Value = inst.value
	} else if args.Prepare > inst.prepare {
		inst.prepare = args.Prepare
		reply.Code = Ok
		reply.AcPr = inst.accept
		reply.Value = inst.value
	} else {
		reply.Code = LowPrepare
		reply.AcPr = inst.prepare
	}
	inst.mu.Unlock()
}
