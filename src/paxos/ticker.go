package paxos

import "time"

func (px *Paxos) GetDone(args *DoneArgs, reply *DoneReply) {
	px.mu.Lock()
	reply.Done = px.done
	reply.Decided = px.decided
	px.mu.Unlock()
}

func (px *Paxos) ticker() {
	for !px.isdead() {
		px.mu.Lock()
		done := px.done
		decided := px.decided
		startIndex := px.startIndex
		myDecided := px.decided
		px.mu.Unlock()
		nextWakeup := time.Now().Add(tickerIntv * time.Millisecond)

		numServers := len(px.peers)
		replyCh := make(chan DoneReply, numServers-1)
		args := DoneArgs{}
		for i, peer := range px.peers {
			if i != px.me {
				reply := DoneReply{}
				peer := peer
				go func() {
					peer.Call("Paxos.GetDone", &args, &reply)
					replyCh <- reply
				}()
			}
		}
		for i := 1; i < numServers; i++ {
			reply := <-replyCh
			if reply.Done < done {
				done = reply.Done
			}
			if reply.Decided > decided {
				decided = reply.Decided
			}
		}

		if done > startIndex {
			px.mu.Lock()
			if done > px.startIndex+len(px.instances) {
				done = px.startIndex + len(px.instances)
			}
			for i := px.startIndex; i < done; i++ {
				inst := px.getInstanceL(i)
				go px.decide(i, inst, nil)
			}
			instances := px.instances[done-px.startIndex:]
			px.instances = make([]*Instance, len(instances))
			copy(px.instances, instances)
			px.startIndex = done
			px.persistL()
			for i := px.startIndex; i <= decided; i++ {
				inst := px.getInstanceL(i)
				go px.proposer(i, nil, inst)
			}
			px.mu.Unlock()
		} else if decided > myDecided {
			px.mu.Lock()
			for i := px.startIndex; i <= decided; i++ {
				inst := px.getInstanceL(i)
				go px.proposer(i, nil, inst)
			}
			px.mu.Unlock()
		}

		time.Sleep(time.Until(nextWakeup))
	}
}
