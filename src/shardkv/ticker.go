package shardkv

import (
	"sync/atomic"
	"time"

	"6.5840/shardctrler"
)

const (
	ctrlerMs    = 100
	leaderMs    = 1000
	sendLongMs  = 1000
	sendShortMs = 200
)

func (kv *ShardKV) checkTerm(term int32) {
	for {
		oldTerm := atomic.LoadInt32(&kv.nowTerm)
		if term > oldTerm {
			if atomic.CompareAndSwapInt32(&kv.nowTerm, oldTerm, term) {
				kv.wakeup()
				return
			}
		} else {
			return
		}
	}
}

func (kv *ShardKV) leaderChecker() {
	const d = time.Millisecond * leaderMs
	timer := time.NewTimer(d)
	for {
		select {
		case <-kv.doneCh:
			timer.Stop()
			return
		case <-timer.C:
			term, _ := kv.rf.GetState()
			kv.checkTerm(int32(term))
		}
		timer.Reset(d)
	}
}

func (kv *ShardKV) ctrlerTicker() {
	const d = time.Millisecond * ctrlerMs
	timer := time.NewTimer(d)
	for {
		select {
		case <-kv.doneCh:
			timer.Stop()
			return
		case <-timer.C:
			cfg := kv.cfgclerk.Query(-1)
			kv.cmu.Lock()
			upd := cfg.Num > kv.config.Num
			kv.cmu.Unlock()
			if upd {
				kv.rf.Start(cfg)
			}
		}
		timer.Reset(d)
	}
}

func (kv *ShardKV) sender() {
	for {
		waiting := false
		for i := 0; i < shardctrler.NShards; i++ {
			waiting = waiting || kv.sendShard(i)
		}
		d := time.Millisecond * sendLongMs
		if waiting {
			d = time.Millisecond * sendShortMs
		}
		timer := time.NewTimer(d)
		select {
		case <-kv.doneCh:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
