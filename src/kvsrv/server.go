package kvsrv

import (
	"log"
	"sync"
	"time"
)

const (
	Debug      = false
	NoLog      = false
	GET        = 0
	PUT        = 1
	APPEND     = 2
	LogTimeout = 100
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Log struct {
	client, seq int64
	retvalue    string
	acctime     int64
	ack         bool
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]string
	logs []Log
}

func (kv *KVServer) cleanLog() {
	done := len(kv.logs)
	now := time.Now().UnixMilli()
	for i, kvlog := range kv.logs {
		if !kvlog.ack && now-kvlog.acctime < LogTimeout {
			done = i
			break
		}
	}
	kv.logs = kv.logs[done:]
}

func (kv *KVServer) addLog(client, seq int64, op uint8, key, value string) (ret string) {
	for i, kvlog := range kv.logs {
		if client == kvlog.client {
			if seq == kvlog.seq {
				kv.logs[i].acctime = time.Now().UnixMilli()
				return kvlog.retvalue
			}
			kv.logs[i].ack = true
		}
	}
	kv.cleanLog()
	switch op {
	case PUT:
		kv.data[key] = value
	case APPEND:
		ret = kv.data[key]
		kv.data[key] = ret + value
	}
	newlog := Log{client, seq, ret, time.Now().UnixMilli(), false}
	kv.logs = append(kv.logs, newlog)
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if NoLog {
		reply.Value = kv.data[args.Key]
		return
	}
	for i, kvlog := range kv.logs {
		if args.Cltno == kvlog.client {
			kv.logs[i].ack = true
		}
	}
	kv.cleanLog()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if NoLog {
		reply.Value = kv.data[args.Key]
		kv.data[args.Key] = args.Value
		return
	}
	reply.Value = kv.addLog(args.Cltno, args.Seqno, PUT, args.Key, args.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if NoLog {
		reply.Value = kv.data[args.Key]
		kv.data[args.Key] += args.Value
		return
	}
	reply.Value = kv.addLog(args.Cltno, args.Seqno, APPEND, args.Key, args.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	if !NoLog {
		kv.logs = make([]Log, 0)
	}

	return kv
}
