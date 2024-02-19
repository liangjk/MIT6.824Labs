package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	UNSTARTED = 0
	DONE      = -1
	CRASHINT  = 10
)

type Coordinator struct {
	// Your definitions here.
	files                 []string
	mapState, reduceState []int
	mapCount, reduceCount int
	mutex                 sync.Mutex
}

//
// Your code here -- RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) RequestJob(args *MrRpcArgs, reply *MrRpcReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	switch {
	case args.JobId < 0:
		x := -args.JobId - 1
		if c.mapState[x] != DONE {
			c.mapState[x] = DONE
			c.mapCount--
		}
		log.Println("Finish map job", x)
	case args.JobId > 0:
		x := args.JobId - 1
		if c.reduceState[x] != DONE {
			c.reduceState[x] = DONE
			c.reduceCount--
		}
		log.Println("Finish reduce job", x)
	}
	if c.mapCount == 0 {
		if c.reduceCount == 0 {
			reply.JobId = 0
			return nil
		}
		for i, flag := range c.reduceState {
			if flag == UNSTARTED {
				c.reduceState[i] = int(time.Now().Unix())
				reply.JobId = i + 1
				reply.JobCount = len(c.mapState)
				return nil
			}
		}
		for i, flag := range c.reduceState {
			if flag != DONE {
				now := int(time.Now().Unix())
				if now-c.reduceState[i] < CRASHINT {
					continue
				}
				c.reduceState[i] = now
				reply.JobId = i + 1
				reply.JobCount = len(c.mapState)
				return nil
			}
		}
		reply.JobId = 0
		reply.JobLoad = "Reduce"
		return nil
	}
	for i, flag := range c.mapState {
		if flag == UNSTARTED {
			c.mapState[i] = int(time.Now().Unix())
			reply.JobId = -(i + 1)
			reply.JobCount = len(c.reduceState)
			reply.JobLoad = c.files[i]
			return nil
		}
	}
	for i, flag := range c.mapState {
		if flag != DONE {
			now := int(time.Now().Unix())
			if now-c.mapState[i] < CRASHINT {
				continue
			}
			c.mapState[i] = now
			reply.JobId = -(i + 1)
			reply.JobCount = len(c.reduceState)
			reply.JobLoad = c.files[i]
			return nil
		}
	}
	reply.JobId = 0
	reply.JobLoad = "Map"
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// log.Println("Coordinator Done() called")
	ret := false

	// Your code here.
	c.mutex.Lock()
	if c.mapCount == 0 && c.reduceCount == 0 {
		ret = true
	}
	c.mutex.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, reduceCount: nReduce}
	len := len(files)
	c.mapState = make([]int, len)
	c.reduceState = make([]int, nReduce)
	c.mapCount = len

	c.server()
	log.Println("Coordinator server started")
	return &c
}
