package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func saveMapResult(mapid, reduceid int, kvs []KeyValue, wg *sync.WaitGroup) {
	defer wg.Done()
	tmpname := "mr-map-" + strconv.Itoa(mapid) + "-" + strconv.Itoa(reduceid)
	outfile, err := ioutil.TempFile(".", tmpname+"*.json")
	if err != nil {
		log.Printf("cannot open temp file %v", tmpname)
		return
	}
	enc := json.NewEncoder(outfile)
	for _, kv := range kvs {
		err := enc.Encode(&kv)
		if err != nil {
			log.Printf("cannot encode %v", tmpname)
			outfile.Close()
			return
		}
	}
	outfile.Close()
	// log.Printf("Saved imtermediate result to %v", outfile.Name())
	err = os.Rename(outfile.Name(), tmpname+".json")
	if err != nil {
		os.Remove(outfile.Name())
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	lastJob := 0
	for {
		args := MrRpcArgs{lastJob}
		reply := MrRpcReply{}
		ok := call("Coordinator.RequestJob", &args, &reply)
		if ok {
			lastJob = reply.JobId
			switch {
			case reply.JobId == 0:
				log.Println("Received no job reply")
				return
			case reply.JobId < 0:
				log.Println("Start map job", -reply.JobId-1)
				file, err := os.Open(reply.JobLoad)
				if err != nil {
					log.Printf("cannot open %v", reply.JobLoad)
					continue
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Printf("cannot read %v", reply.JobLoad)
					file.Close()
					continue
				}
				file.Close()
				kva := mapf(reply.JobLoad, string(content))
				kvsplit := make([][]KeyValue, reply.JobCount)
				for _, kv := range kva {
					hash := ihash(kv.Key) % reply.JobCount
					kvsplit[hash] = append(kvsplit[hash], kv)
				}
				var wg sync.WaitGroup
				for i := 0; i < reply.JobCount; i++ {
					wg.Add(1)
					go saveMapResult(-reply.JobId, i+1, kvsplit[i], &wg)
				}
				wg.Wait()
			case reply.JobId > 0:
				log.Println("Start reduce job", reply.JobId-1)
				var kva []KeyValue
				for i := 0; i < reply.JobCount; i++ {
					filename := "mr-map-" + strconv.Itoa(i+1) + "-" + strconv.Itoa(reply.JobId) + ".json"
					file, err := os.Open(filename)
					if err != nil {
						log.Printf("cannot open %v", filename)
						continue
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
					file.Close()
				}
				tmpname := "mr-out-" + strconv.Itoa(reply.JobId)
				ofile, err := ioutil.TempFile(".", tmpname+"*.txt")
				if err != nil {
					log.Printf("cannot open temp file %v", tmpname)
					continue
				}
				sort.Sort(ByKey(kva))
				for i := 0; i < len(kva); {
					values := []string{kva[i].Value}
					j := i + 1
					for ; j < len(kva) && kva[j].Key == kva[i].Key; j++ {
						values = append(values, kva[j].Value)
					}
					output := reducef(kva[i].Key, values)
					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
					i = j
				}
				ofile.Close()
				err = os.Rename(ofile.Name(), tmpname)
				if err != nil {
					os.Remove(ofile.Name())
				}
			}
		} else {
			break
		}
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
