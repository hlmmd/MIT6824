package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var WorkerId int
var NReduce int

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

func doMap(res TaskResponse, mapf func(string, string) []KeyValue) {
	intermediate := make([][]KeyValue, NReduce)
	for i := 0; i < NReduce; i++ {
		intermediate[i] = make([]KeyValue, 0)
	}
	filename := res.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	for _, key := range kva {
		idx := ihash(key.Key) % NReduce
		intermediate[idx] = append(intermediate[idx], key)
	}
	for i := 0; i < len(intermediate); i++ {
		sort.Sort(ByKey(intermediate[i]))
		oname := fmt.Sprintf("mr-%v-%v", res.MapId, i)
		ofile, _ := os.Create(oname)

		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				break
			}
		}

		ofile.Close()

		log.Printf("file: %v, map:%v reduce: %v count: %v", filename, res.MapId, i, len(intermediate[i]))
	}

}

// 完成task后，通知coordinator

func completeTask(res TaskResponse) {
	req := StatusRequest{}
	req.Type = res.Type
	req.Status = DONE
	if res.Type == MAP_TASK {
		req.MapId = res.MapId
	} else if res.Type == REDUCE_TASK {
		req.ReduceId = res.ReduceId
	}
	call("Coordinator.OnCompleteTask", &req, &StatusReSponse{})
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	WorkerId, NReduce = Register()
	log.Printf("id:%v nreduce:%v", WorkerId, NReduce)

	for {
		req := TaskRequest{}
		res := TaskResponse{}
		call("Coordinator.OnTaskRequest", &req, &res)
		if res.Type == MAP_TASK {
			doMap(res, mapf)
		} else if res.Type == REDUCE_TASK {
			log.Println("reduce")
		} else if res.Type == END_TASK {
			break
		} else if res.Type == WAIT_TASK {
			time.Sleep(time.Second)
		} else {
			log.Fatalf("unknown task type")
			break
		}

		completeTask(res)
	}

}

func Register() (int, int) {

	res := RegisterRespnse{}
	call("Coordinator.OnRegister", RegisterRequest{}, &res)
	return res.WorkerId, res.NReduce
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
