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
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return
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
	}

}

func doRecude(res TaskResponse, reducef func(string, []string) string) {
	reduceId := res.ReduceId
	intermediate := []KeyValue{}
	for mapId := res.MapIdStart; mapId < res.MapIdEnd; mapId++ {
		tmpFileName := fmt.Sprintf("mr-%v-%v", mapId, reduceId)
		file, err := os.Open(tmpFileName)
		if err != nil {
			log.Fatalf("cannot open %v", tmpFileName)
			break
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	// 在文件写完之后，再rename成目标文件，以防有人读到不完整的文件
	oname := fmt.Sprintf("./mr-out-%v", reduceId)
	// ofile, _ := ioutil.TempFile(os.TempDir(), "mr-temp")
	ofile, ok := ioutil.TempFile("./", "mr-temp")
	if ok != nil {
		log.Fatalf("can create temp file")
	}
	// ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tmpFilename := ofile.Name()
	ofile.Close()
	err := os.Rename(tmpFilename, oname)
	if err != nil {
		log.Fatalf("rename fial %v %v", tmpFilename, oname)
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

	go func() {
		for {
			time.Sleep(time.Second * HEART_BEAT_INTERVAL)
			req := HeartBeatRequest{}
			req.WorkerId = WorkerId
			call("Coordinator.OnHeartBeat", &req, &HeartBeatResponse{})
		}
	}()

	for {
		req := TaskRequest{}
		res := TaskResponse{}
		req.WorkerId = WorkerId
		call("Coordinator.OnTaskRequest", &req, &res)
		if res.Type == MAP_TASK {
			doMap(res, mapf)
			completeTask(res)
		} else if res.Type == REDUCE_TASK {
			doRecude(res, reducef)
			completeTask(res)
		} else if res.Type == END_TASK {
			break
		} else if res.Type == WAIT_TASK {
			time.Sleep(time.Second)
		} else {
			log.Fatalf("unknown task type")
			break
		}

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

