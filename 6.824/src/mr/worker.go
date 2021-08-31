package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	res := SendMapRequest()
	if res.WorkType == "MAP" {
		fmt.Printf("filename %v\n", res.Filename)

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
		log.Printf("%v\n", len(kva))
		// for k, v := range kva {
		// 	fmt.Printf("%v %v", k, v)
		// }
		//intermediate = append(intermediate, kva...)

	} else if res.WorkType == "REDUCE" {
		fmt.Print("empty")
	} else {
		fmt.Print("unknown work type")
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func SendMapRequest() MapRespnse {

	// declare an argument structure.
	req := MapRequest{}

	// declare a reply structure.
	res := MapRespnse{}

	// send the RPC request, wait for the reply.
	call("Coordinator.OnMapRequest", &req, &res)
	return res
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
