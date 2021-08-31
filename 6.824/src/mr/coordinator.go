package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"

type Coordinator struct {
	// Your definitions here.
	FileMap map[string]bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) OnMapRequest(req *MapRequest, res *MapRespnse) error {
	for key, v := range c.FileMap {
		if !v {
			res.Filename = key
			res.WorkType = "MAP"
			c.FileMap[key] = true
			break
		}
	}
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.FileMap = make(map[string]bool)
	for _, filename := range files {
		fmt.Printf("%v ", filename)
		c.FileMap[filename] = false
	}

	// Your code here.

	c.server()
	return &c
}
