package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mu            sync.Mutex
	filenameArray []string
	finished      map[string]bool
	workers       []int
	nReduce       int
	mapId         int
	done          bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) OnRegister(req *RegisterRequest, res *RegisterRespnse) error {
	c.workers = append(c.workers, req.WokerId)
	return nil
}
func (c *Coordinator) OnMapReduceRequest(req *MapReduceRequest, res *MapReduceRespnse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	hasUnFinished := false
	for _, filename := range c.filenameArray {
		if !c.finished[filename] {
			res.Type = "MAP"
			res.Filename = filename
			res.MapId = c.mapId
			c.mapId++
			c.finished[filename] = true
			hasUnFinished = true
			break
		}
	}
	if !hasUnFinished {
		res.Done = true
		c.done = true
	}
	return nil
}

// func (c *Coordinator) OnMapRequest(req *MapRequest, res *MapRespnse) error {
// 	c.mu.Lock()
// 	defer c.mu.Unlock()
// 	for _, filename := range c.filenameArray {
// 		if !c.finished[filename] {
// 			res.Filename = filename
// 			res.MapId = c.mapId
// 			c.mapId++
// 			c.finished[filename] = true
// 			break
// 		}
// 	}
// 	return nil
// }

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
	c.mu.Lock()
	done := c.done
	c.mu.Unlock()
	log.Printf("done: %v", done)
	return done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.finished = make(map[string]bool)
	for _, filename := range files {
		log.Printf("%v ", filename)
		c.filenameArray = append(c.filenameArray, filename)
		c.finished[filename] = false
	}

	// Your code here.

	c.server()
	return &c
}
