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

type TaskInfo struct {
	status   TaskStatus
	workerId int
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	mapTaskStatus    []TaskInfo
	reduceTaskStatus []TaskInfo
	workerAliveStamp map[int]int64

	filenameArray []string
	workerId      int
	nReduce       int

	doneMap bool
	done    bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

const (
	HEART_BEAT_INTERVAL = 5
	TIMEOUT             = 10
)

func (c *Coordinator) OnHeartBeat(req *HeartBeatRequest, res *HeartBeatResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.workerAliveStamp[req.WorkerId]
	if !ok {
		return nil
	}
	// 更新timestamp
	c.workerAliveStamp[req.WorkerId] = time.Now().Unix()

	return nil
}

func (c *Coordinator) OnRegister(req *RegisterRequest, res *RegisterRespnse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workerId++
	res.NReduce = c.nReduce
	res.WorkerId = c.workerId
	c.workerAliveStamp[res.WorkerId] = time.Now().Unix()
	return nil
}

func (c *Coordinator) finishAllMapTasks() bool {
	if c.doneMap {
		return true
	}
	for _, info := range c.mapTaskStatus {
		if info.status != DONE {
			return false
		}
	}
	return true
}

func (c *Coordinator) finishAllReduceTasks() bool {
	if c.done {
		return true
	}
	for _, info := range c.reduceTaskStatus {
		if info.status != DONE {
			return false
		}
	}
	return true
}

func (c *Coordinator) getUnStartedMapTask() int {
	for mapId, info := range c.mapTaskStatus {
		if info.status == TODO {
			return mapId
		}
	}
	return -1
}

func (c *Coordinator) getUnStartedReduceTask() int {
	for mapId, info := range c.reduceTaskStatus {
		if info.status == TODO {
			return mapId
		}
	}
	return -1
}

func (c *Coordinator) sendMapTask(mapId int, req *TaskRequest, res *TaskResponse) error {

	if c.mapTaskStatus[mapId].status != TODO {
		// TODO 如何定义error?
		return nil
	}
	c.mapTaskStatus[mapId].status = DOING
	c.mapTaskStatus[mapId].workerId = req.WorkerId

	res.Type = MAP_TASK
	res.Filename = c.filenameArray[mapId]
	res.MapId = mapId
	return nil
}

func (c *Coordinator) sendReduceTask(reduceId int, req *TaskRequest, res *TaskResponse) error {

	if c.reduceTaskStatus[reduceId].status != TODO {
		// TODO 如何定义error?
		return nil
	}
	c.reduceTaskStatus[reduceId].status = DOING
	c.reduceTaskStatus[reduceId].workerId = req.WorkerId

	res.Type = REDUCE_TASK
	res.ReduceId = reduceId
	res.MapIdStart = 0
	res.MapIdEnd = len(c.mapTaskStatus)
	return nil
}

func (c *Coordinator) OnCompleteTask(req *StatusRequest, res *StatusReSponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.Type == MAP_TASK {
		c.mapTaskStatus[req.MapId].status = DONE
	} else if req.Type == REDUCE_TASK {
		c.reduceTaskStatus[req.ReduceId].status = DONE
	}

	return nil
}

func (c *Coordinator) OnTaskRequest(req *TaskRequest, res *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// worker 超时后，coordinator断开了连接，之后再收到该worker的消息便忽略
	_, ok := c.workerAliveStamp[req.WorkerId]
	if !ok {
		return nil
	}
	if c.finishAllMapTasks() {
		if c.finishAllReduceTasks() {
			res.Type = END_TASK
			c.done = true
		} else {
			reduceId := c.getUnStartedReduceTask()
			if reduceId == -1 {
				res.Type = WAIT_TASK
				//	log.Printf("Wait for doing Reduce")
			} else {
				c.sendReduceTask(reduceId, req, res)
			}
		}
	} else {
		mapId := c.getUnStartedMapTask()
		if mapId == -1 {
			res.Type = WAIT_TASK
			//	log.Printf("Wait for doing Map")
		} else {
			c.sendMapTask(mapId, req, res)
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.filenameArray = files
	c.mapTaskStatus = make([]TaskInfo, len(files))
	c.reduceTaskStatus = make([]TaskInfo, nReduce)
	c.workerAliveStamp = make(map[int]int64)

	// Your code here.

	go func() {
		for {
			nowTimestamp := time.Now().Unix()
			c.mu.Lock()
			for workerId, ts := range c.workerAliveStamp {
				timeDiff := nowTimestamp - ts
				// 当一个worker fail的时候，重置其所有已经完成的map task
				// reduce task不需要重置，因为其结果存放在gfs上，可以访问到
				if timeDiff >= TIMEOUT {
					delete(c.workerAliveStamp, workerId)

					for i := 0; i < len(c.mapTaskStatus); i++ {
						if c.mapTaskStatus[i].workerId == workerId {
							c.mapTaskStatus[i].status = TODO
						}
					}

					for i := 0; i < len(c.reduceTaskStatus); i++ {
						if c.reduceTaskStatus[i].workerId == workerId && c.reduceTaskStatus[i].status == DOING {
							c.reduceTaskStatus[i].status = TODO
						}
					}
				}
			}
			c.mu.Unlock()
			time.Sleep(time.Second * HEART_BEAT_INTERVAL)
		}
	}()

	c.server()
	return &c
}
