package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType int

const (
	END_TASK    TaskType = 0
	MAP_TASK    TaskType = 1
	REDUCE_TASK TaskType = 2
	WAIT_TASK   TaskType = 3
)

type TaskStatus int

const (
	TODO  TaskStatus = 0
	DOING TaskStatus = 1
	DONE  TaskStatus = 2
)

type RegisterRequest struct {
}

type RegisterRespnse struct {
	WorkerId int
	NReduce  int
}

type TaskRequest struct {
	WorkerId int
}

type TaskResponse struct {
	Type TaskType

	// for map tasks
	Filename string
	MapId    int

	// for reduce tasks
	ReduceId   int
	MapIdStart int
	MapIdEnd   int
}

type StatusRequest struct {
	Type     TaskType
	MapId    int
	ReduceId int
	Status   TaskStatus
}

type StatusReSponse struct {
}

type HeartBeatRequest struct {
	WorkerId       int
}

type HeartBeatResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
