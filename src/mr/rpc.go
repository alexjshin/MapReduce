package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type TaskStatus int

const (
	IDLE TaskStatus = iota
	IN_PROGRESS
	COMPLETED
)

type Task struct {
	Status 		TaskStatus
	WorkerId 	string
	StartedAt 	time.Time
}

type MapTask struct {
	FileName 	string
	NReduce 	int	// needs nReduce input to determine number of interdmediate files to create
	Task
}

type ReduceTask struct {
	// review this struct and understand
	Region 		int
	Locations 	[]string
	Task
}

type TaskType int 

const (
	EXIT TaskType = iota
	WAIT
	MAP
	REDUCE
)

/*
RPC Definitions
*/

type TaskResponse struct {
	Type 		TaskType
	MapTask 	MapTask
	ReduceTask 	ReduceTask
}

type RequestTaskReply struct {
	Response TaskResponse
	NReduce  int
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
