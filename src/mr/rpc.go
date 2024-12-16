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
	WAIT TaskType = iota
	MAP
	REDUCE
)

/*
RPC Definitions
*/

// RPC structures for requesting tasks
type TaskRequestArgs struct {
    WorkerId string
}

type TaskRequestReply struct {
    TaskType   TaskType
    MapTask    *MapTask
    ReduceTask *ReduceTask
    NReduce    int
}

// RPC structures for completing tasks
type TaskCompletionArgs struct {
    TaskType   TaskType
    MapTask    *MapTask
    ReduceTask *ReduceTask
}

type TaskCompletionReply struct {
    // Empty for now, could add acknowledgment info if needed
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
