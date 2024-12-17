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
	Index		int
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
	EXIT
)

/*
RPC Definitions
*/

// RPC Request/Response structures
type TaskRequestArgs struct {
    WorkerId string
}

type TaskRequestReply struct {
    TaskType   TaskType
    MapTask    *MapTask
    ReduceTask *ReduceTask
    NReduce    int
}

type TaskCompletionArgs struct {
    TaskType   TaskType
    MapTask    *MapTask
    ReduceTask *ReduceTask
    WorkerId   string    // Added for better task tracking
}

type TaskCompletionReply struct {
    Success bool   // Added for acknowledgment
    Error   string // Added for error reporting
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
