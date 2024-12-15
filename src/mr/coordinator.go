package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)


type Coordinator struct {
	// Your definitions here.
	MapTasks []MapTask
	ReduceTasks []ReduceTask


	MapTasksRemaining int
	ReduceTasksRemaining int

	mu sync.Mutex
}

/*
- need to ensure tasks aren't duplicated across workers
- need to keep track of the status of each task
- need to keep track of task to worker assignments
- if worker crashes, need to reassign task to another worker
- It will also be helpful to have a sense of how long workers are taking to finish a task. 
If a task takes more than 10 seconds to finish, we’ll assume the worker is having difficulty 
and reassign the task to another worker.
*/

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
// once all map and reduce tasks are done
//
func (c *Coordinator) Done() bool {
	ret := false
	if c.MapTasksRemaining == 0 && c.ReduceTasksRemaining == 0 {
		ret = true
	} 
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// need to create 1 intermediate file for each nReduce bucket where intermediate keys are read from
// hash function assigns each key to one of the nReduce buckets
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapTasks = make([]MapTask, len(files))
	c.MapTasksRemaining = len(files)
	c.ReduceTasks = make([]ReduceTask, nReduce)
	c.ReduceTasksRemaining = nReduce
	c.mu = sync.Mutex{}

	// Initialize map tasks
	for i, file := range files {
		c.MapTasks[i] = MapTask{
			FileName: file,
			NReduce: nReduce,
			Task: Task{Status: IDLE},
		}
	}

	// Initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = ReduceTask{
			Region: nReduce + i,
			Task: Task{Status: IDLE},
		}
	}

	fmt.Printf("Coordinator initialized with %v Map Tasks\n", len(files))
	fmt.Printf("Coordinator initialized with %v Reduce Tasks\n", nReduce)
	c.server()
	return &c
}
