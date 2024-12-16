package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	files 					[]string
	nReduce 				int

	mapTasks 				[]MapTask
	reduceTasks 			[]ReduceTask

	mapTasksRemaining 		int
	reduceTasksRemaining 	int

	mu 						sync.Mutex
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

// RPC Handlers

//
// Workers call NotifyComplete to tell the coordinator that they've finished a task.
// This lets the coordinator update its count of remaining tasks and task status
//
func (c *Coordinator) NotifyComplete(args *TaskCompletionArgs, reply *TaskCompletionReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case MAP:
		if args.MapTask.Status == IN_PROGRESS {
			args.MapTask.Status = COMPLETED
			c.mapTasksRemaining--
		}	
	case REDUCE:
		if args.ReduceTask.Status == IN_PROGRESS {
			args.ReduceTask.Status = COMPLETED
			c.reduceTasksRemaining--
		}
	}
	return nil
}

func (c *Coordinator) RequestTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First check if all map tasks are done
	if c.mapTasksRemaining > 0 {
		// Check for timed-out tasks and reassign
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == IN_PROGRESS && time.Since(c.mapTasks[i].StartedAt) > 10*time.Second {
				c.mapTasks[i].Status = IDLE
				c.mapTasks[i].WorkerId = args.WorkerId
				c.mapTasks[i].StartedAt = time.Now()

				reply.TaskType = MAP
				reply.MapTask = &c.mapTasks[i]
				reply.NReduce = c.nReduce
				return nil
			}	
		}

		// Assign available IDLE map tasks
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == IDLE {
				c.mapTasks[i].Status = IN_PROGRESS
				c.mapTasks[i].WorkerId = args.WorkerId
				c.mapTasks[i].StartedAt = time.Now()

				reply.TaskType = MAP
				reply.MapTask = &c.mapTasks[i]
				reply.NReduce = c.nReduce
				return nil
			}
		}

		reply.TaskType = WAIT
		return nil
	}

	// Check if all reduce tasks are done
	if c.reduceTasksRemaining > 0 {
		// Check for timed-out tasks and reassign
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == IN_PROGRESS && time.Since(c.reduceTasks[i].StartedAt) > 10*time.Second {
				c.reduceTasks[i].Status = IDLE
				c.reduceTasks[i].WorkerId = args.WorkerId
				c.reduceTasks[i].StartedAt = time.Now()

				reply.TaskType = REDUCE
				reply.ReduceTask = &c.reduceTasks[i]
				return nil
			}
		}

		// Assign available IDLE reduce tasks
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == IDLE {
				c.reduceTasks[i].Status = IN_PROGRESS
				c.reduceTasks[i].WorkerId = args.WorkerId
				c.reduceTasks[i].StartedAt = time.Now()

				reply.TaskType = REDUCE
				reply.ReduceTask = &c.reduceTasks[i]
				return nil
			}
		}

		reply.TaskType = WAIT
		return nil
	}

	// All tasks are done
	return fmt.Errorf("all tasks completed")
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
	c.mu.Lock()
    defer c.mu.Unlock()
    return c.mapTasksRemaining == 0 && c.reduceTasksRemaining == 0
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
	c.files = files
	c.nReduce = nReduce
	c.mapTasks = make([]MapTask, len(files))
	c.reduceTasks = make([]ReduceTask, nReduce)
	c.mapTasksRemaining = len(files)
	c.reduceTasksRemaining = nReduce
	// c.mu = sync.Mutex{}

	// Initialize map tasks
	for i, file := range files {
		c.mapTasks[i] = MapTask{
			FileName: file,
			NReduce: nReduce,
			Task: Task{Status: IDLE},
		}
	}

	// Initialize reduce tasks
	for i:= range c.reduceTasks {
		locations := make([]string , len(files))
		for j := range files {
			locations[j] = fmt.Sprintf("mr-%d-%d", j, i)
		}

		c.reduceTasks[i] = ReduceTask{
			Region: i,
			Locations: locations,
			Task: Task{Status: IDLE},
		}
	}

	fmt.Printf("Coordinator initialized with %v Map Tasks\n", len(files))
	fmt.Printf("Coordinator initialized with %v Reduce Tasks\n", nReduce)
	c.server()
	return &c
}
