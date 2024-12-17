package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
    Key   string
    Value string
}

// Worker is the main entry point function that matches the original interface
func Worker(
    mapf func(string, string) []KeyValue,
    reducef func(string, []string) string,
) {
    w := &worker{
        id:      fmt.Sprintf("worker-%v-%v", os.Getpid(), time.Now().UnixNano()),
        mapf:    mapf,
        reducef: reducef,
    }
    w.run()
}

// private worker struct for internal implementation
type worker struct {
    id      string
    mapf    func(string, string) []KeyValue
    reducef func(string, []string) string
}

func (w *worker) run() {
    for {
        task, err := w.requestTask()
        if err != nil {
            log.Printf("Error requesting task: %v", err)
            return
        }

        var taskErr error
        switch task.TaskType {
        case MAP:
            taskErr = w.doMap(task.MapTask)
        case REDUCE:
            taskErr = w.doReduce(task.ReduceTask)
        case WAIT:
            time.Sleep(time.Second)
            continue
        case EXIT:
            return
        }

        if taskErr != nil {
            log.Printf("Error executing task: %v", taskErr)
            continue
        }

        if err := w.notifyTaskComplete(task); err != nil {
            log.Printf("Error notifying task completion: %v", err)
        }
    }
}

func (w *worker) requestTask() (*TaskRequestReply, error) {
    args := TaskRequestArgs{WorkerId: w.id}
    reply := TaskRequestReply{}

    if err := call("Master.RequestTask", &args, &reply); err != nil {
        return nil, fmt.Errorf("RPC RequestTask error: %v", err)
    }

    return &reply, nil
}

func (w *worker) notifyTaskComplete(task *TaskRequestReply) error {
    args := TaskCompletionArgs{
        WorkerId: w.id,
        TaskType: task.TaskType,
    }

    if task.TaskType == MAP {
        args.MapTask = task.MapTask
    } else if task.TaskType == REDUCE {
        args.ReduceTask = task.ReduceTask
    }

    reply := TaskCompletionReply{}
    if err := call("Master.NotifyComplete", &args, &reply); err != nil {
        return fmt.Errorf("RPC NotifyComplete error: %v", err)
    }

    if !reply.Success {
        return fmt.Errorf("task completion failed: %s", reply.Error)
    }

    return nil
}

func (w *worker) doMap(task *MapTask) error {
    // Read input file
    content, err := ioutil.ReadFile(task.FileName)
    if err != nil {
        return fmt.Errorf("cannot read %v: %v", task.FileName, err)
    }

    // Apply map function to get key-value pairs
    kva := w.mapf(task.FileName, string(content))

    // Create intermediate files
    intermediateFiles := make([]*os.File, task.NReduce)
    encoders := make([]*json.Encoder, task.NReduce)

    for i := 0; i < task.NReduce; i++ {
        tempFile, err := ioutil.TempFile("", "map-")
        if err != nil {
            return fmt.Errorf("cannot create temp file: %v", err)
        }
        intermediateFiles[i] = tempFile
        encoders[i] = json.NewEncoder(tempFile)
    }

    // Write key-value pairs to appropriate intermediate files
    for _, kv := range kva {
        reduceTaskNum := ihash(kv.Key) % task.NReduce
        if err := encoders[reduceTaskNum].Encode(&kv); err != nil {
            return fmt.Errorf("cannot encode key-value pair: %v", err)
        }
    }

    // Close and rename all intermediate files
    for i, f := range intermediateFiles {
		f.Close()
		finalName := fmt.Sprintf("mr-%d-%d", task.Task.Index, i)  // Using task Index instead of WorkerId
		if err := os.Rename(f.Name(), finalName); err != nil {
			return fmt.Errorf("cannot rename temp file: %v", err)
		}
	}

    return nil
}

func (w *worker) doReduce(task *ReduceTask) error {
    // Read all intermediate files
    var intermediate []KeyValue
    for _, filename := range task.Locations {
        file, err := os.Open(filename)
        if err != nil {
            // Skip files that don't exist (from failed map tasks)
            continue
        }
        dec := json.NewDecoder(file)
        for {
            var kv KeyValue
            if err := dec.Decode(&kv); err != nil {
                break
            }
            intermediate = append(intermediate, kv)
        }
        file.Close()
    }

    // Sort by key
    sort.Slice(intermediate, func(i, j int) bool {
        return intermediate[i].Key < intermediate[j].Key
    })

    // Create output file
    tempFile, err := ioutil.TempFile("", "reduce-")
    if err != nil {
        return fmt.Errorf("cannot create temp file: %v", err)
    }
    
    // Process sorted key-value pairs
    i := 0
    for i < len(intermediate) {
        j := i + 1
        for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
            j++
        }
        
        values := make([]string, 0, j-i)
        for k := i; k < j; k++ {
            values = append(values, intermediate[k].Value)
        }
        
        // Apply reduce function and write output
        output := w.reducef(intermediate[i].Key, values)
        fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
        
        i = j
    }

    // Close and rename output file
    tempFile.Close()
    outputFile := fmt.Sprintf("mr-out-%d", task.Region)
    if err := os.Rename(tempFile.Name(), outputFile); err != nil {
        return fmt.Errorf("cannot rename output file: %v", err)
    }

    return nil
}

// ihash returns a hash value for a key
func ihash(key string) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32() & 0x7fffffff)
}

// call sends an RPC request to the master
func call(rpcname string, args interface{}, reply interface{}) error {
    sockname := masterSock()
    c, err := rpc.DialHTTP("unix", sockname)
    if err != nil {
        return err
    }
    defer c.Close()

    if err := c.Call(rpcname, args, reply); err != nil {
        return err
    }

    return nil
}