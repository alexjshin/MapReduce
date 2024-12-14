package mr

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	"github.com/google/uuid"
)

// Worker represents a worker node in the MapReduce system.
type Worker struct {
	WorkerId      string             // Unique ID assigned to this worker
	RegionToPairs map[int][]KeyValue // Map storing key-value pairs categorized by regions
}

// Sockname generates a unique UNIX socket name for the worker.
// The socket name is based on the worker's unique ID.
func (w *Worker) Sockname() string {
	s := "/var/tmp/5840-mr-" // Common prefix for socket names
	s += w.WorkerId          // Append the unique WorkerId
	return s
}

// server initializes and starts an RPC server on a UNIX socket.
func (w *Worker) server() {
	rpc.Register(w)                      // Register the Worker to handle RPC requests
	rpc.HandleHTTP()                     // Register HTTP handlers for RPC
	sockname := w.Sockname()             // Generate the unique socket name
	os.Remove(sockname)                  // Remove the socket if it already exists
	l, e := net.Listen("unix", sockname) // Start listening on the UNIX socket
	if e != nil {
		log.Fatal("listen error:", e) // Handle listen error
	}
	go http.Serve(l, nil) // Serve HTTP requests on the UNIX socket
}

// KeyValue represents a key-value pair used in map and reduce tasks.
type KeyValue struct {
	Key   string
	Value string
}

// ihash computes a hash value for a given key using a FNV hash function.
func ihash(key string) int {
	h := fnv.New32a()                  // Create a new hash instance
	h.Write([]byte(key))               // Write the key to the hash
	return int(h.Sum32() & 0x7fffffff) // Return a non-negative hash value
}

// MakeWorker initializes a new worker, sets up the RPC server, and processes tasks.
func MakeWorker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerId := uuid.New().String()[0:6] // Generate a short unique worker ID
	w := Worker{
		WorkerId:      workerId,                 // Assign the worker ID
		RegionToPairs: make(map[int][]KeyValue), // Initialize the region-to-key-value map
	}
	w.server() // Start the worker's RPC server

	// Register the MapTask and ReduceTask types for RPC decoding
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	taskReply := new(MapReduceTaskReply)

	// Continuously request tasks from the coordinator
	for call(coordinatorSock(), "Coordinator.AssignTask", w.WorkerId, taskReply) && taskReply != nil {
		switch task := (*taskReply).(type) {
		case MapTask:
			log.Printf("Processing Map Task: %v", task) // Log processing of MapTask
			if w.processMap(mapf, task.FileName, task.NReduce) {
				acknowledgeTask(task) // Acknowledge completion of the task
			}

		case ReduceTask:
			log.Printf("Processing Reduce Task: %v", task) // Log processing of ReduceTask
			if w.processReduce(reducef, task.Region, task.Locations) {
				acknowledgeTask(task) // Acknowledge completion of the task
			}

		default:
			log.Printf("Unknown task type %T, terminating program.", task) // Handle unknown task types
			return
		}
		time.Sleep(2 * time.Second) // Sleep for a while before requesting the next task
	}
}

// processMap handles the execution of a MapTask.
func (w *Worker) processMap(mapf func(string, string) []KeyValue, fileName string, nReduce int) bool {
	file, err := os.Open(fileName) // Open the input file
	if err != nil {
		log.Printf("Cannot open file %v: %v", fileName, err) // Log error if file cannot be opened
		return false
	}
	defer file.Close() // Ensure the file is closed when done

	content, err := io.ReadAll(file) // Read the entire file content
	if err != nil {
		log.Printf("Cannot read file %v: %v", fileName, err) // Log error if file cannot be read
		return false
	}

	// Apply the map function to generate key-value pairs
	kva := mapf(fileName, string(content))
	for _, kv := range kva {
		region := ihash(kv.Key) % nReduce                             // Determine the region by hashing the key
		w.RegionToPairs[region] = append(w.RegionToPairs[region], kv) // Store key-value pairs in the appropriate region
	}
	log.Printf("Map task completed for file %v", fileName)
	return true
}

// processReduce handles the execution of a ReduceTask.
func (w *Worker) processReduce(reducef func(string, []string) string, region int, locations []string) bool {
	intermediate := []KeyValue{}

	// Read intermediate key-value pairs from the input locations
	for _, location := range locations {
		file, err := os.Open(location) // Open each intermediate file
		if err != nil {
			log.Printf("Cannot open intermediate file %v: %v", location, err) // Log error if file cannot be opened
			return false
		}
		defer file.Close()

		decoder := gob.NewDecoder(file) // Create a decoder to read the key-value pairs
		var kva []KeyValue
		if err := decoder.Decode(&kva); err != nil {
			log.Printf("Cannot decode intermediate file %v: %v", location, err) // Log error if decoding fails
			return false
		}
		intermediate = append(intermediate, kva...) // Collect all decoded key-value pairs
	}

	output := make(map[string][]string)
	for _, kv := range intermediate {
		output[kv.Key] = append(output[kv.Key], kv.Value) // Group values by their keys
	}

	outfile, err := os.Create(fmt.Sprintf("mr-out-%d", region)) // Create an output file for the reduce results
	if err != nil {
		log.Printf("Cannot create output file for region %d: %v", region, err) // Log error if file cannot be created
		return false
	}
	defer outfile.Close()

	for key, values := range output {
		result := reducef(key, values)               // Apply the reduce function
		fmt.Fprintf(outfile, "%v %v\n", key, result) // Write the result to the output file
	}
	log.Printf("Reduce task completed for region %d", region)
	return true
}

// acknowledgeTask notifies the coordinator that a task has been successfully completed.
func acknowledgeTask(task interface{}) {
	tmpReply := 0
	if !call(coordinatorSock(), "Coordinator.CompleteTask", task, &tmpReply) {
		log.Printf("Failed to acknowledge task completion for %v", task)
	}
}

// call handles the RPC call to the coordinator.
func call(sockname string, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("unix", sockname) // Dial the coordinator using UNIX socket
	if err != nil {
		log.Printf("Dialing error: %v", err) // Log dialing error if connection fails
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err != nil { // Perform the RPC call
		log.Printf("RPC call error: %v", err) // Log RPC call error if it fails
		return false
	}
	return true
}
