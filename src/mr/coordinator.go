package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"
import "strconv"

type State int

const Debug = false

func debugPrint(s string) {
  if Debug {
    fmt.Println(s)
  }
}

const (
  Idle State = 0
  Inprogress State = 1
  Completed State = 2
)

type Coordinator struct {
  // Number of map tasks
  nmap int
  // Number of reduce tasks
  nreduce int
  // Input files' name 
  files []string
  // States recorded for map
  mapstates []State
  // States recorded for reduce
  reducestates []State
  // Whether the entire task has finished
  finished bool
  // Mutex lock to protect all the data
  mu sync.Mutex
  // Only make sense when the task state is not idle
  maptime []time.Time
  reducetime []time.Time


  // Not sure.
  // Number of map tasks has been finished.
  mapDone int
  reduceDone int
}

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

// Assuem we have two different rpcs:
/*
  1. RequestTask -> request a task (either map or reduce) to be executed.
      args: none...
      reply: for a map task, the worker needs to know the file related to the task
      plus how to divide the file into different pieces.
      for a reduce task, the worker needs to know the file related to the task
      for a nil task, some fileds should be set to nil in the reply.
  2. FinishTask -> tell the coordinator that some tasks have been done.
*/

// Find an idle reduce task
// Return the number for the task or -1
// IMPORTANT: Invoke under lock
func (c *Coordinator) scheduleReduceTask() int {
  for i, v := range c.reducestates {
    if v == Idle {
      return i
    } else if v == Inprogress {
      if c.reducetime[i].IsZero(){
        fmt.Println("Error occurs in scheduleReduceTask, time is nil")
      }
      diff := time.Since(c.reducetime[i])
      if diff.Seconds() > 10.0 {
        return i
      }
    }
  }
  return -1
}

func (c *Coordinator) scheduleMapTask() int {
  for i, v := range c.mapstates{
    if v == Idle {
      return i
    } else if v == Inprogress {
      if c.maptime[i].IsZero(){
        fmt.Println("Error occurs in scheduleMapTask, time is nil")
      }
      diff := time.Since(c.maptime[i])
      if diff.Seconds() > 10.0 {
        return i
      }
    }
  }
  return -1

}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
  c.mu.Lock()
  defer c.mu.Unlock()

  if c.mapDone == c.nmap {
    reduceTask := c.scheduleReduceTask()
    if reduceTask == -1 {
      debugPrint(fmt.Sprintf("No task is available now\n map tasks:%v \n reduce tasks:%v", c.mapstates, c.reducestates))
      reply.IsMap = false
      reply.NReduce = c.nreduce
      reply.Files = nil
      reply.TaskNumber = -1
      return nil
    } else {
      // Allocate reduce task to it.
      reply.IsMap = false
      reply.NReduce = c.nreduce
      temp := make([]string, c.nmap)
      for i := 0; i < c.nmap; i ++ {
        a := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceTask)
        temp[i] = a
      }
      reply.Files = temp
      reply.TaskNumber = reduceTask
      // Set the time for its task
      c.reducetime[reduceTask] = time.Now()
      c.reducestates[reduceTask] = Inprogress
      return nil
    }
  } else {
    // Allocate a map task
    maptask := c.scheduleMapTask()
    if maptask == -1 {
      debugPrint(fmt.Sprintf("All the map tasks has been allocated: %v\n", c.mapstates))
      reply.IsMap = true
      reply.NReduce = c.nreduce
      reply.Files = nil
      reply.TaskNumber = -1
      return nil
    } else {
      reply.IsMap = true
      reply.NReduce = c.nreduce
      temp := make([]string, 1)
      temp[0] = c.files[maptask]
      reply.Files = temp
      reply.TaskNumber = maptask
      c.maptime[maptask] = time.Now()
      c.mapstates[maptask] = Inprogress
      return nil
    }
  }
}

func (c *Coordinator) FinishTask(args *FinishTaskRequest, reply *FinishTaskReply) error {
  c.mu.Lock()
  defer c.mu.Unlock()

  tasknumber := args.TaskNumber
  if args.IsMap {
    if c.mapstates[tasknumber] == Completed {
      return nil
    } else {
      // We have finished a new task
      c.mapstates[tasknumber] = Completed
      c.mapDone ++
    }
  } else {
    if c.reducestates[tasknumber] == Completed {
      return nil
    } else {
      c.reducestates[tasknumber] = Completed
      c.reduceDone ++
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
	// Your code here.


	return c.reduceDone == c.nreduce
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.
  // For each file in the files, we create a map task for it.
  mapstates := make([]State, len(files))
  reducestates := make([]State, nReduce)
  maptime := make([]time.Time, len(files))
  reducetime := make([]time.Time, nReduce)

  // Initialize the values in it
  for i := range mapstates {
    mapstates[i] = Idle
  }

  for i := range reducestates {
    reducestates[i] = Idle
  }
  c := Coordinator{
    nmap :len(files),
    nreduce :nReduce,
    files :files,
    mapstates :mapstates,
    reducestates :reducestates,
    finished :false,
    mapDone :0,
    maptime :maptime,
    reducetime :reducetime,
    reduceDone : 0,
  }
  debugPrint(fmt.Sprintf("Coordinator initialization done:%v", c))
	c.server()
	return &c
}
