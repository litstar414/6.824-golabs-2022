package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
  "sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func handleReduceTask(filenames []string, reducef func(string, []string) string, tasknumber int) {
	// We want to read all the pairs back into a slice
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("Fail to open file:%v\n", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

  // sort the intermediate key pairs
  sort.Sort(ByKey(intermediate))

  oname := "mr-out-" + strconv.Itoa(tasknumber)
  // TODO: handle errors here.
  ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

  ofile.Close()

}

func handleMapTask(filename string, mapf func(string, string) []KeyValue, nreduce int, tasknumber int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	// for each pair, write it into the corresponding bucket.
	container := make([][]KeyValue, nreduce)
	for i, _ := range container {
		container[i] = make([]KeyValue, 0)
	}
	// put each pair into the corresponding bucket
	for _, v := range kva {
		bucket := ihash(v.Key) % nreduce
		container[bucket] = append(container[bucket], v)
	}

	// write it into the file system
	for i := 0; i < nreduce; i++ {
		filename := "mr-" + strconv.Itoa(tasknumber) + "-" + strconv.Itoa(i)
		file, err := os.Create(filename)
		if err != nil {
      // TODO: handle errors
			fmt.Printf("Create file %v failed!\n", filename)
		}
		enc := json.NewEncoder(file)
		for _, kv := range container[i] {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Printf("Encoder failed:%v\n", err)
			}
		}
	}

	// Task done
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		args := RequestTaskArgs{}

		reply := RequestTaskReply{}

		ok := call("Coordinator.RequestTask", &args, &reply)
		if ok {
			if reply.TaskNumber == -1 {
				time.Sleep(1 * time.Second)
				continue
			}
			if reply.IsMap {
				filename := reply.Files[0]
				handleMapTask(filename, mapf, reply.NReduce, reply.TaskNumber)
				finishArgs := FinishTaskRequest{
					IsMap:      true,
					TaskNumber: reply.TaskNumber,
				}
				finishReply := FinishTaskReply{}
				ok := call("Coordinator.FinishTask", &finishArgs, &finishReply)
				if !ok {
					//fmt.Printf("worker closed, bye!\n")
					break
				}
			} else {
				// handle reduce task
        handleReduceTask(reply.Files, reducef, reply.TaskNumber)
        finishArgs := FinishTaskRequest {
          IsMap: false,
          TaskNumber: reply.TaskNumber,
        }
        finishReply := FinishTaskReply{}
				ok := call("Coordinator.FinishTask", &finishArgs, &finishReply)
				if !ok {
					//fmt.Printf("worker closed, bye!\n")
					break
				}
			}
		} else {
			// Server closed.
			//fmt.Printf("worker closed, bye!\n")
			break
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
