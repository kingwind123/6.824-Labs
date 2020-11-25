package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	RequestTask(mapf, reducef)

}

func mapTask(mapf func(string, string) []KeyValue, reply *TaskReply) {
	filename := reply.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)

	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	var kva []KeyValue = mapf(filename, string(content))
	mapNum := reply.TaskNum

	// create a set for output files
	var member void
	set := make(map[int]map[string]void)

	for _, kv := range kva {
		reduceNum := ihash(kv.Key) % reply.NReduce
		oname := fmt.Sprintf("mr-%d-%d", mapNum, reduceNum)
		_, exists := set[reduceNum]
		if !exists {
			set[reduceNum] = make(map[string]void)
		}
		set[reduceNum][oname] = member
		// If the file doesn't exist, create it, or append to the file
		f, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		enc := json.NewEncoder(f)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
		f.Close()
	}

	rt := FinishTaskArgs{OutFiles: set}
	rp := MasterReply{}
	call("Master.FinishTask", &rt, &rp)
}

func reduceTask(reducef func(string, []string) string, reply *TaskReply) {
	reduceNum := reply.TaskNum
}

// Ask master for map task
func RequestTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	args := RequestTaskArgs{Free: true}
	reply := TaskReply{}

	call("Master.AssignTask", &args, &reply)
	fmt.Printf("%+v\n", reply)

	if reply.TaskType == "map" {
		mapTask(mapf, &reply)
	} else if reply.TaskType == "reduce" {
		reduceTask(reducef, &reply)
	}
}

func Finish() {

}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
