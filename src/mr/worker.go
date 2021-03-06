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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		working := RequestTask(mapf, reducef)
		if !working {
			// fmt.Println("Stop worker")
			break
		}
		// time.Sleep(time.Second)
	}

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

	bucketMap := make(map[int][]KeyValue)
	for i := 0; i < reply.NReduce; i++ {
		bucketMap[i] = make([]KeyValue, 0)
	}

	for _, kv := range kva {
		reduceNum := ihash(kv.Key) % reply.NReduce
		bucketMap[reduceNum] = append(bucketMap[reduceNum], kv)
	}

	for i := 0; i < reply.NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", mapNum, i)

		// Create creates or truncates the named file.
		f, err := os.Create(oname)
		if err != nil {
			log.Fatalf("MapTask: cannot open %v", oname)
		}

		for _, kv := range bucketMap[i] {
			enc := json.NewEncoder(f)
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}

		f.Close()
	}

	rt := FinishTaskArgs{TaskType: "map", TaskNum: mapNum}
	rp := MasterReply{}
	call("Master.FinishTask", &rt, &rp)
}

func reduceTask(reducef func(string, []string) string, reply *TaskReply) {
	reduceNum := reply.TaskNum
	mapNum := reply.NMap
	intermediate := []KeyValue{}

	for i := 0; i < mapNum; i++ {
		oname := fmt.Sprintf("mr-%d-%d", i, reduceNum)
		if _, err := os.Stat(oname); err == nil {
			file, err := os.Open(oname)
			if err != nil {
				log.Fatalf("ReduceTask: cannot open %v", oname)
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
	}

	sort.Sort(ByKey(intermediate))

	tmpfile, err := ioutil.TempFile("./", "tmp*")
	if err != nil {
		log.Fatal(err)
	}

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
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tmpfile.Close()
	outname := fmt.Sprintf("mr-out-%d", reduceNum)
	err = os.Rename(tmpfile.Name(), outname)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < mapNum; i++ {
		oname := fmt.Sprintf("mr-%d-%d", i, reduceNum)
		err = os.Remove(oname)
		if err != nil {
			log.Fatal(err)
		}
	}

	rt := FinishTaskArgs{TaskType: "reduce", TaskNum: reduceNum}
	rp := MasterReply{}
	call("Master.FinishTask", &rt, &rp)

}

// Ask master for map task
func RequestTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	args := RequestTaskArgs{Free: true}
	reply := TaskReply{}

	res := call("Master.AssignTask", &args, &reply)
	if !res {
		return false
	}
	// fmt.Printf("%+v\n", reply)

	// _, err := os.Stat("./mr-out")

	// if os.IsNotExist(err) {
	// 	errDir := os.MkdirAll("./mr-out", 0755)
	// 	if errDir != nil {
	// 		log.Fatal(errDir)
	// 	}

	// }

	if reply.TaskType == "map" {
		mapTask(mapf, &reply)
	} else if reply.TaskType == "reduce" {
		reduceTask(reducef, &reply)
	} else if reply.TaskType == "wait" {
		time.Sleep(time.Second)
	} else if reply.TaskType == "finished" {
		return false
	}
	return true
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
