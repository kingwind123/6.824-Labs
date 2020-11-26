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

type Master struct {
	// Your definitions here.
	mu        sync.Mutex
	nReduce   int
	mapNum    int
	reduceNum int
	files     []string
	finished  bool
	// intermediates map[int][]string
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AssignTask(args *RequestTaskArgs, reply *TaskReply) error {
	m.mu.Lock()
	fmt.Printf("%v\n", m.files)
	reply.NReduce = m.nReduce
	if len(m.files) > 0 {
		reply.TaskType = "map"
		reply.TaskNum = m.mapNum
		m.mapNum++

		reply.File = m.files[0]
		m.files = m.files[1:]
	} else if m.reduceNum < m.nReduce {
		reply.TaskType = "reduce"
		reply.TaskNum = m.reduceNum
		reply.NMap = m.mapNum

		// reply.File = m.intermediates[0]
		// m.files = m.intermediates[1:]
		m.reduceNum++
	} else {
		reply.TaskType = "Finished"
		m.finished = true
	}
	m.mu.Unlock()

	return nil
}

func (m *Master) FinishTask(returnVal *FinishTaskArgs, reply *MasterReply) error {
	if returnVal.TaskType == "map" {

	} else {

	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.

	return m.finished
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:   nReduce,
		files:     files,
		mapNum:    0,
		reduceNum: 0,
		// intermediates: make(map[int]map[string]void),
		finished: false,
	}

	// Your code here.

	m.server()
	return &m
}
