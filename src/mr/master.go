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

const (
	MaxTaskRuntime = 5 * time.Second
)

type Master struct {
	// Your definitions here.
	mu      sync.Mutex
	nReduce int // should be 10 for this lab

	mapNum    []int // map task index, 0: unassigned, 1: assigned, 2: finished
	reduceNum []int // redue task index

	mapTime    []time.Time
	reduceTime []time.Time

	files          []string // all files for processing
	mapFinished    bool
	reduceFinished bool
	// intermediates map[int][]string
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AssignTask(args *RequestTaskArgs, reply *TaskReply) error {
	m.mu.Lock()
	// fmt.Printf("%v\n", m.files)
	reply.NReduce = m.nReduce
	reply.NMap = len(m.files)
	if !m.mapFinished {
		reply.TaskType = "wait"
		for idx, status := range m.mapNum {
			if status == 0 || (status == 1 && time.Since(m.mapTime[idx]) > MaxTaskRuntime) {
				reply.TaskType = "map"
				reply.TaskNum = idx
				reply.File = m.files[idx]
				m.mapNum[idx] = 1 // set to assigned
				m.mapTime[idx] = time.Now()
				break
			}
		}
	} else if !m.reduceFinished {
		reply.TaskType = "wait"
		for idx, status := range m.reduceNum {
			if status == 1 && time.Since(m.reduceTime[idx]) > MaxTaskRuntime {
				fmt.Println("Reduce Task timeout!")
			}
			if status == 0 || (status == 1 && time.Since(m.reduceTime[idx]) > MaxTaskRuntime) {
				reply.TaskType = "reduce"
				reply.TaskNum = idx
				m.reduceNum[idx] = 1 // set to assigned
				m.reduceTime[idx] = time.Now()
				break
			}
		}
	} else {
		reply.TaskType = "finished"
	}
	m.mu.Unlock()

	return nil
}

func (m *Master) FinishTask(returnVal *FinishTaskArgs, reply *MasterReply) error {
	m.mu.Lock()
	index := returnVal.TaskNum
	if returnVal.TaskType == "map" {
		m.mapNum[index] = 2
		m.mapFinished = true
		for _, status := range m.mapNum {
			if status != 2 {
				m.mapFinished = false
				break
			}
		}
	} else { // reduce
		m.reduceNum[index] = 2
		fmt.Println(index)
		m.reduceFinished = true
		for _, status := range m.reduceNum {
			if status != 2 {
				m.reduceFinished = false
				break
			}
		}
	}
	m.mu.Unlock()
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mapFinished && m.reduceFinished
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:        nReduce,
		files:          files,
		mapFinished:    false,
		reduceFinished: false,
	}

	m.mapNum = make([]int, len(files))
	m.reduceNum = make([]int, nReduce)
	m.mapTime = make([]time.Time, len(files))
	m.reduceTime = make([]time.Time, nReduce)

	m.server()
	return &m
}
