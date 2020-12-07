package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

type RequestTaskArgs struct {
	Free bool
}

// type void struct{}

type FinishTaskArgs struct {
	TaskType string // "map", "reduce"
	TaskNum  int    // task index
}

type MasterReply struct {
}

type TaskReply struct {
	TaskType string // "map", "reduce", "finished", "wait"
	TaskNum  int    // task index

	NReduce int
	NMap    int

	File string // file for map task
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
