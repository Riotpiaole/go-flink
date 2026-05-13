package pipeline

import (
	"fmt"
	"log"
	"net/rpc"
)

type TaskStatus int

const (
	MAPPED TaskStatus = iota
	READY
	RUNNIGN
)

type Worker struct {
	ID int
}

// CallForStatusReport implements TaskProcessor.
func (w *Worker) CallForStatusReport() error {
	panic("unimplemented")
}

// CallForTask implements TaskProcessor.
func (w *Worker) CallForTask() *MessageReply {
	panic("unimplemented")
}

var _ TaskProcessor = (*Worker)(nil)

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
