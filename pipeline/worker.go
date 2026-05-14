package pipeline

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"time"
)

type Worker struct {
	ID int
}

// CallForStatusReport implements TaskProcessor.
func (w *Worker) CallForStatusReport(status MsgType, taskId int) bool {
	args := MessageSend{
		MsgType: status,
		TaskID:  taskId,
	}

	return call("Coordinator.NoticeResult", &args, nil)
}

// CallForTask implements TaskProcessor.
func (w *Worker) CallForTask() *MessageReply {
	args := MessageSend{
		MsgType: AskForTask,
	}

	reply := MessageReply{}
	success := call("Coordinator.AskForTask", &args, &reply)
	if success {
		return &reply
	}
	return nil
}

var _ TaskProcessor = (*Worker)(nil)

func StartWorker() {
	Worker := &Worker{}
	var replyStatus MsgType
	for {
		reply := Worker.CallForTask()
		fmt.Printf("Call for task %v\n", reply)
		switch reply.MsgType {
		case MapTaskAlloc:
			err := HandleMapTask(reply)

			if err == nil {
				replyStatus = MapSuccess
			} else {
				replyStatus = MapFailed
			}
			Worker.CallForStatusReport(replyStatus, reply.TaskID)
		case ReduceTaskAlloc:
			err := HandleReduceTask(reply)

			if err == nil {
				replyStatus = ReduceSuccess
			} else {
				replyStatus = ReduceFailed
			}
			Worker.CallForStatusReport(replyStatus, reply.TaskID)
		case Wait:
			time.Sleep(time.Second * 10)
		case Shutdown:
			os.Exit(0)
		}

		time.Sleep(1 * time.Second)
	}

}

func HandleMapTask(replyMsg *MessageReply) error {
	return nil
}

func HandleReduceTask(replyMsg *MessageReply) error {
	return nil
}

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
