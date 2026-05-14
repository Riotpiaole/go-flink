package pipeline

import (
	"os"
	"strconv"
)

type MsgType int

const (
	AskForTask      MsgType = iota // `Worker`请求任务
	MapTaskAlloc                   // `Coordinator`分配`Map`任务
	ReduceTaskAlloc                // `Coordinator`分配`Reduce`任务
	MapSuccess                     // `Worker`报告`Map`任务的执行成功
	MapFailed                      // `Worker`报告`Map`任务的执行失败
	ReduceSuccess                  // `Worker`报告`Reduce`任务的执行成功
	ReduceFailed                   //`Worker`报告`Reduce`任务的执行失败
	Shutdown                       // `Coordinator`告知`Worker`退出（所有任务执行成功）
	Wait                           //`Coordinator`告知`Worker`休眠（暂时没有任务需要执行）
)

type MicroBatchMsg struct {
	BatchID int
	Data    string
}

type TaskScheduler interface {
	AskForTask(*MessageSend, *MessageReply) error
	NoticeResult(*MessageSend, *MessageReply) error
}

type TaskProcessor interface {
	CallForTask() *MessageReply
	CallForStatusReport(MsgType, int) bool
}

type MessageSend struct {
	MsgType MsgType
	TaskID  int
}

type MessageReply struct {
	MsgType  MsgType
	NReduce  int
	TaskID   int
	TaskName string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"

	s += strconv.Itoa(os.Getuid())
	return s
}
