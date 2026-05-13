package pipeline

import (
	"os"
	"strconv"
)

type MicroBatchMsg struct {
	BatchID int
	Data    string
}

type TaskScheduler interface {
	AskForTask(*MessageSend, *MessageReply)
	NoticeResult(*MessageSend, *MessageReply)
}

type TaskProcessor interface {
	CallForTask() *MessageReply
	CallForStatusReport() error
}

type MessageSend struct {
}

type MessageReply struct {
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
