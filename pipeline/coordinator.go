package pipeline

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

type TaskPhase int

const (
	MAPPING TaskPhase = iota
	PREPROCESSING
	MAPPING TaskPhase = iota
	PREPROCESSING
	REDUCING
	GROUPING
	SINKING
	WAITING
	COMPLETED
)

type taskInfo struct {
	FilePath string
	Status   TaskStatus
	TaskId   int
}

type Coordinator struct {
	Phase  TaskPhase
	mu     sync.Mutex
	readMu sync.RWMutex

	NumTasks int
	idx      int

	JobStatus map[int]taskInfo
}

// AskForTask implements TaskScheduler.
func (c *Coordinator) AskForTask(*MessageSend, *MessageReply) {
	panic("unimplemented")
}

// NoticeResult implements TaskScheduler.
func (c *Coordinator) NoticeResult(*MessageSend, *MessageReply) {
	panic("unimplemented")
}

var _ TaskScheduler = (*Coordinator)(nil)

func NewCoordinator() *Coordinator {
	return &Coordinator{
		Phase:     MAPPING,
		JobStatus: make(map[int]taskInfo),

		NumTasks: 0,
		idx:      0,
	}
}

func (c *Coordinator) StartsServer() {
	// start a thread that listens for RPCs from worker.g
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

}

func (c *Coordinator) ListenFromDataSource(msgCh <-chan string) {
	fmt.Printf("Started Coordinator Listening from msgChan\n")
	// message collector it collects the msg and proceed to send the worker
	go func() {
		for {

			select {
			case msg, ok := <-msgCh:
				if !ok {
					fmt.Println("Channel is closed ")
					c.Phase = WAITING
					return
				}
				fmt.Printf("Received Msg, %v\n", c.NumTasks)
				if c.mu.TryLock() {
					c.JobStatus[c.idx] = taskInfo{
						FilePath: msg,
						TaskId:   c.idx,
						Status:   READY,
					}
					c.NumTasks++

					c.mu.Unlock()
				}
				time.Sleep(30 * time.Millisecond)
			case <-time.After(1 * time.Second):
				fmt.Println("Timed out: No message arrived after 1 second.")
			default:
			}
			time.Sleep(30 * time.Millisecond)
		}
	}()
}

func (c *Coordinator) Done() bool {
	return c.Phase == COMPLETED
}
