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

	pq "github.com/emirpasic/gods/queues/priorityqueue"
	"github.com/emirpasic/gods/utils"
)

type TaskStatus int

const (
	MAPPING TaskStatus = iota
	READY
	MAPED
	REDUCING
	REDUCED
	GROUPING
	GROUPPED
	SINKING
	SINKED
)

// 1. Define the Priority Rank Map
// Lower number = higher priority (comes out first)
var StatusPriority = map[TaskStatus]int{
	READY:    1,
	MAPPING:  2,
	MAPED:    3,
	REDUCING: 4,
	REDUCED:  5,
	GROUPING: 6,
	GROUPPED: 7,
	SINKING:  8,
	SINKED:   9,
}

type TaskInfo struct {
	FilePath string
	Status   TaskStatus
	TaskId   int
}

type Coordinator struct {
	Phase  TaskStatus
	mu     sync.Mutex
	readMu sync.RWMutex

	NumTasks   int
	NumReduced int

	mapIdx    int
	reduceIdx int

	JobStatus *pq.Queue
}

// AskForTask implements TaskScheduler.
func (c *Coordinator) AskForTask(req *MessageSend, reply *MessageReply) error {
	if req.MsgType != AskForTask {
		log.Println("Bad Message recevied \n")
		return fmt.Errorf("Bad Message type")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Printf("A worker is ask for task\n")

	task, empty := c.NextJob()
	if empty && c.Done() {
		reply.MsgType = Shutdown
		return nil
	}

	if !empty {
		reply.MsgType = MapTaskAlloc
		reply.TaskID = task.TaskId
		reply.TaskName = task.FilePath
	}

	// we reached the task became empty and we need to

	return nil
}

// NoticeResult implements TaskScheduler.
func (c *Coordinator) NoticeResult(req *MessageSend, reply *MessageReply) error {
	return nil
}

func byPriority(a, b interface{}) int {
	priorityA := StatusPriority[a.(*TaskInfo).Status]
	priorityB := StatusPriority[b.(*TaskInfo).Status]
	return -utils.IntComparator(priorityA, priorityB) // "-" descending order
}

var _ TaskScheduler = (*Coordinator)(nil)

func NewCoordinator() *Coordinator {
	jobStatus := pq.NewWith(byPriority)
	return &Coordinator{
		Phase:     MAPPING,
		JobStatus: jobStatus,

		NumTasks:   0,
		NumReduced: 0,
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
					return
				}
				fmt.Printf("Received Msg, %v\n", msg)
				if c.mu.TryLock() {
					c.JobStatus.Enqueue(
						&TaskInfo{
							FilePath: msg,
							Status:   READY,
							TaskId:   c.mapIdx,
						})
					c.mapIdx++
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

func (c *Coordinator) NextJob() (*TaskInfo, bool) {
	c.readMu.RLock()
	defer c.readMu.RUnlock()

	incompletedTask, hasElement := c.JobStatus.Dequeue()

	if !hasElement {
		// our task is completed
		return nil, true
	}
	fmt.Printf("Check for queue %+v \n", incompletedTask)
	return incompletedTask.(*TaskInfo), false
}

func (c *Coordinator) Done() bool {
	return c.mapIdx == c.NumTasks && c.NumReduced == c.reduceIdx
}
