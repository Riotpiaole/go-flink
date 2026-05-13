package pipeline

import (
	"fmt"
	"sync"
	"time"
)

type TaskPhase int

const (
	MAPPING TaskPhase = iota
	PREPROCESSING
	REDUCING
	GROUPING
	SINKING
	WAITING
)

type Coordinator struct {
	Phase     TaskPhase
	JobStatus map[int]string
	NumTasks  int
	idx       int
	mu        sync.Mutex
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		Phase:     MAPPING,
		JobStatus: make(map[int]string),
	}
}

func (c *Coordinator) OnMessage(msgCh <-chan string) {
	fmt.Printf("Started Coordinator\n")
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
					c.JobStatus[c.NumTasks] = msg
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

func (c *Coordinator) Done() {
}
