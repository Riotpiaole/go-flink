package pipeline

import (
	"os"
	"strconv"
	"sync"
)

type MsgType int
type Key string

const (
	AskForTask   MsgType = iota
	TaskAlloc            // generic task dispatch for any phase
	TaskSuccess          // generic completion report
	TaskFailed           // generic failure report
	TaskContinue         // worker finished one chunk; more file data remains
	Shutdown
	Wait
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
	CallForStatusReport(status MsgType, taskId int, taskName string, phaseIdx int) bool
}

// MessageSend is sent from worker → coordinator.
type MessageSend struct {
	MsgType      MsgType
	TaskID       int
	TaskName     string
	PhaseIdx     int   // which phase this report belongs to
	NextOffset   int64 // for TaskContinue: byte offset of the next unprocessed chunk
	DispatchedAt int64 // unix-nano of when coordinator dispatched this task; guards against stale sweeper re-enqueue
}

// MessageReply is sent from coordinator → worker.
type MessageReply struct {
	MsgType     MsgType
	NReduce     int
	TaskID      int
	TaskName    string // file path for phase 0 (map)
	FileName    string // base file name of the source chunk
	ChunkID     string // UUID identifying this specific file chunk
	JobID       string // UUID of the pipeline job; scopes output directories and MongoDB keys
	BucketID    int    // partition index for reduce/groupby stages
	ActionIndex int    // kept for backward compatibility; prefer StageIdx
	PhaseIdx     int    // coordinator's current phaseIdx at dispatch time
	ChunkOffset  int64  // byte offset where this map task should begin reading
	DispatchedAt int64  // unix-nano timestamp of this dispatch; echoed back in MessageSend to detect stale reports

	// Phase-UUID fields — used for job-isolated file paths and MongoDB keys.
	// Format: <outputDir>/<jobID>/<actionDir>/mr-<PhaseUUID>-...
	PhaseUUID      string   // UUID of the current phase; unique per phase across all jobs
	InputPhaseUUID string   // UUID of the phase whose outputs are this task's inputs
	InputActionType TaskType // ActionType of the input phase; drives inDir() subfolder resolution

	// Stage-aware execution graph fields
	PluginName    string   // plugin filename stem to load (e.g. "wc")
	StageIdx      int      // which stage in the pipeline this task belongs to
	InputStageIdx int      // stage whose output files are this task's inputs (legacy; prefer InputPhaseUUID)
	ActionType    TaskType // Map | Filter | Reduce | GroupBy | Sink
}

// ChunkRequest is sent from worker → coordinator to fetch raw chunk content.
type ChunkRequest struct {
	ChunkID string
}

// ChunkReply carries the raw bytes of a chunk back to the requesting worker.
type ChunkReply struct {
	Content []byte
}

// KeyValue is the fundamental unit exchanged between map and reduce workers.
type KeyValue struct {
	Key   Key
	Value any
}

// IntermediateStore is shared in-process between the coordinator and workers.
// Map workers write to buckets; reduce workers read from buckets and write to results.
type IntermediateStore struct {
	mu      sync.Mutex
	buckets [][]KeyValue
	results []KeyValue
}

func NewIntermediateStore(nReduce int) *IntermediateStore {
	return &IntermediateStore{
		buckets: make([][]KeyValue, nReduce),
		results: []KeyValue{},
	}
}

// coordinatorSock returns a unique-ish UNIX-domain socket path in /var/tmp.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
