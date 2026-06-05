package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	pq "github.com/emirpasic/gods/queues/priorityqueue"
	"github.com/emirpasic/gods/utils"
	"github.com/google/uuid"
	"riotpiaole.com/vec_db_pipeline/pipeline/datasource"
)

const (
	maxRetries     = 3
	taskTimeout    = 30 * time.Second // re-enqueue in-flight tasks that exceed this
	sweepInterval  = 5 * time.Second  // how often the timeout sweeper runs
)

type TaskInfo struct {
	FilePath     string
	FileName     string    // base name of the source file
	ChunkID      string    // UUID identifying this specific 100 MB chunk
	Status       TaskStatus
	TaskId       int
	PhaseIdx     int       // which phase this task belongs to
	StageIdx     int       // same as PhaseIdx; carried into MessageReply for workers
	ActionType   TaskType  // Map/Filter/Reduce/GroupBy/SelectKey/Sink — makes tasks self-describing
	PluginName   string    // plugin to invoke for this task
	Retries      int       // how many times this task has been retried
	DispatchedAt time.Time // when it was last handed to a worker (zero = not dispatched)
	ChunkOffset  int64     // byte offset where the next map chunk begins (0 = start of file)
}

type Coordinator struct {
	mu sync.Mutex

	ProcessAction []StreamProcessAction

	// phaseIdx is the cursor into ProcessAction.
	// Phase 0 = map (tasks from data source).
	// Phase 1+ = subsequent stages (NReduce partitioned tasks each).
	// phaseIdx == len(ProcessAction) means all phases complete.
	phaseIdx int

	NReduce  int
	NumTasks int // total map tasks ever enqueued (grows as source streams)

	// phaseDone counts completed tasks in the current phase.
	phaseDone int

	// failedTasks counts tasks that exhausted their retries this phase.
	failedTasks int

	// sourceDone is set when the data source channel closes.
	sourceDone bool

	JobStatus *pq.Queue

	// inFlight tracks dispatched tasks that haven't reported back yet.
	// Key = TaskId. Used to detect crashed workers via the timeout sweeper.
	inFlight map[int]*TaskInfo

	// taskFiles maps TaskId → ChunkID so reduce tasks reference the right map output files.
	taskFiles map[int]string

	// taskFileNames maps TaskId → original FileName for logging and worker display.
	taskFileNames map[int]string

	// chunkStore holds raw chunk content keyed by ChunkID (UUID).
	chunkStore map[string][]byte

	// CompactedBucketStore persists reduceDone / compactDispatched state to MongoDB
	// so GroupBy dispatch survives leader failover. Connect() is called from Start
	// and StartWithRaft. A nil client inside the store disables reactive dispatch.
	CompactedBucketStore *CompactedBucketStore

	// chunkDir is an optional on-disk fallback for chunk bytes (set by StartWithRaft).
	// Allows a restarted leader to serve GetChunk without re-streaming the source.
	// Note: does NOT help when a different pod becomes leader, since each pod has its
	// own local disk. A shared PVC mount on the coordinator StatefulSet would be needed
	// to cover that case.
	chunkDir string

	Intermediates *IntermediateStore

	// raft is nil in single-node embedded mode; set by InitRaft for distributed operation.
	raftNode *raft.Raft

	// Fields used by the unified node model (StartWithRaft).
	myRPCAddr       string                        // this node's TCP RPC address (e.g. ":8000")
	peerRPCAddrs    map[raft.ServerAddress]string // raftAddr → rpcAddr for follower→leader routing
	workerRegistry  *PluginRegistry
	workerOutputDir string
}

func byPriority(a, b interface{}) int {
	priorityA := StatusPriority[a.(*TaskInfo).Status]
	priorityB := StatusPriority[b.(*TaskInfo).Status]
	return -utils.IntComparator(priorityA, priorityB)
}

var _ TaskScheduler = (*Coordinator)(nil)
var _ raft.FSM = (*Coordinator)(nil)

func NewCoordinator(nReduce int, actions []StreamProcessAction) *Coordinator {
	return &Coordinator{
		ProcessAction: actions,
		phaseIdx:      0,
		NReduce:       nReduce,
		JobStatus:     pq.NewWith(byPriority),
		inFlight:      make(map[int]*TaskInfo),
		taskFiles:     make(map[int]string),
		taskFileNames: make(map[int]string),
		chunkStore:           make(map[string][]byte),
		Intermediates:        NewIntermediateStore(nReduce),
		CompactedBucketStore: NewCompactedBucketStore(""),
	}
}

func (c *Coordinator) chunkPath(chunkID string) string {
	if c.chunkDir == "" {
		return ""
	}
	return filepath.Join(c.chunkDir, chunkID+".bin")
}

// proposeCmd proposes a state mutation through Raft when distributed, or applies
// it directly to the FSM in single-node embedded mode.
// MUST be called without holding c.mu — FSM.Apply acquires it internally.
func (c *Coordinator) proposeCmd(cmd RaftCommand) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal raft command: %w", err)
	}
	if c.raftNode == nil {
		// Embedded single-node mode: apply directly without network round-trip.
		if res := c.Apply(&raft.Log{Data: data}); res != nil {
			return res.(error)
		}
		return nil
	}
	// Distributed mode: replicate via Raft consensus (blocks until committed).
	f := c.raftNode.Apply(data, 5*time.Second)
	return f.Error()
}

// InitRaft bootstraps a Raft cluster for this coordinator node.
// nodeID is a unique string (e.g. "node-0"), raftBind is the TCP listen address
// (e.g. ":7000"), raftAdvertise is the address peers use to reach this node
// (e.g. "go-flink-0.go-flink:7000"; empty = same as raftBind), peers is the
// full list of advertised addresses for all cluster members, and dataDir is the
// directory for Raft WAL and snapshots.
func (c *Coordinator) InitRaft(nodeID, raftBind, raftAdvertise string, peers []string, dataDir string) error {
	cfg := raft.DefaultConfig()
	// Use raftAdvertise as the Raft server ID so it matches the peer list entries.
	// Falls back to nodeID when running single-node without an advertise address.
	localID := raftAdvertise
	if localID == "" {
		localID = nodeID
	}
	cfg.LocalID = raft.ServerID(localID)

	var advertise net.Addr
	if raftAdvertise != "" {
		// Pod DNS entries may not propagate immediately — retry for up to 60 s.
		var tcpAddr *net.TCPAddr
		var err error
		for i := 0; i < 30; i++ {
			tcpAddr, err = net.ResolveTCPAddr("tcp", raftAdvertise)
			if err == nil {
				break
			}
			log.Printf("[raft] waiting for DNS %s (%d/30): %v", raftAdvertise, i+1, err)
			time.Sleep(2 * time.Second)
		}
		if err != nil {
			return fmt.Errorf("raft advertise addr %s: %w", raftAdvertise, err)
		}
		advertise = tcpAddr
	}

	transport, err := raft.NewTCPTransport(raftBind, advertise, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("raft transport: %w", err)
	}

	snapStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("raft snapshot store: %w", err)
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("raft boltdb store: %w", err)
	}

	r, err := raft.NewRaft(cfg, c, boltStore, boltStore, snapStore, transport)
	if err != nil {
		return fmt.Errorf("raft.NewRaft: %w", err)
	}

	// Bootstrap only when this is the first time — check by trying to get config.
	future := r.GetConfiguration()
	if err := future.Error(); err != nil || len(future.Configuration().Servers) == 0 {
		servers := make([]raft.Server, len(peers))
		for i, p := range peers {
			servers[i] = raft.Server{
				ID:      raft.ServerID(p),
				Address: raft.ServerAddress(p),
			}
		}
		bf := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := bf.Error(); err != nil && err != raft.ErrCantBootstrap {
			return fmt.Errorf("raft bootstrap: %w", err)
		}
	}

	c.raftNode = r
	return nil
}

// AskForTask implements TaskScheduler.
func (c *Coordinator) AskForTask(req *MessageSend, reply *MessageReply) error {
	if req.MsgType != AskForTask {
		return fmt.Errorf("bad message type")
	}

	// Followers don't hold the full job state — tell external workers to wait
	// and retry; they will eventually land on the leader via the load-balancer.
	if c.raftNode != nil && c.raftNode.State() != raft.Leader {
		reply.MsgType = Wait
		return nil
	}

	c.mu.Lock()
	if c.done() {
		c.mu.Unlock()
		reply.MsgType = Shutdown
		return nil
	}

	task, empty := c.nextJob()
	if empty {
		complete := c.currentPhaseComplete()
		c.mu.Unlock()
		if complete {
			// transitionToNextPhase proposes via Raft — must not hold c.mu.
			c.transitionToNextPhase()
			c.mu.Lock()
			done := c.done()
			c.mu.Unlock()
			if done {
				reply.MsgType = Shutdown
				return nil
			}
		}
		reply.MsgType = Wait
		return nil
	}

	// Capture phase state before releasing the lock.
	now := time.Now()
	task.PhaseIdx = c.phaseIdx
	action := c.ProcessAction[c.phaseIdx]
	phaseIdx := c.phaseIdx
	nReduce := c.NReduce
	c.mu.Unlock()

	// Replicate dispatch through Raft so inFlight survives leader failover.
	// proposeCmd must not be called with c.mu held.
	if err := c.proposeCmd(RaftCommand{
		Type:         CmdDispatchTask,
		Task:         task,
		DispatchedAt: now,
	}); err != nil {
		// Re-enqueue the dequeued task so it isn't lost.
		task.Status = UnAssigned
		task.DispatchedAt = time.Time{}
		c.mu.Lock()
		c.JobStatus.Enqueue(task)
		c.mu.Unlock()
		reply.MsgType = Wait
		return nil
	}

	reply.MsgType  = TaskAlloc
	reply.TaskID   = task.TaskId
	reply.TaskName = task.FilePath
	reply.FileName = task.FileName
	reply.ChunkID  = task.ChunkID
	reply.NReduce  = nReduce
	// BucketID is only meaningful for bucket-parallel phases (Reduce/GroupBy/SelectKey).
	// For those phases transitionToNextPhase sets TaskId == bucket index, so reuse it.
	switch action.ActionType {
	case ReduceTask, GroupByTask, SelectKeyTask:
		reply.BucketID = task.TaskId
	}
	reply.ActionIndex   = phaseIdx
	reply.PhaseIdx      = phaseIdx
	reply.ChunkOffset   = task.ChunkOffset
	reply.PluginName    = action.Name
	reply.StageIdx      = phaseIdx
	reply.InputStageIdx = phaseIdx - 1
	reply.ActionType    = action.ActionType
	reply.DispatchedAt  = now.UnixNano()
	return nil
}

// AskForCompactTask is the RPC endpoint for Compacter nodes. It serves GroupBy
// tasks reactively (as reduce buckets complete) and Sink tasks from the regular
// queue once the GroupBy phase advances to Sink.
func (c *Coordinator) AskForCompactTask(req *MessageSend, reply *MessageReply) error {
	if c.raftNode != nil && c.raftNode.State() != raft.Leader {
		reply.MsgType = Wait
		return nil
	}

	c.mu.Lock()
	if c.done() {
		c.mu.Unlock()
		reply.MsgType = Shutdown
		return nil
	}
	if c.phaseIdx >= len(c.ProcessAction) {
		c.mu.Unlock()
		reply.MsgType = Wait
		return nil
	}

	switch c.ProcessAction[c.phaseIdx].ActionType {
	case GroupByTask:
		// Snapshot shared state, then release the lock before hitting MongoDB.
		phaseIdx := c.phaseIdx
		nReduce := c.NReduce
		action := c.ProcessAction[c.phaseIdx]
		reducePhaseIdx := c.phaseIdx - 1
		c.mu.Unlock()

		for bucket := 0; bucket < nReduce; bucket++ {
			if !c.CompactedBucketStore.IsReduceDone(phaseIdx, bucket) {
				continue
			}
			// ClaimCompactDispatch uses MongoDB insert uniqueness as an atomic claim.
			// Returns false if another compacter already claimed this bucket.
			if !c.CompactedBucketStore.ClaimCompactDispatch(phaseIdx, bucket) {
				continue
			}
			now := time.Now()
			task := &TaskInfo{
				TaskId:     bucket,
				ActionType: GroupByTask,
				StageIdx:   phaseIdx,
				PhaseIdx:   phaseIdx,
				PluginName: action.Name,
			}
			// Replicate dispatch through Raft so inFlight survives leader failover.
			if err := c.proposeCmd(RaftCommand{
				Type:         CmdDispatchTask,
				Task:         task,
				DispatchedAt: now,
			}); err != nil {
				reply.MsgType = Wait
				return nil
			}
			reply.MsgType       = TaskAlloc
			reply.TaskID        = bucket
			reply.BucketID      = bucket
			reply.ActionType    = GroupByTask
			reply.StageIdx      = phaseIdx
			reply.InputStageIdx = reducePhaseIdx
			reply.NReduce       = nReduce
			reply.PluginName    = action.Name
			reply.PhaseIdx      = phaseIdx
			reply.DispatchedAt  = now.UnixNano()
			return nil
		}
		reply.MsgType = Wait
		return nil

	case SinkTask:
		// Sink tasks are pre-enqueued (one per bucket); drain them like AskForTask.
		task, empty := c.nextJob()
		if empty {
			c.mu.Unlock()
			reply.MsgType = Wait
			return nil
		}
		now := time.Now()
		task.PhaseIdx = c.phaseIdx
		action := c.ProcessAction[c.phaseIdx]
		phaseIdx := c.phaseIdx
		nReduce := c.NReduce
		// InputStageIdx = -1 signals Compacter to read mr-out-<bucket> (GroupBy output).
		// Otherwise it reads mr-out-s<N>-<bucket> (Reduce output).
		inputStageIdx := phaseIdx - 1
		if inputStageIdx >= 0 && c.ProcessAction[inputStageIdx].ActionType == GroupByTask {
			inputStageIdx = -1
		}
		c.mu.Unlock()

		// Replicate dispatch through Raft so inFlight survives leader failover.
		if err := c.proposeCmd(RaftCommand{
			Type:         CmdDispatchTask,
			Task:         task,
			DispatchedAt: now,
		}); err != nil {
			task.Status = UnAssigned
			task.DispatchedAt = time.Time{}
			c.mu.Lock()
			c.JobStatus.Enqueue(task)
			c.mu.Unlock()
			reply.MsgType = Wait
			return nil
		}
		reply.MsgType       = TaskAlloc
		reply.TaskID        = task.TaskId
		reply.BucketID      = task.TaskId
		reply.ActionType    = SinkTask
		reply.StageIdx      = phaseIdx
		reply.InputStageIdx = inputStageIdx
		reply.NReduce       = nReduce
		reply.PluginName    = action.Name
		reply.PhaseIdx      = phaseIdx
		reply.DispatchedAt  = now.UnixNano()
		return nil

	default:
		c.mu.Unlock()
		reply.MsgType = Wait
		return nil
	}
}

// NoticeResult implements TaskScheduler.
func (c *Coordinator) NoticeResult(req *MessageSend, reply *MessageReply) error {
	// Read shared state under lock, then release before proposing to Raft.
	c.mu.Lock()
	if req.PhaseIdx != c.phaseIdx {
		c.mu.Unlock()
		fmt.Printf("[coordinator] stale report for phase %d (current %d), ignoring\n",
			req.PhaseIdx, c.phaseIdx)
		return nil
	}
	task, inflight := c.inFlight[req.TaskID]
	var chunkID string
	if inflight {
		// Reject reports from a worker that was superseded by a sweeper re-dispatch.
		// The worker echoes back the DispatchedAt token we gave it; if it doesn't
		// match the current record, this is a late report from the old worker.
		if req.DispatchedAt != 0 && task.DispatchedAt.UnixNano() != req.DispatchedAt {
			c.mu.Unlock()
			fmt.Printf("[coordinator] stale report for task %d (dispatch token mismatch), ignoring\n", req.TaskID)
			return nil
		}
		chunkID = task.ChunkID
		// Evict from inFlight immediately under the same lock.
		// This closes the race where the sweeper could observe the task as
		// still dispatched and re-enqueue it between now and when FSM.Apply
		// runs inside proposeCmd.  FSM.Apply's delete is a safe no-op.
		delete(c.inFlight, req.TaskID)
	}
	c.mu.Unlock()

	// TaskContinue re-enqueues locally (offset advance, no phase-level counter change).
	if req.MsgType == TaskContinue {
		if !inflight {
			return nil
		}
		c.mu.Lock()
		task.ChunkOffset = req.NextOffset
		task.Status = UnAssigned
		task.DispatchedAt = time.Time{}
		c.JobStatus.Enqueue(task)
		c.mu.Unlock()
		fmt.Printf("[coordinator] phase %d: task %d continuing at offset %d\n",
			req.PhaseIdx, req.TaskID, req.NextOffset)
		return nil
	}

	// Capture ActionType before the lock is gone (task is already removed from inFlight).
	var taskActionType TaskType
	if inflight {
		taskActionType = task.ActionType
	}

	// TaskSuccess and TaskFailed mutate phaseDone/retries — propose through Raft.
	switch req.MsgType {
	case TaskSuccess:
		if err := c.proposeCmd(RaftCommand{
			Type:     CmdCompleteTask,
			TaskID:   req.TaskID,
			ChunkID:  chunkID,
			PhaseIdx: req.PhaseIdx,
		}); err != nil {
			return err
		}
		c.mu.Lock()
		fmt.Printf("[coordinator] phase %d: task %d succeeded (%d/%d done)\n",
			req.PhaseIdx, req.TaskID, c.phaseDone, c.phaseTotal())
		c.mu.Unlock()
		// Persist task completion to MongoDB outside the lock so I/O doesn't
		// stall other RPC handlers waiting on c.mu.
		switch taskActionType {
		case MapTask:
			if inflight {
				c.CompactedBucketStore.MarkMapTaskDone(req.PhaseIdx, req.TaskID, chunkID, task.FileName)
			}
		case ReduceTask:
			c.CompactedBucketStore.MarkReduceDone(req.PhaseIdx, req.TaskID)
		}

	case TaskFailed:
		if !inflight {
			return nil
		}
		if err := c.proposeCmd(RaftCommand{
			Type:     CmdFailTask,
			TaskID:   req.TaskID,
			PhaseIdx: req.PhaseIdx,
		}); err != nil {
			return err
		}
	}

	return nil
}

// SubmitJob accepts a JobSpec from a remote client, wires up the data source,
// and starts streaming chunks into the coordinator so workers can pick them up.
// In distributed mode only the Raft leader can accept new jobs.
func (c *Coordinator) SubmitJob(spec *JobSpec, reply *JobReply) error {
	if c.raftNode != nil && c.raftNode.State() != raft.Leader {
		leaderAddr, _ := c.raftNode.LeaderWithID()
		rpcAddr := c.peerRPCAddrs[leaderAddr]
		reply.Error = fmt.Sprintf("not leader; redirect to %s (rpc: %s)", leaderAddr, rpcAddr)
		return nil
	}

	if len(spec.Stages) == 0 {
		reply.Error = "job has no stages"
		return nil
	}

	// Convert StageSpecs to StreamProcessActions.
	actions := make([]StreamProcessAction, len(spec.Stages))
	for i, s := range spec.Stages {
		actions[i] = StreamProcessAction{Name: s.PluginName, ActionType: s.Type}
	}

	src, err := datasource.NewFromConfig(spec.Source.Type, spec.Source.Config)
	if err != nil {
		reply.Error = err.Error()
		return nil
	}

	c.mu.Lock()
	c.ProcessAction = actions
	if spec.NReduce > 0 {
		c.NReduce = spec.NReduce
	}
	c.mu.Unlock()
	c.CompactedBucketStore.SetJobID(spec.JobID)

	ctx := context.Background()
	q := src.StreamChunks(ctx)
	go c.listenFromDataSource(q)

	reply.JobID = spec.JobID
	reply.Status = "accepted"
	return nil
}

// IsDone lets a remote client poll for job completion.
func (c *Coordinator) IsDone(req *JobSpec, reply *JobReply) error {
	reply.JobID = req.JobID
	if c.Done() {
		reply.Status = "done"
	} else {
		reply.Status = "running"
	}
	return nil
}

// GetChunk implements TaskScheduler. Workers call this to retrieve raw chunk bytes by UUID.
func (c *Coordinator) GetChunk(req *ChunkRequest, reply *ChunkReply) error {
	c.mu.Lock()
	content, ok := c.chunkStore[req.ChunkID]
	c.mu.Unlock()

	if ok && content != nil {
		reply.Content = content
		return nil
	}

	// Memory miss (nil placeholder on follower, or leader restarted) — try disk.
	if path := c.chunkPath(req.ChunkID); path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			reply.Content = data
			c.mu.Lock()
			c.chunkStore[req.ChunkID] = data // warm the cache
			c.mu.Unlock()
			return nil
		}
	}

	return fmt.Errorf("chunk %s not found", req.ChunkID)
}

// sweepTimedOutTasks re-enqueues any in-flight task that has exceeded taskTimeout.
// Called periodically by the background sweeper goroutine — not holding mu on entry.
func (c *Coordinator) sweepTimedOutTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for id, task := range c.inFlight {
		if !task.DispatchedAt.IsZero() && now.Sub(task.DispatchedAt) > taskTimeout {
			delete(c.inFlight, id)
			task.Retries++
			if task.Retries >= maxRetries {
				c.failedTasks++
				c.phaseDone++
				if task.ChunkID != "" {
					delete(c.chunkStore, task.ChunkID)
					if path := c.chunkPath(task.ChunkID); path != "" {
						go os.Remove(path)
					}
				}
				fmt.Printf("[coordinator] task %d timed out and exhausted retries — giving up\n", id)
			} else {
				task.Status = UnAssigned
				task.DispatchedAt = time.Time{}
				c.JobStatus.Enqueue(task)
				fmt.Printf("[coordinator] task %d timed out (retry %d/%d), re-enqueued\n",
					id, task.Retries, maxRetries)
			}
		}
	}
}

// phaseTotal returns the expected number of task completions for the current phase.
func (c *Coordinator) phaseTotal() int {
	if c.phaseIdx >= len(c.ProcessAction) {
		return 0
	}
	switch c.ProcessAction[c.phaseIdx].ActionType {
	case ReduceTask, GroupByTask, SelectKeyTask, SinkTask:
		return c.NReduce
	default:
		return c.NumTasks
	}
}

// transitionToNextPhase advances phaseIdx and enqueues tasks for the new phase.
// MUST be called without holding c.mu — proposeCmd acquires it internally.
// Task count and shape depend on the incoming stage type:
//   - Map, Filter:            one task per input chunk (chunk-parallel)
//   - Reduce, GroupBy, SelectKey, Sink: one task per NReduce bucket (bucket-parallel)
func (c *Coordinator) transitionToNextPhase() {
	// Propose phase advancement (increments phaseIdx, resets counters on all nodes).
	if err := c.proposeCmd(RaftCommand{Type: CmdAdvancePhase}); err != nil {
		fmt.Printf("[coordinator] transitionToNextPhase: proposeCmd: %v\n", err)
		return
	}

	c.mu.Lock()
	if c.done() {
		c.mu.Unlock()
		fmt.Println("[coordinator] all phases complete")
		return
	}
	action := c.ProcessAction[c.phaseIdx]
	phaseIdx := c.phaseIdx
	numTasks := c.NumTasks
	nReduce := c.NReduce
	// Copy the maps we need before releasing the lock.
	taskFiles := make(map[int]string, numTasks)
	taskFileNames := make(map[int]string, numTasks)
	for k, v := range c.taskFiles { taskFiles[k] = v }
	for k, v := range c.taskFileNames { taskFileNames[k] = v }
	c.mu.Unlock()

	// For Reduce/SelectKey phases, try MongoDB for Map task outputs first.
	// MongoDB may have a more complete picture after a leader failover where
	// the Raft snapshot was taken mid-Map-phase. Falls back to taskFiles if
	// MongoDB returns nothing (nil client or local embedded mode).
	if action.ActionType == ReduceTask || action.ActionType == SelectKeyTask {
		if mongoFiles, mongoNames, err := c.CompactedBucketStore.MapTaskOutputs(phaseIdx - 1); err != nil {
			log.Printf("[coordinator] warning: MapTaskOutputs phase %d: %v — using in-memory taskFiles", phaseIdx-1, err)
		} else if len(mongoFiles) > 0 {
			taskFiles = mongoFiles
			taskFileNames = mongoNames
		}
	}

	fmt.Printf("[coordinator] transitioning to phase %d (%v plugin=%s)\n",
		phaseIdx, action.ActionType, action.Name)

	switch action.ActionType {
	case MapTask, FilterTask:
		if phaseIdx == 0 {
			return // phase 0 tasks arrive dynamically from listenFromDataSource
		}
		for i := 0; i < numTasks; i++ {
			chunkID := taskFiles[i]
			_ = c.proposeCmd(RaftCommand{Type: CmdEnqueueTask, Task: &TaskInfo{
				TaskId:     i,
				FilePath:   chunkID,
				FileName:   taskFileNames[i],
				ChunkID:    chunkID,
				Status:     UnAssigned,
				PhaseIdx:   phaseIdx,
				StageIdx:   phaseIdx,
				ActionType: action.ActionType,
				PluginName: action.Name,
			}})
		}

	case ReduceTask, SelectKeyTask:
		for bucket := 0; bucket < nReduce; bucket++ {
			_ = c.proposeCmd(RaftCommand{Type: CmdEnqueueTask, Task: &TaskInfo{
				TaskId:     bucket,
				FilePath:   fmt.Sprintf("bucket-%d", bucket),
				Status:     UnAssigned,
				PhaseIdx:   phaseIdx,
				StageIdx:   phaseIdx,
				ActionType: action.ActionType,
				PluginName: action.Name,
			}})
		}

	case GroupByTask:
		// GroupBy tasks are not pre-enqueued. AskForCompactTask dispatches them
		// reactively as reduce buckets complete (see reduceDoneBuckets).

	case SinkTask:
		// One task per bucket so Compacters can parallelise the sink phase.
		for bucket := 0; bucket < nReduce; bucket++ {
			_ = c.proposeCmd(RaftCommand{Type: CmdEnqueueTask, Task: &TaskInfo{
				TaskId:     bucket,
				FilePath:   fmt.Sprintf("sink-bucket-%d", bucket),
				Status:     UnAssigned,
				PhaseIdx:   phaseIdx,
				StageIdx:   phaseIdx,
				ActionType: SinkTask,
				PluginName: action.Name,
			}})
		}
	}
}

// currentPhaseComplete returns true when all tasks for the active phase have finished
// (either succeeded or exhausted retries).
func (c *Coordinator) currentPhaseComplete() bool {
	allReported := c.phaseDone == c.phaseTotal()
	noneInFlight := len(c.inFlight) == 0
	if c.phaseIdx >= len(c.ProcessAction) {
		return true
	}
	// The first Map phase depends on the source being exhausted first.
	// Subsequent chunk-parallel phases (Map/Filter at phaseIdx>0) do not.
	if c.ProcessAction[c.phaseIdx].ActionType == MapTask && c.phaseIdx == 0 {
		return c.sourceDone && allReported && noneInFlight
	}
	return allReported && noneInFlight
}

func (c *Coordinator) done() bool {
	// No job submitted yet — ProcessAction is empty.
	if len(c.ProcessAction) == 0 {
		return false
	}
	return c.phaseIdx >= len(c.ProcessAction)
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done()
}

func (c *Coordinator) nextJob() (*TaskInfo, bool) {
	item, ok := c.JobStatus.Dequeue()
	if !ok {
		return nil, true
	}
	return item.(*TaskInfo), false
}

// removeFromQueue drains and refills the priority queue, dropping the task with
// the given ID. Called from CmdDispatchTask.Apply during WAL replay so that a
// task dispatched before a crash (and therefore dequeued on the old leader) is
// not re-dispatched by the new leader after restore.
// MUST be called with c.mu held.
func (c *Coordinator) removeFromQueue(taskID int) {
	tmp := pq.NewWith(byPriority)
	for {
		item, ok := c.JobStatus.Dequeue()
		if !ok {
			break
		}
		t := item.(*TaskInfo)
		if t.TaskId != taskID {
			tmp.Enqueue(t)
		}
	}
	c.JobStatus = tmp
}

// StartWithRaft is the entry point for the unified node model.
// It registers RPCs, opens a TCP listener, then watches Raft leadership changes:
//   - On becoming leader: activates the coordinator role (task scheduler).
//   - On becoming follower: starts a worker loop polling the leader's RPC address.
//
// peerRPCAddrs maps each peer's Raft transport address to its worker RPC address
// (e.g. "node-0:7000" → "node-0:8000") so followers can find the current leader.
func (c *Coordinator) StartWithRaft(myRPCAddr string, peerRPCAddrs map[raft.ServerAddress]string, registry *PluginRegistry, outputDir string) {
	c.myRPCAddr = myRPCAddr
	c.peerRPCAddrs = peerRPCAddrs
	c.workerRegistry = registry
	c.workerOutputDir = outputDir

	if err := c.CompactedBucketStore.Connect(os.Getenv("MONGO_URI")); err != nil {
		log.Printf("[coordinator] warning: %v — GroupBy reactive dispatch disabled", err)
	}

	chunkDir := filepath.Join(outputDir, "chunks")
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		log.Printf("[coordinator] warning: could not create chunk dir %s: %v — disk fallback disabled", chunkDir, err)
	} else {
		c.chunkDir = chunkDir
	}

	rpc.Register(c)
	rpc.HandleHTTP()
	if err := c.ListenTCP(myRPCAddr); err != nil {
		log.Fatal(err)
	}
	go c.runSweeper()
	go c.watchLeadership()
}

// watchLeadership reacts to Raft leadership changes, activating coordinator or
// worker role on this node accordingly.
func (c *Coordinator) watchLeadership() {
	for isLeader := range c.raftNode.LeaderCh() {
		if isLeader {
			fmt.Printf("[node] became leader — coordinator role active (rpc=%s)\n", c.myRPCAddr)
			// The coordinator is always registered; it starts serving tasks as soon as
			// SubmitJob is called.  No extra activation needed beyond logging.
		} else {
			leaderRaftAddr := c.raftNode.Leader()
			leaderRPCAddr := c.peerRPCAddrs[leaderRaftAddr]
			if leaderRPCAddr == "" {
				fmt.Printf("[node] became follower (leader raft=%s, rpc unknown — waiting)\n", leaderRaftAddr)
				continue
			}
			fmt.Printf("[node] became follower — starting worker (leader rpc=%s)\n", leaderRPCAddr)
			go StartWorkerRemote(os.Getpid(), c.workerRegistry, c.workerOutputDir, leaderRPCAddr)
		}
	}
}

func (c *Coordinator) Start(q *datasource.ChunkQueue) {
	if err := c.CompactedBucketStore.Connect(os.Getenv("MONGO_URI")); err != nil {
		log.Printf("[coordinator] warning: %v — GroupBy reactive dispatch disabled", err)
	}
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go c.listenFromDataSource(q)
	go c.runSweeper()
	time.Sleep(30 * time.Millisecond)
}

// ListenTCP opens an additional TCP listener so remote clients (go-flink submit)
// can reach the coordinator's RPCs at addr (host:port).
func (c *Coordinator) ListenTCP(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("coordinator TCP listen on %s: %w", addr, err)
	}
	go http.Serve(l, nil)
	fmt.Printf("[coordinator] accepting remote RPC connections on %s\n", addr)
	return nil
}

// runSweeper periodically re-enqueues tasks whose workers have gone silent.
func (c *Coordinator) runSweeper() {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for range ticker.C {
		if c.Done() {
			return
		}
		c.sweepTimedOutTasks()
	}
}

func (c *Coordinator) listenFromDataSource(q *datasource.ChunkQueue) {
	fmt.Println("[coordinator] listening from data source")
	idx := 0

	c.mu.Lock()
	phase0Plugin := ""
	if len(c.ProcessAction) > 0 {
		phase0Plugin = c.ProcessAction[0].Name
	}
	c.mu.Unlock()

	for !q.Done() {
		chunk, ok := q.Pop()
		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		chunkID := uuid.New().String()

		// Store raw bytes locally (too large for Raft log; workers fetch via GetChunk RPC).
		c.mu.Lock()
		c.chunkStore[chunkID] = chunk.Content
		c.mu.Unlock()

		// Also write to disk so a restarted leader can serve GetChunk from the same pod.
		if path := c.chunkPath(chunkID); path != "" {
			if err := os.WriteFile(path, chunk.Content, 0o644); err != nil {
				fmt.Printf("[coordinator] warning: failed to persist chunk %s: %v\n", chunkID, err)
			}
		}

		// Propose the task enqueue through Raft (no lock held).
		_ = c.proposeCmd(RaftCommand{Type: CmdEnqueueTask, Task: &TaskInfo{
			FilePath:   chunkID,
			FileName:   chunk.FileName,
			ChunkID:    chunkID,
			Status:     UnAssigned,
			TaskId:     idx,
			PhaseIdx:   0,
			StageIdx:   0,
			ActionType: MapTask,
			PluginName: phase0Plugin,
		}})

		fmt.Printf("[coordinator] enqueued map task %d: file=%s chunk=%s (%d bytes)\n",
			idx, chunk.FileName, chunkID, len(chunk.Content))
		idx++
	}

	c.mu.Lock()
	c.sourceDone = true
	c.mu.Unlock()
	fmt.Println("[coordinator] data source exhausted")
}
