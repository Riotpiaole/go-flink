package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/hashicorp/raft"
	pq "github.com/emirpasic/gods/queues/priorityqueue"
)

// RaftCommandType identifies the mutation being replicated.
type RaftCommandType string

const (
	CmdEnqueueTask  RaftCommandType = "EnqueueTask"
	CmdDispatchTask RaftCommandType = "DispatchTask"
	CmdCompleteTask RaftCommandType = "CompleteTask"
	CmdFailTask     RaftCommandType = "FailTask"
	CmdAdvancePhase RaftCommandType = "AdvancePhase"
)

// RaftCommand is JSON-encoded and stored in the Raft log.
type RaftCommand struct {
	Type         RaftCommandType
	TaskID       int
	ChunkID      string
	PhaseIdx     int
	NextOffset   int64
	DispatchedAt time.Time // set for CmdDispatchTask; recorded in inFlight for timeout detection
	Task         *TaskInfo // full task for EnqueueTask and CmdDispatchTask
}

// raftSnapshot captures the coordinator state for log compaction.
// chunkStore is intentionally excluded — raw bytes are too large and
// workers re-fetch chunks via GetChunk RPC.
type raftSnapshot struct {
	JobID         string
	PhaseIdx      int
	PhaseDone     int
	FailedTasks   int
	NumTasks      int
	SourceDone    bool
	InFlight      map[int]*TaskInfo
	TaskFiles     map[int]string
	TaskFileNames map[int]string
	Tasks         []*TaskInfo // queue contents at snapshot time
}

// Apply implements raft.FSM. Called on every node when a log entry commits.
func (c *Coordinator) Apply(l *raft.Log) interface{} {
	var cmd RaftCommand
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("apply: unmarshal: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	switch cmd.Type {
	case CmdEnqueueTask:
		if cmd.Task != nil {
			if _, exists := c.chunkStore[cmd.Task.ChunkID]; !exists {
				c.chunkStore[cmd.Task.ChunkID] = nil // placeholder for follower nodes; leader already stored content
			}
			c.JobStatus.Enqueue(cmd.Task)
			c.taskFiles[cmd.Task.TaskId] = cmd.Task.ChunkID
			c.taskFileNames[cmd.Task.TaskId] = cmd.Task.FileName
			c.NumTasks++
		}

	case CmdDispatchTask:
		if cmd.Task != nil {
			cmd.Task.DispatchedAt = cmd.DispatchedAt
			c.inFlight[cmd.Task.TaskId] = cmd.Task
			// On WAL replay a task may still be in the PQ (enqueued before crash,
			// dequeued by the old leader but not yet snapshotted). Remove it so
			// the restored queue doesn't re-dispatch it.
			c.removeFromQueue(cmd.Task.TaskId)
		}

	case CmdCompleteTask:
		delete(c.inFlight, cmd.TaskID)
		if cmd.ChunkID != "" {
			delete(c.chunkStore, cmd.ChunkID)
			if path := c.chunkPath(cmd.ChunkID); path != "" {
				go os.Remove(path) // async: avoid blocking Apply while holding mu
			}
		}
		c.phaseDone++

	case CmdFailTask:
		if task, ok := c.inFlight[cmd.TaskID]; ok {
			delete(c.inFlight, cmd.TaskID)
			task.Retries++
			if task.Retries >= maxRetries {
				c.failedTasks++
				c.phaseDone++
			} else {
				task.Status = UnAssigned
				task.DispatchedAt = time.Time{}
				c.JobStatus.Enqueue(task)
			}
		}

	case CmdAdvancePhase:
		c.phaseIdx++
		c.phaseDone = 0
		c.failedTasks = 0
		c.inFlight = make(map[int]*TaskInfo)
		// reduceDoneBuckets and compactDispatched are now persisted in MongoDB
		// (CompactedBucketStore). Documents from the old phase are automatically
		// invisible because keys are scoped by instanceID + phaseIdx.
	}

	return nil
}

// Snapshot implements raft.FSM. Produces a point-in-time snapshot of coordinator state.
func (c *Coordinator) Snapshot() (raft.FSMSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Drain the queue into a slice so we can serialize it.
	var tasks []*TaskInfo
	tmp := pq.NewWith(byPriority)
	for {
		item, ok := c.JobStatus.Dequeue()
		if !ok {
			break
		}
		t := item.(*TaskInfo)
		tasks = append(tasks, t)
		tmp.Enqueue(t)
	}
	// Restore the queue.
	for {
		item, ok := tmp.Dequeue()
		if !ok {
			break
		}
		c.JobStatus.Enqueue(item)
	}

	snap := &raftSnapshot{
		JobID:         c.CompactedBucketStore.JobID(),
		PhaseIdx:      c.phaseIdx,
		PhaseDone:     c.phaseDone,
		FailedTasks:   c.failedTasks,
		NumTasks:      c.NumTasks,
		SourceDone:    c.sourceDone,
		InFlight:      c.inFlight,
		TaskFiles:     c.taskFiles,
		TaskFileNames: c.taskFileNames,
		Tasks:         tasks,
	}
	return &fsmSnapshot{data: snap}, nil
}

// Restore implements raft.FSM. Rebuilds coordinator state from a snapshot.
func (c *Coordinator) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snap raftSnapshot
	if err := json.NewDecoder(rc).Decode(&snap); err != nil {
		return fmt.Errorf("restore: decode: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.phaseIdx = snap.PhaseIdx
	c.phaseDone = snap.PhaseDone
	c.failedTasks = snap.FailedTasks
	c.NumTasks = snap.NumTasks
	c.sourceDone = snap.SourceDone
	c.inFlight = snap.InFlight
	c.taskFiles = snap.TaskFiles
	c.taskFileNames = snap.TaskFileNames
	if snap.JobID != "" {
		c.CompactedBucketStore.SetJobID(snap.JobID)
	}

	// Rebuild the priority queue from the snapshot task list.
	c.JobStatus = pq.NewWith(byPriority)
	for _, t := range snap.Tasks {
		c.JobStatus.Enqueue(t)
	}

	return nil
}

// fsmSnapshot implements raft.FSMSnapshot.
type fsmSnapshot struct {
	data *raftSnapshot
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(s.data); err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(buf.Bytes()); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
