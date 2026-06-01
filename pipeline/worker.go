package pipeline

import (
	"encoding/json"
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/serialx/hashring"
)

type Worker struct {
	ID              int
	registry        *PluginRegistry
	outputDir       string
	coordinatorAddr string // empty = use Unix socket (embedded mode); host:port = TCP cluster mode
	activeReply     *MessageReply
	lastErr         error
}

// dial returns an RPC client connected to the coordinator.
func (w *Worker) dial() (*rpc.Client, error) {
	if w.coordinatorAddr != "" {
		return rpc.DialHTTP("tcp", w.coordinatorAddr)
	}
	return rpc.DialHTTP("unix", coordinatorSock())
}

func (w *Worker) workerCall(rpcname string, args interface{}, reply interface{}) bool {
	c, err := w.dial()
	if err != nil {
		fmt.Printf("[worker %d] dial error: %v\n", w.ID, err)
		return false
	}
	defer c.Close()
	if err := c.Call(rpcname, args, reply); err != nil {
		fmt.Printf("[worker %d] RPC %s error: %v\n", w.ID, rpcname, err)
		return false
	}
	return true
}

func (w *Worker) CallForTask() *MessageReply {
	args := MessageSend{MsgType: AskForTask}
	reply := MessageReply{}
	if w.workerCall("Coordinator.AskForTask", &args, &reply) {
		return &reply
	}
	return nil
}

func (w *Worker) CallForStatusReport(status MsgType, taskId int, taskName string, phaseIdx int) bool {
	args := MessageSend{
		MsgType:      status,
		TaskID:       taskId,
		TaskName:     taskName,
		PhaseIdx:     phaseIdx,
		DispatchedAt: w.activeReply.DispatchedAt, // echo token to guard against stale-report race
	}
	return w.workerCall("Coordinator.NoticeResult", &args, &MessageReply{})
}

// StartWorker runs a worker event loop against the local embedded coordinator (Unix socket).
func StartWorker(id int, registry *PluginRegistry, outputDir string) {
	w := &Worker{ID: id, registry: registry, outputDir: outputDir}
	runWorkerLoop(w)
}

// StartWorkerRemote runs a worker event loop against a remote coordinator at coordinatorAddr (host:port).
func StartWorkerRemote(id int, registry *PluginRegistry, outputDir, coordinatorAddr string) {
	w := &Worker{ID: id, registry: registry, outputDir: outputDir, coordinatorAddr: coordinatorAddr}
	runWorkerLoop(w)
}

func runWorkerLoop(w *Worker) {
	for {
		reply := w.CallForTask()
		if reply == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		switch reply.MsgType {
		case Wait:
			time.Sleep(200 * time.Millisecond)
		case Shutdown:
			fmt.Printf("[worker %d] shutting down\n", w.ID)
			return
		default:
			if w.invoke(reply) == nil {
				w.CallForStatusReport(TaskSuccess, reply.TaskID, reply.TaskName, reply.PhaseIdx)
			}
		}
	}
}

// invoke dispatches to the correct stage handler based on the task's ActionType.
func (w *Worker) invoke(reply *MessageReply) error {
	w.activeReply = reply
	w.lastErr = nil

	pf, err := w.registry.Get(reply.PluginName)
	if err != nil {
		w.lastErr = err
		w.CallForStatusReport(TaskFailed, reply.TaskID, reply.TaskName, reply.PhaseIdx)
		return err
	}
	switch reply.ActionType {
	case MapTask, FilterTask:
		w.runMap(pf)
	case ReduceTask:
		w.runReduce(pf)
	case SelectKeyTask:
		w.runSelectKey(pf)
	}
	return w.lastErr
}

// runMap handles Map and Filter stages. Distributes output KVs across NReduce buckets
// by key hash so that Reduce tasks can do cross-chunk aggregation.
func (w *Worker) runMap(pf *PluginFuncs) {
	if err := w.mapErr(pf); err != nil {
		w.lastErr = err
		reply := w.activeReply
		fmt.Printf("[worker %d] map/filter task %d failed: %v\n", w.ID, reply.TaskID, err)
		w.CallForStatusReport(TaskFailed, reply.TaskID, reply.TaskName, reply.PhaseIdx)
	}
}

func (w *Worker) mapErr(pf *PluginFuncs) error {
	reply := w.activeReply

	// Checkpoint: skip if output files for this stage+chunk already exist.
	checkpointGlob := filepath.Join(w.outputDir,
		fmt.Sprintf("mr-s%d-%s-*", reply.StageIdx, reply.ChunkID))
	if existing, _ := filepath.Glob(checkpointGlob); len(existing) > 0 {
		fmt.Printf("[worker %d] stage %d map %s: checkpoint found, skipping\n",
			w.ID, reply.StageIdx, reply.ChunkID)
		return nil
	}

	// Fetch input content.
	var filename string
	var content []byte
	var err error
	if reply.StageIdx == 0 {
		// Stage 0: raw chunk bytes from coordinator.
		content, err = w.getChunk(reply.ChunkID)
		if err != nil {
			return fmt.Errorf("fetch chunk %s: %w", reply.ChunkID, err)
		}
		filename = reply.FileName
	} else {
		// Stage N>0: read previous stage's intermediate files for this chunk.
		pattern := filepath.Join(w.outputDir,
			fmt.Sprintf("mr-s%d-%s-*", reply.InputStageIdx, reply.ChunkID))
		content, err = readFilesConcat(pattern)
		if err != nil {
			return fmt.Errorf("read stage %d intermediates for chunk %s: %w",
				reply.InputStageIdx, reply.ChunkID, err)
		}
		filename = reply.ChunkID
	}

	result := pf.Map(filename, string(content))
	kvs, ok := result.([]KeyValue)
	if !ok {
		return fmt.Errorf("map action must return []KeyValue, got %T", result)
	}

	// Distribute each KV to its bucket by key hash.
	ring := buildRing(reply.NReduce)
	bucketKVs := make(map[string][]KeyValue, reply.NReduce)
	for _, kv := range kvs {
		bucket, _ := ring.GetNode(string(kv.Key))
		bucketKVs[bucket] = append(bucketKVs[bucket], kv)
	}

	for bucket, bkvs := range bucketKVs {
		path := filepath.Join(w.outputDir,
			fmt.Sprintf("mr-s%d-%s-%s", reply.StageIdx, reply.ChunkID, bucket))
		out, err := os.Create(path)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(out)
		for _, kv := range bkvs {
			if encErr := enc.Encode(kv); encErr != nil {
				out.Close()
				return encErr
			}
		}
		out.Close()
	}

	fmt.Printf("[worker %d] stage %d map %s (%s) → %d kvs across %d buckets\n",
		w.ID, reply.StageIdx, reply.FileName, reply.ChunkID, len(kvs), len(bucketKVs))
	return nil
}

// runReduce handles Reduce, GroupBy, SelectKey. One task per bucket, reads across all chunks.
func (w *Worker) runReduce(pf *PluginFuncs) {
	if err := w.reduceErr(pf); err != nil {
		w.lastErr = err
		reply := w.activeReply
		fmt.Printf("[worker %d] reduce/groupby bucket %d failed: %v\n", w.ID, reply.BucketID, err)
		w.CallForStatusReport(TaskFailed, reply.TaskID, reply.TaskName, reply.PhaseIdx)
	}
}

func (w *Worker) reduceErr(pf *PluginFuncs) error {
	reply := w.activeReply
	outPath := filepath.Join(w.outputDir,
		fmt.Sprintf("mr-out-s%d-%d", reply.StageIdx, reply.BucketID))

	if _, err := os.Stat(outPath); err == nil {
		fmt.Printf("[worker %d] stage %d reduce bucket %d: checkpoint found, skipping\n",
			w.ID, reply.StageIdx, reply.BucketID)
		return nil
	}

	// Read ALL map outputs for this bucket, across every chunk.
	pattern := filepath.Join(w.outputDir,
		fmt.Sprintf("mr-s%d-*-%d", reply.InputStageIdx, reply.BucketID))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	var intermediate []KeyValue
	for _, fname := range files {
		f, err := os.Open(fname)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		f.Close()
	}

	sort.Slice(intermediate, func(i, j int) bool { return intermediate[i].Key < intermediate[j].Key })

	ofile, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer ofile.Close()

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, j-i)
		for k := i; k < j; k++ {
			values[k-i], _ = intermediate[k].Value.(string)
		}
		reduced := pf.Reduce(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, reduced)
		i = j
	}

	fmt.Printf("[worker %d] stage %d reduce bucket %d → %s\n",
		w.ID, reply.StageIdx, reply.BucketID, outPath)
	return nil
}

// runSink consolidates ALL reduce output files from the previous stage and upserts
// each key→value pair into MongoDB.
// Connection string: MONGO_URI env var (default: mongodb://localhost:27017).
// Database: MONGO_DB (default: "pipeline").
// Collection: MONGO_COLLECTION (default: "output").
// runSelectKey re-keys records from a reduce output. Reads the previous stage's
// reduce output for this bucket, calls plugin.Map(key, value) to assign new keys,
// and re-buckets into mr-s<StageIdx>-<inputBucket>-<newBucket> intermediates so a
// downstream Reduce can glob them with mr-s<N>-*-<newBucket>.
func (w *Worker) runSelectKey(pf *PluginFuncs) {
	if err := w.selectKeyErr(pf); err != nil {
		w.lastErr = err
		reply := w.activeReply
		fmt.Printf("[worker %d] selectkey bucket %d failed: %v\n", w.ID, reply.BucketID, err)
		w.CallForStatusReport(TaskFailed, reply.TaskID, reply.TaskName, reply.PhaseIdx)
	}
}

func (w *Worker) selectKeyErr(pf *PluginFuncs) error {
	reply := w.activeReply

	checkpointGlob := filepath.Join(w.outputDir,
		fmt.Sprintf("mr-s%d-%d-*", reply.StageIdx, reply.BucketID))
	if existing, _ := filepath.Glob(checkpointGlob); len(existing) > 0 {
		fmt.Printf("[worker %d] stage %d selectkey bucket %d: checkpoint found, skipping\n",
			w.ID, reply.StageIdx, reply.BucketID)
		return nil
	}

	inPath := filepath.Join(w.outputDir,
		fmt.Sprintf("mr-out-s%d-%d", reply.InputStageIdx, reply.BucketID))
	kvs, err := readReduceOutput(inPath)
	if err != nil {
		return fmt.Errorf("selectkey: read %s: %w", inPath, err)
	}

	ring := buildRing(reply.NReduce)
	bucketKVs := make(map[string][]KeyValue, reply.NReduce)
	for _, kv := range kvs {
		result := pf.Map(string(kv.Key), kv.Value.(string))
		newKVs, ok := result.([]KeyValue)
		if !ok {
			return fmt.Errorf("selectkey Map must return []KeyValue, got %T", result)
		}
		for _, nkv := range newKVs {
			bucket, _ := ring.GetNode(string(nkv.Key))
			bucketKVs[bucket] = append(bucketKVs[bucket], nkv)
		}
	}

	// Sort each bucket by key before writing so downstream Reduce gets pre-sorted input.
	for bucket, bkvs := range bucketKVs {
		sort.Slice(bkvs, func(i, j int) bool { return bkvs[i].Key < bkvs[j].Key })
		path := filepath.Join(w.outputDir,
			fmt.Sprintf("mr-s%d-%d-%s", reply.StageIdx, reply.BucketID, bucket))
		if err := encodeKVs(path, bkvs); err != nil {
			return err
		}
	}

	fmt.Printf("[worker %d] stage %d selectkey bucket %d → %d new-key buckets\n",
		w.ID, reply.StageIdx, reply.BucketID, len(bucketKVs))
	return nil
}

func (w *Worker) getChunk(chunkID string) ([]byte, error) {
	req := ChunkRequest{ChunkID: chunkID}
	reply := ChunkReply{}
	if !w.workerCall("Coordinator.GetChunk", &req, &reply) {
		return nil, fmt.Errorf("RPC GetChunk failed for chunk %s", chunkID)
	}
	return reply.Content, nil
}

// readFilesConcat globs pattern and returns the concatenated contents of all matched files.
func readFilesConcat(pattern string) ([]byte, error) {
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	var buf []byte
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		buf = append(buf, data...)
	}
	return buf, nil
}

func buildRing(nReduce int) *hashring.HashRing {
	nodes := make([]string, nReduce)
	for i := range nodes {
		nodes[i] = strconv.Itoa(i)
	}
	return hashring.New(nodes)
}

