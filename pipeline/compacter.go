package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Compacter handles GroupBy (compaction) and Sink tasks.
// It polls AskForCompactTask on the coordinator and runs independently of
// the regular Worker pool, allowing GroupBy to start as soon as individual
// reduce buckets complete rather than waiting for the full Reduce phase.
type Compacter struct {
	ID              int
	registry        *PluginRegistry // needed for GroupBy's pf.Reduce calls
	outputDir       string
	coordinatorAddr string
	activeReply     *MessageReply
	lastErr         error
}

// StartCompacter runs a Compacter against the local embedded coordinator (Unix socket).
func StartCompacter(id int, registry *PluginRegistry, outputDir string) {
	c := &Compacter{ID: id, registry: registry, outputDir: outputDir}
	runCompacterLoop(c)
}

// StartCompacterRemote runs a Compacter against a remote coordinator at addr (host:port).
func StartCompacterRemote(id int, registry *PluginRegistry, outputDir, coordinatorAddr string) {
	c := &Compacter{ID: id, registry: registry, outputDir: outputDir, coordinatorAddr: coordinatorAddr}
	runCompacterLoop(c)
}

func (c *Compacter) dial() (*rpc.Client, error) {
	if c.coordinatorAddr != "" {
		return rpc.DialHTTP("tcp", c.coordinatorAddr)
	}
	return rpc.DialHTTP("unix", coordinatorSock())
}

func (c *Compacter) call(rpcname string, args, reply interface{}) bool {
	cl, err := c.dial()
	if err != nil {
		fmt.Printf("[compacter %d] dial error: %v\n", c.ID, err)
		return false
	}
	defer cl.Close()
	if err := cl.Call(rpcname, args, reply); err != nil {
		fmt.Printf("[compacter %d] RPC %s error: %v\n", c.ID, rpcname, err)
		return false
	}
	return true
}

func (c *Compacter) askForTask() *MessageReply {
	args := MessageSend{MsgType: AskForTask}
	reply := MessageReply{}
	if c.call("Coordinator.AskForCompactTask", &args, &reply) {
		return &reply
	}
	return nil
}

func (c *Compacter) reportStatus(status MsgType, taskID int, taskName string, phaseIdx int) bool {
	args := MessageSend{
		MsgType:      status,
		TaskID:       taskID,
		TaskName:     taskName,
		PhaseIdx:     phaseIdx,
		DispatchedAt: c.activeReply.DispatchedAt,
	}
	return c.call("Coordinator.NoticeResult", &args, &MessageReply{})
}

func runCompacterLoop(c *Compacter) {
	for {
		reply := c.askForTask()
		if reply == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		switch reply.MsgType {
		case Wait:
			time.Sleep(200 * time.Millisecond)
		case Shutdown:
			fmt.Printf("[compacter %d] shutting down\n", c.ID)
			return
		default:
			c.activeReply = reply
			c.lastErr = nil
			var err error
			switch reply.ActionType {
			case GroupByTask:
				pf, loadErr := c.registry.Get(reply.PluginName)
				if loadErr != nil {
					c.lastErr = loadErr
					c.reportStatus(TaskFailed, reply.TaskID, reply.TaskName, reply.PhaseIdx)
					continue
				}
				err = c.groupByErr(pf)
			case SinkTask:
				err = c.sinkErr()
			}
			if err != nil {
				c.lastErr = err
				c.reportStatus(TaskFailed, reply.TaskID, reply.TaskName, reply.PhaseIdx)
			} else {
				c.reportStatus(TaskSuccess, reply.TaskID, reply.TaskName, reply.PhaseIdx)
			}
		}
	}
}

// groupByErr compacts ALL staged reduce outputs for a given bucket into mr-out-<bucket>.
// Uses an atomic write (temp file → rename) so the checkpoint is safe on retry.
func (c *Compacter) groupByErr(pf *PluginFuncs) error {
	reply := c.activeReply
	outPath := filepath.Join(c.outputDir, fmt.Sprintf("mr-out-%d", reply.BucketID))
	tmpPath := outPath + ".tmp"

	if _, err := os.Stat(outPath); err == nil {
		fmt.Printf("[compacter %d] groupby bucket %d: checkpoint found, skipping\n",
			c.ID, reply.BucketID)
		return nil
	}

	// Read ALL prior staged reduce outputs for this bucket (cross-stage compaction).
	pattern := filepath.Join(c.outputDir, fmt.Sprintf("mr-out-s*-%d", reply.BucketID))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("groupby bucket %d: no staged reduce outputs found", reply.BucketID)
	}

	var intermediate []KeyValue
	for _, fname := range files {
		f, err := os.Open(fname)
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), " ", 2)
			if len(parts) == 2 {
				intermediate = append(intermediate, KeyValue{Key: Key(parts[0]), Value: parts[1]})
			}
		}
		f.Close()
	}

	sort.Slice(intermediate, func(i, j int) bool { return intermediate[i].Key < intermediate[j].Key })

	tmp, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	written := make(map[Key]struct{})
	writeErr := func() error {
		for i := 0; i < len(intermediate); {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			key := intermediate[i].Key
			if _, exists := written[key]; exists {
				i = j
				continue
			}
			values := make([]string, j-i)
			for k := i; k < j; k++ {
				values[k-i], _ = intermediate[k].Value.(string)
			}
			reduced := pf.Reduce(key, values)
			if _, werr := fmt.Fprintf(tmp, "%v %v\n", key, reduced); werr != nil {
				return werr
			}
			written[key] = struct{}{}
			i = j
		}
		return nil
	}()

	tmp.Close()
	if writeErr != nil {
		os.Remove(tmpPath)
		return writeErr
	}

	if err := os.Rename(tmpPath, outPath); err != nil {
		os.Remove(tmpPath)
		return err
	}

	fmt.Printf("[compacter %d] groupby bucket %d → %s (%d unique keys)\n",
		c.ID, reply.BucketID, outPath, len(written))
	return nil
}

// sinkErr writes one bucket's compacted output to MongoDB.
// InputStageIdx < 0 means read mr-out-<bucket> (after GroupBy);
// InputStageIdx >= 0 means read mr-out-s<N>-<bucket> (after Reduce directly).
func (c *Compacter) sinkErr() error {
	reply := c.activeReply

	var inPath string
	if reply.InputStageIdx < 0 {
		inPath = filepath.Join(c.outputDir, fmt.Sprintf("mr-out-%d", reply.BucketID))
	} else {
		inPath = filepath.Join(c.outputDir,
			fmt.Sprintf("mr-out-s%d-%d", reply.InputStageIdx, reply.BucketID))
	}

	f, err := os.Open(inPath)
	if err != nil {
		return fmt.Errorf("sink bucket %d: open %s: %w", reply.BucketID, inPath, err)
	}
	defer f.Close()

	var kvs []KeyValue
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " ", 2)
		if len(parts) == 2 {
			kvs = append(kvs, KeyValue{Key: Key(parts[0]), Value: parts[1]})
		}
	}

	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://localhost:27017"
	}
	dbName := os.Getenv("MONGO_DB")
	if dbName == "" {
		dbName = "pipeline"
	}
	collName := os.Getenv("MONGO_COLLECTION")
	if collName == "" {
		collName = "output"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(mongoopts.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("sink bucket %d: connect mongo: %w", reply.BucketID, err)
	}
	defer client.Disconnect(ctx)

	coll := client.Database(dbName).Collection(collName)
	for _, kv := range kvs {
		filter := bson.D{{Key: "_id", Value: string(kv.Key)}}
		update := bson.D{{Key: "$set", Value: bson.D{
			{Key: "value", Value: kv.Value},
			{Key: "updatedAt", Value: time.Now()},
		}}}
		if _, err := coll.UpdateOne(ctx, filter, update,
			mongoopts.UpdateOne().SetUpsert(true)); err != nil {
			return fmt.Errorf("sink bucket %d: upsert key %q: %w", reply.BucketID, kv.Key, err)
		}
	}

	fmt.Printf("[compacter %d] sink bucket %d → upserted %d records to %s.%s\n",
		c.ID, reply.BucketID, len(kvs), dbName, collName)
	return nil
}

// readReduceOutput parses plain-text "key value" lines from a reduce output file.
// Exported for use in SelectKey (worker.go).
func readReduceOutput(path string) ([]KeyValue, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var kvs []KeyValue
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " ", 2)
		if len(parts) == 2 {
			kvs = append(kvs, KeyValue{Key: Key(parts[0]), Value: parts[1]})
		}
	}
	return kvs, scanner.Err()
}

// encodeKVs writes a []KeyValue slice as JSON lines to path.
func encodeKVs(path string, kvs []KeyValue) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, kv := range kvs {
		if err := enc.Encode(kv); err != nil {
			return err
		}
	}
	return nil
}
