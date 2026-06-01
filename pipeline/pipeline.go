package pipeline

import (
	"context"
	"fmt"
	"net/rpc"
	"os"
	"time"

	"github.com/google/uuid"
	"riotpiaole.com/vec_db_pipeline/pipeline/datasource"
)

const DefaultOutputDir = "mr-out"

type Pipeline struct {
	Sources      datasource.DataSource
	SourceConfig SourceConfig
	Actions      []StreamProcessAction
	NReduce      int
	OutputDir    string
	RPCAddr      string // optional TCP address for remote job submission (e.g. ":8000")
}

func NewPipeline(source datasource.DataSource) *Pipeline {
	return &Pipeline{
		Sources:   source,
		Actions:   []StreamProcessAction{},
		NReduce:   4,
		OutputDir: DefaultOutputDir,
	}
}

// NewPipelineFromConfig constructs a Pipeline whose data source is described
// by a SourceConfig. Use this when submitting to a remote cluster so the
// coordinator can reconstruct the source on the leader node.
func NewPipelineFromConfig(cfg SourceConfig) (*Pipeline, error) {
	src, err := datasource.NewFromConfig(cfg.Type, cfg.Config)
	if err != nil {
		return nil, err
	}
	return &Pipeline{
		Sources:      src,
		SourceConfig: cfg,
		Actions:      []StreamProcessAction{},
		NReduce:      4,
		OutputDir:    DefaultOutputDir,
	}, nil
}

// Map appends a map stage using the named plugin.
func (p *Pipeline) Map(pluginName string) *Pipeline {
	p.Actions = append(p.Actions, StreamProcessAction{
		Name:       pluginName,
		ActionType: MapTask,
	})
	return p
}

// Reduce appends a reduce stage using the named plugin.
func (p *Pipeline) Reduce(pluginName string) *Pipeline {
	p.Actions = append(p.Actions, StreamProcessAction{
		Name:       pluginName,
		ActionType: ReduceTask,
	})
	return p
}

// Filter appends a filter stage using the named plugin.
func (p *Pipeline) Filter(pluginName string) *Pipeline {
	p.Actions = append(p.Actions, StreamProcessAction{
		Name:       pluginName,
		ActionType: FilterTask,
	})
	return p
}

// GroupBy appends a compaction stage that folds all staged reduce outputs for
// each bucket into a single mr-out-<bucket> file. Must immediately follow a Reduce stage.
func (p *Pipeline) GroupBy(pluginName string) *Pipeline {
	if len(p.Actions) == 0 || p.Actions[len(p.Actions)-1].ActionType != ReduceTask {
		panic("GroupBy must immediately follow a Reduce stage")
	}
	p.Actions = append(p.Actions, StreamProcessAction{
		Name:       pluginName,
		ActionType: GroupByTask,
	})
	return p
}

// Sink appends a sink stage. The built-in sink writes to MongoDB;
// pluginName is recorded but the sink logic is handled by the worker directly.
func (p *Pipeline) Sink(pluginName string) *Pipeline {
	p.Actions = append(p.Actions, StreamProcessAction{
		Name:       pluginName,
		ActionType: SinkTask,
	})
	return p
}

// stages converts the pipeline's Actions into JobSpec StageSpecs for remote submission.
func (p *Pipeline) stages() []StageSpec {
	out := make([]StageSpec, len(p.Actions))
	for i, a := range p.Actions {
		out[i] = StageSpec{Type: a.ActionType, PluginName: a.Name}
	}
	return out
}

// Submit serialises this pipeline as a JobSpec and sends it to a running cluster
// at clusterAddr (host:port). It blocks until the job completes.
func (p *Pipeline) Submit(clusterAddr string) error {
	if len(p.Actions) == 0 {
		return fmt.Errorf("no processing stages defined for the pipeline")
	}

	spec := &JobSpec{
		JobID:     uuid.NewString(),
		Source:    p.SourceConfig,
		Stages:    p.stages(),
		OutputDir: p.OutputDir,
		NReduce:   p.NReduce,
	}

	client, err := rpc.DialHTTP("tcp", clusterAddr)
	if err != nil {
		return fmt.Errorf("connect to cluster %s: %w", clusterAddr, err)
	}
	defer client.Close()

	reply := &JobReply{}
	if err := client.Call("Coordinator.SubmitJob", spec, reply); err != nil {
		return fmt.Errorf("SubmitJob RPC: %w", err)
	}
	if reply.Error != "" {
		return fmt.Errorf("job rejected: %s", reply.Error)
	}
	fmt.Printf("[submit] job %s accepted, polling for completion…\n", reply.JobID)

	for {
		time.Sleep(time.Second)
		doneReply := &JobReply{}
		if err := client.Call("Coordinator.IsDone", &JobSpec{JobID: reply.JobID}, doneReply); err != nil {
			return fmt.Errorf("IsDone RPC: %w", err)
		}
		if doneReply.Status == "done" {
			fmt.Printf("[submit] job %s complete\n", reply.JobID)
			return nil
		}
	}
}

func (p *Pipeline) Start() {
	if len(p.Actions) == 0 {
		panic("no processing stages defined for the pipeline")
	}
	if err := os.MkdirAll(p.OutputDir, 0755); err != nil {
		panic("failed to create output dir: " + err.Error())
	}

	ctx := context.Background()
	msgCh := p.Sources.StreamChunks(ctx)
	coordinator := NewCoordinator(p.NReduce, p.Actions)

	if p.RPCAddr != "" {
		if err := coordinator.ListenTCP(p.RPCAddr); err != nil {
			panic("coordinator TCP listen: " + err.Error())
		}
	}

	go coordinator.Start(msgCh)
	fmt.Printf("[pipeline] coordinator started (nReduce=%d, stages=%d)\n",
		p.NReduce, len(p.Actions))

	for !coordinator.Done() {
		time.Sleep(time.Second)
	}
}
