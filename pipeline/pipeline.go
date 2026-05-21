package pipeline

import (
	"context"
	"fmt"
	"os"
	"time"

	"riotpiaole.com/vec_db_pipeline/pipeline/datasource"
)

const DefaultOutputDir = "mr-out"

type InpuType int

const DEFAULT_WINDOW_SIZE = 1 * 1024 * 1024 // 1 MB
const (
	FILE_DIR InpuType = iota
	FILE_LIST
	DB_URL
	EVETN_STREAM
)

var _ StreamListener = (*Pipeline)(nil)

type Pipeline struct {
	Sources   datasource.DataSource
	Actions   []StreamProcessAction
	NReduce   int
	OutputDir string
}

// Listen implements StreamListener.
func (p *Pipeline) Listen(source <-chan string) {
	panic("unimplemented")
}

// ListenRawBytes implements StreamListener.
func (p *Pipeline) ListenRawBytes(source <-chan []byte) {
	panic("unimplemented")
}

// NewPipeline creates a new instance
func NewPipeline(source datasource.DataSource) *Pipeline {
	return &Pipeline{
		Sources:   source,
		Actions:   []StreamProcessAction{},
		OutputDir: DefaultOutputDir,
	}
}

// Map appends a map stage and returns the pipeline for chaining.
func (p *Pipeline) Map(fn func(args ...any) any) *Pipeline {
	p.Actions = append(p.Actions, StreamProcessAction{
		Action:     fn,
		ActionType: MapTask,
	})
	return p
}

// Reduce appends a reduce stage and returns the pipeline for chaining.
func (p *Pipeline) Reduce(fn func(args ...any) any) *Pipeline {
	p.Actions = append(p.Actions, StreamProcessAction{
		Action:     fn,
		ActionType: ReduceTask,
	})
	return p
}

// Sink appends a sink stage and returns the pipeline for chaining.
func (p *Pipeline) Sink(fn func(args ...any) any) *Pipeline {
	p.Actions = append(p.Actions, StreamProcessAction{
		Action:     fn,
		ActionType: SinkTask,
	})
	return p
}

func (p *Pipeline) Start() {
	if len(p.Actions) == 0 {
		panic("No processing allocate for the pipeline.")
	}

	if err := os.MkdirAll(p.OutputDir, 0755); err != nil {
		panic("failed to create output dir: " + err.Error())
	}

	ctx := context.Background()

	msgCh := p.Sources.StreamChunks(ctx)
	coordinator := NewCoordinator(p.NReduce, p.Actions)

	go coordinator.Start(msgCh)
	fmt.Printf("[pipeline] coordinator started (nReduce=%d, phases=%d)\n", p.NReduce, len(p.Actions))

	for !coordinator.Done() {
		time.Sleep(time.Second)
	}
}
