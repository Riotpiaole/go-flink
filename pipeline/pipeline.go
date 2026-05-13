package pipeline

import (
	"context"

	"context"

	"riotpiaole.com/vec_db_pipeline/pipeline/datasource"
)

type InpuType int

const DEFAULT_WINDOW_SIZE = 1 * 1024 * 1024 // 1 MB
const (
	FILE_DIR InpuType = iota
	FILE_LIST
	DB_URL
	EVETN_STREAM
)

var _ StreamProcess = (*Pipeline)(nil)
var _ StreamListener = (*Pipeline)(nil)

// FileDataSource handles directory ingestion
type Pipeline struct {
	Sources       datasource.DataSource
	Sources       datasource.DataSource
	Clusters      []Coordinator
	WindowSize    int
	PartitionFunc func(string) string
}

// Listen implements StreamListener.
func (p *Pipeline) Listen(source <-chan string) {
	panic("unimplemented")
}

// ListenRawBytes implements StreamListener.
func (p *Pipeline) ListenRawBytes(source <-chan []byte) {
	panic("unimplemented")
}

// Filter implements StreamProcess.
func (p *Pipeline) Filter(validateFunc func(string) bool) StreamProcess {
	panic("unimplemented")
}

// GroupBy implements StreamProcess.
func (p *Pipeline) GroupBy(groupFunc func(string) string) StreamProcess {
	panic("unimplemented")
}

// Map implements StreamProcess.
func (p *Pipeline) Map(mapFunc func(string) string) StreamProcess {
	panic("unimplemented")
}

// Reduce implements StreamProcess.
func (p *Pipeline) Reduce(reduceFunc func(string, string) string) StreamProcess {
	panic("unimplemented")
}

// Sink implements StreamProcess.
func (p *Pipeline) Sink(sinkFunc func(string) error) error {
	panic("unimplemented")
}

// NewPipeline creates a new instance
func NewPipeline(source datasource.DataSource, windowSize int, partitionFunc func(string) string) *Pipeline {
func NewPipeline(source datasource.DataSource, windowSize int, partitionFunc func(string) string) *Pipeline {
	return &Pipeline{
		Sources:       source,
		Clusters:      []Coordinator{}, // This can be populated with actual cluster addresses
		WindowSize:    windowSize,
		PartitionFunc: partitionFunc,
	}
}

func (p *Pipeline) Start() {

	ctx := context.Background()

	msgCh := p.Sources.Stream(ctx)
	coordinator := NewCoordinator()

	coordinator.ListenFromDataSource(msgCh)
	coordinator.StartsServer()
}
