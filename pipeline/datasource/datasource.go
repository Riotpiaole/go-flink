package datasource

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

const ChunkSize = 10 * 1024 * 1024 // 10 MB

// FileChunk carries the original file name and up to 10 MB of its content.
type FileChunk struct {
	FileName string
	Content  []byte
}

// ChunkQueue is a thread-safe non-blocking FIFO for FileChunk items.
// Producers call Push then Close; consumers call Pop until Done returns true.
type ChunkQueue struct {
	mu     sync.Mutex
	items  []FileChunk
	closed bool
}

func NewChunkQueue() *ChunkQueue { return &ChunkQueue{} }

func (q *ChunkQueue) Push(c FileChunk) {
	q.mu.Lock()
	q.items = append(q.items, c)
	q.mu.Unlock()
}

// Pop returns the next item and true, or zero-value and false if the queue is empty.
func (q *ChunkQueue) Pop() (FileChunk, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return FileChunk{}, false
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item, true
}

// Close signals that no more items will be pushed.
func (q *ChunkQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
}

// Done returns true when the producer has closed the queue and all items have been consumed.
func (q *ChunkQueue) Done() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed && len(q.items) == 0
}

func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

type DataSource interface {
	StreamChunks(ctx context.Context) *ChunkQueue
}

// NewFromConfig constructs a DataSource from a generic config map.
// Supported types: "file" (requires "path" key), "s3" and "kafka" are stubs.
func NewFromConfig(sourceType string, cfg map[string]string) (DataSource, error) {
	switch sourceType {
	case "file":
		path, ok := cfg["path"]
		if !ok {
			return nil, fmt.Errorf("file source requires \"path\" key")
		}
		return &FilesDataSource{FilePath: path}, nil
	case "s3":
		return nil, fmt.Errorf("s3 source not yet implemented")
	case "kafka":
		return nil, fmt.Errorf("kafka source not yet implemented")
	default:
		return nil, fmt.Errorf("unknown source type %q", sourceType)
	}
}

var _ DataSource = (*FilesDataSource)(nil)

type FilesDataSource struct {
	FilePath string
}

// StreamChunks walks the directory and pushes each file in up to 10 MB chunks
// into a ChunkQueue. The queue is closed when all files have been enqueued.
func (fd *FilesDataSource) StreamChunks(ctx context.Context) *ChunkQueue {
	q := NewChunkQueue()
	go func() {
		defer q.Close()
		err := filepath.WalkDir(fd.FilePath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}

			f, err := os.Open(path)
			if err != nil {
				fmt.Printf("failed to open %s: %v\n", path, err)
				return nil
			}
			defer f.Close()

			buf := make([]byte, ChunkSize)
			for {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				n, readErr := io.ReadFull(f, buf)
				if n > 0 {
					chunk := make([]byte, n)
					copy(chunk, buf[:n])
					q.Push(FileChunk{FileName: path, Content: chunk})
				}
				if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
					break
				}
				if readErr != nil {
					fmt.Printf("error reading %s: %v\n", path, readErr)
					break
				}
			}
			return nil
		})
		if err != nil {
			fmt.Printf("failed to walk directory: %s\n", err)
		}
	}()
	return q
}
