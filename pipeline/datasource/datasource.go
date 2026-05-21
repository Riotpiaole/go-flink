package datasource

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

const ChunkSize = 100 * 1024 * 1024 // 100 MB

// FileChunk carries the original file name and up to 100 MB of its content.
type FileChunk struct {
	FileName string
	Content  []byte
}

type DataSource interface {
	Stream(ctx context.Context) <-chan string
	StreamChunks(ctx context.Context) <-chan FileChunk
}

var _ DataSource = (*FilesDataSource)(nil)

type FilesDataSource struct {
	FilePath string
}

// Stream implements DataSource.
func (fd *FilesDataSource) Stream(ctx context.Context) <-chan string {
	jobs := make(chan string)
	go func() {
		defer close(jobs)
		err := filepath.WalkDir(fd.FilePath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			select {
			case jobs <- path:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
		if err != nil {
			fmt.Printf("failed to walk directory: %s\n", err)
		}
	}()
	return jobs
}

// StreamChunks implements DataSource. It walks the directory and emits each file
// in up to 100 MB chunks. Each FileChunk carries the full file path and its content.
func (fd *FilesDataSource) StreamChunks(ctx context.Context) <-chan FileChunk {
	ch := make(chan FileChunk)
	go func() {
		defer close(ch)
		err := filepath.WalkDir(fd.FilePath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
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
					select {
					case ch <- FileChunk{FileName: path, Content: chunk}:
					case <-ctx.Done():
						return ctx.Err()
					}
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
	return ch
}
