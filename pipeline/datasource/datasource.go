package datasource

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

type DataSource interface {
	Stream(ctx context.Context) <-chan string
	StreamBytes(ctx context.Context) <-chan []byte
}

var _ DataSource = (*FilesDataSource)(nil)

type FilesDataSource struct {
	FilePath string
}

// Stream implements DataSource.
func (fd *FilesDataSource) Stream(ctx context.Context) <-chan string {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	fmt.Println("Start streaming")

	jobs := make(chan string)

	// monitor for shutdown signal
	go func() {
		sig := <-sigChan
		fmt.Printf("\nReceived signal: %v. Shutting down...\n", sig)
		close(jobs)
	}()

	go func() {
		fmt.Println("A coordinator is listening")
		defer close(jobs)
		err := filepath.WalkDir(fd.FilePath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err // Stop and return error if a directory can't be accessed
			}

			// Check if it's a file (not a directory)
			if !d.IsDir() {
				fmt.Printf("Send msg %v\n", path)
				jobs <- path
			}

			return nil
		})
		if err != nil {
			fmt.Printf("failed to go through directory %s\n", err)
		}

	}()

	return jobs
}

// StreamBytes implements DataSource.
func (f *FilesDataSource) StreamBytes(ctx context.Context) <-chan []byte {
	panic("unimplemented")
}
