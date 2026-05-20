// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/pbnjay/memory"
	"github.com/spf13/cobra"
	"riotpiaole.com/vec_db_pipeline/pipeline"
	"riotpiaole.com/vec_db_pipeline/pipeline/datasource"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   " [inputs...] processor.so",
		Short: "Run an ETL process with given files and a shared object plugin",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			// The last argument is the .so file

			// Everything before the last argument is part of the input
			inputs := args[0]

			fmt.Printf("🚀 Starting ETL Process\n")
			fmt.Printf("📂 Inputs:    %v\n", inputs)
			// fmt.Printf("⚙️  Processor: %s\n", processor)

			// Logic to handle DB URL vs File Dir vs File List would go here
			RunPipeline(inputs)
		},
	}

	var workerCmd = &cobra.Command{
		Use:   "worker",
		Short: "Run a worker",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			// _ := args[0]
			pipeline.StartWorker()
		},
	}
	rootCmd.AddCommand(workerCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// resolveNumWorkers determines how many goroutine workers to launch.
//
// Resolution order:
//  1. NUM_WORKER in .env / process environment — user-supplied hard limit.
//  2. Memory-bound fallback — derived from free RAM divided by the per-goroutine
//     stack budget (4 KB).  If the result is still zero (extremely constrained
//     host), NumCPU is used as a last-resort floor.
func resolveNumWorkers() int {
	// Each goroutine starts with a 4 KB stack; use that as the budget unit.
	const bytesPerWorker = 4 * 1024

	// L1: load .env into the process environment (no-op when file is absent).
	_ = godotenv.Load()

	// L1: user-specified value takes priority over any automatic calculation.
	if raw := os.Getenv("NUM_WORKER"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err == nil && n > 0 {
			fmt.Printf("NUM_WORKER from env: %d\n", n)
			return n
		}
		// Value is present but malformed — warn and fall through to the resource-based path.
		fmt.Fprintf(os.Stderr, "invalid NUM_WORKER=%q, falling back to memory-bound calculation\n", raw)
	}

	// Fallback: compute from available RAM so we never over-commit goroutines.
	free := memory.FreeMemory()
	NUM_WORKER := int(free / bytesPerWorker)

	// Guard against NUM_WORKER == 0 on a heavily loaded or very small host.
	if NUM_WORKER < 1 {
		NUM_WORKER = runtime.NumCPU()
	}

	fmt.Printf("NUM_WORKER (memory-bound): %d  (free RAM: %d MB)\n", NUM_WORKER, free/1024/1024)
	return NUM_WORKER
}

func RunPipeline(filePath string) {
	numWorkers := resolveNumWorkers()
	datasource := datasource.FilesDataSource{
		FilePath: filePath,
	}
	ppl := pipeline.NewPipeline(&datasource, numWorkers, func(s string) string { return "" })
	ppl.Start()
}
