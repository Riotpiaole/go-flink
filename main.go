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

func RunPipeline(filePath string) {
	datasource := datasource.FilesDataSource{
		FilePath: filePath,
	}
	ppl := pipeline.NewPipeline(&datasource, 10, func(s string) string { return "" })
	ppl.Start()

}
