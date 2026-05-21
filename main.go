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
	"path/filepath"

	"github.com/spf13/cobra"
	"riotpiaole.com/vec_db_pipeline/pipeline"
	"riotpiaole.com/vec_db_pipeline/pipeline/datasource"
)

func defaultOutputDir() string {
	wd, err := os.Getwd()
	if err != nil {
		return pipeline.DefaultOutputDir
	}
	return filepath.Join(wd, pipeline.DefaultOutputDir)
}

func main() {
	var outputDir string

	var rootCmd = &cobra.Command{
		Use:   "<input> <plugin.so>",
		Short: "Run an ETL pipeline using a compiled .so plugin",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			input, soPath := args[0], args[1]
			fmt.Printf("🚀 Starting ETL Process\n")
			fmt.Printf("📂 Inputs: %v\n", input)
			fmt.Printf("🔌 Plugin: %v\n", soPath)
			fmt.Printf("📁 Output: %v\n", outputDir)
			RunPipeline(input, soPath, outputDir)
		},
	}

	var workerID int
	var pluginPath string

	var workerCmd = &cobra.Command{
		Use:   "worker",
		Short: "Start a worker process that loads map/reduce from a .so plugin",
		Run: func(cmd *cobra.Command, args []string) {
			pf, err := pipeline.LoadPlugin(pluginPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "load plugin: %v\n", err)
				os.Exit(1)
			}
			actions := []pipeline.StreamProcessAction{
				{Action: pf.Map, ActionType: pipeline.MapTask},
				{Action: pf.Reduce, ActionType: pipeline.ReduceTask},
			}
			fmt.Printf("[worker %d] loaded plugin %s\n", workerID, pluginPath)
			pipeline.StartWorker(workerID, actions, outputDir)
		},
	}
	workerCmd.Flags().IntVar(&workerID, "id", os.Getpid(), "worker ID")
	workerCmd.Flags().StringVar(&pluginPath, "plugin", "p", "path to .so plugin (required)")
	workerCmd.MarkFlagRequired("plugin")
	rootCmd.AddCommand(workerCmd)
	rootCmd.PersistentFlags().StringVarP(&outputDir, "output", "o", defaultOutputDir(), "directory for intermediate and output files")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func RunPipeline(filePath, soPath, outputDir string) {
	pf, err := pipeline.LoadPlugin(soPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load plugin: %v\n", err)
		os.Exit(1)
	}

	ds := datasource.FilesDataSource{FilePath: filePath}
	ppl := pipeline.NewPipeline(&ds)
	ppl.OutputDir = outputDir
	ppl.Map(pf.Map).Reduce(pf.Reduce)
	ppl.Start()
}
