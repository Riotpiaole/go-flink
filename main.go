package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hashicorp/raft"
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
		Use:   "go-flink",
		Short: "Distributed streaming MapReduce pipeline",
	}

	// run subcommand: start coordinator for a pipeline job on this node
	var pluginDir string
	var pluginName string
	var inputDir string
	var nReduce int
	var withSink bool
	var listenAddr string

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Start coordinator and run a pipeline job (workers must be started separately)",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("[run] plugin-dir=%s plugin=%s input=%s output=%s nReduce=%d\n",
				pluginDir, pluginName, inputDir, outputDir, nReduce)
			return runPipeline(inputDir, pluginDir, pluginName, outputDir, nReduce, withSink, listenAddr)
		},
	}
	runCmd.Flags().StringVar(&pluginDir, "plugin-dir", "./plugins", "directory containing .so plugins")
	runCmd.Flags().StringVar(&pluginName, "plugin", "", "plugin name to use for Map and Reduce stages (required)")
	runCmd.Flags().StringVar(&inputDir, "dir", "./datasets", "input file or directory")
	runCmd.Flags().IntVar(&nReduce, "n-reduce", 4, "number of reduce partitions")
	runCmd.Flags().BoolVar(&withSink, "sink", false, "append a Sink stage that writes results to MongoDB")
	runCmd.Flags().StringVar(&listenAddr, "listen", "", "optional TCP address to accept remote job submissions (e.g. :8000)")
	runCmd.MarkFlagRequired("plugin")

	// worker subcommand: start a worker that pulls tasks from the coordinator
	var workerID int
	var workerPluginDir string
	var workerCoordinatorAddr string

	workerCmd := &cobra.Command{
		Use:   "worker",
		Short: "Start a worker process that loads plugins on demand from --plugin-dir",
		Run: func(cmd *cobra.Command, args []string) {
			registry := pipeline.NewPluginRegistry(workerPluginDir)
			fmt.Printf("[worker %d] started, plugin-dir=%s output=%s coordinator=%s\n",
				workerID, workerPluginDir, outputDir, workerCoordinatorAddr)
			if workerCoordinatorAddr != "" {
				pipeline.StartWorkerRemote(workerID, registry, outputDir, workerCoordinatorAddr)
			} else {
				pipeline.StartWorker(workerID, registry, outputDir)
			}
		},
	}
	workerCmd.Flags().IntVar(&workerID, "id", os.Getpid(), "worker ID")
	workerCmd.Flags().StringVar(&workerPluginDir, "plugin-dir", "./plugins",
		"directory containing .so plugins (loaded on demand)")
	workerCmd.Flags().StringVar(&workerCoordinatorAddr, "coordinator", "",
		"coordinator RPC address (host:port); empty = embedded Unix socket mode")

	// submit subcommand: send a job to a running cluster
	var clusterAddr string
	var submitPlugin string
	var submitDir string
	var submitSourceType string
	var submitNReduce int
	var submitWithSink bool

	submitCmd := &cobra.Command{
		Use:   "submit",
		Short: "Submit a job to a running go-flink cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := pipeline.SourceConfig{
				Type:   submitSourceType,
				Config: map[string]string{"path": submitDir},
			}
			ppl, err := pipeline.NewPipelineFromConfig(cfg)
			if err != nil {
				return err
			}
			ppl.OutputDir = outputDir
			ppl.NReduce = submitNReduce
			ppl.Map(submitPlugin).Reduce(submitPlugin)
			if submitWithSink {
				ppl.Sink(submitPlugin)
			}
			return ppl.Submit(clusterAddr)
		},
	}
	submitCmd.Flags().StringVar(&clusterAddr, "cluster", "localhost:8000", "cluster RPC address (host:port)")
	submitCmd.Flags().StringVar(&submitPlugin, "plugin", "", "plugin name to use for Map and Reduce stages (required)")
	submitCmd.Flags().StringVar(&submitDir, "dir", "./datasets", "input file or directory")
	submitCmd.Flags().StringVar(&submitSourceType, "source-type", "file", "data source type: file | s3 | kafka")
	submitCmd.Flags().IntVar(&submitNReduce, "n-reduce", 4, "number of reduce partitions")
	submitCmd.Flags().BoolVar(&submitWithSink, "sink", false, "append a Sink stage that writes results to MongoDB")
	submitCmd.MarkFlagRequired("plugin")

	// node subcommand: unified node that becomes coordinator (leader) or worker (follower) via Raft
	var nodeID string
	var raftBind string
	var raftAdvertise string
	var rpcBind string
	var raftPeers string
	var nodePluginDir string
	var nodeDataDir string
	var nodeKafkaBrokers string

	nodeCmd := &cobra.Command{
		Use:   "node",
		Short: "Start a unified cluster node (Raft elects coordinator; others become workers)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if nodeID == "" {
				h, _ := os.Hostname()
				nodeID = h
			}

			// Parse comma-separated "raftAddr=rpcAddr" peer mappings.
			// Format: node-0:7000=node-0:8000,node-1:7000=node-1:8000,...
			peerRPCAddrs := map[raft.ServerAddress]string{}
			var raftPeerList []string
			for _, entry := range strings.Split(raftPeers, ",") {
				entry = strings.TrimSpace(entry)
				if entry == "" {
					continue
				}
				parts := strings.SplitN(entry, "=", 2)
				raftAddr := parts[0]
				raftPeerList = append(raftPeerList, raftAddr)
				if len(parts) == 2 {
					peerRPCAddrs[raft.ServerAddress(raftAddr)] = parts[1]
				}
			}
			if len(raftPeerList) == 0 {
				raftPeerList = []string{raftBind}
			}

			registry := pipeline.NewPluginRegistry(nodePluginDir)
			coord := pipeline.NewCoordinator(4, nil)
			if err := coord.InitRaft(nodeID, raftBind, raftAdvertise, raftPeerList, nodeDataDir); err != nil {
				return err
			}
			coord.StartWithRaft(rpcBind, peerRPCAddrs, registry, outputDir)

			// Block until signalled.
			select {}
		},
	}
	nodeCmd.Flags().StringVar(&nodeID, "node-id", "", "unique node identifier (default: hostname)")
	nodeCmd.Flags().StringVar(&raftBind, "raft-bind", ":7000", "host:port to listen on for Raft transport")
	nodeCmd.Flags().StringVar(&raftAdvertise, "raft-advertise", "", "host:port advertised to Raft peers (default: same as --raft-bind)")
	nodeCmd.Flags().StringVar(&rpcBind, "bind", ":8000", "host:port for worker/submit RPC")
	nodeCmd.Flags().StringVar(&raftPeers, "raft-peers", "",
		"comma-separated raftAddr=rpcAddr pairs for all cluster members (e.g. node-0:7000=node-0:8000,...)")
	nodeCmd.Flags().StringVar(&nodePluginDir, "plugin-dir", "./plugins", "directory containing .so plugins")
	nodeCmd.Flags().StringVar(&nodeDataDir, "data-dir", "./raft-data", "directory for Raft WAL and snapshots")
	nodeCmd.Flags().StringVar(&nodeKafkaBrokers, "kafka-brokers", "", "comma-separated Kafka broker addresses (reserved for Kafka task queue, not yet active)")

	// compacter subcommand: dedicated pool for GroupBy (compaction) and Sink tasks
	var compacterID int
	var compacterPluginDir string
	var compacterCoordinatorAddr string

	compacterCmd := &cobra.Command{
		Use:   "compacter",
		Short: "Start a Compacter that handles GroupBy and Sink tasks",
		Run: func(cmd *cobra.Command, args []string) {
			registry := pipeline.NewPluginRegistry(compacterPluginDir)
			fmt.Printf("[compacter %d] started, plugin-dir=%s output=%s coordinator=%s\n",
				compacterID, compacterPluginDir, outputDir, compacterCoordinatorAddr)
			if compacterCoordinatorAddr != "" {
				pipeline.StartCompacterRemote(compacterID, registry, outputDir, compacterCoordinatorAddr)
			} else {
				pipeline.StartCompacter(compacterID, registry, outputDir)
			}
		},
	}
	compacterCmd.Flags().IntVar(&compacterID, "id", os.Getpid(), "compacter ID")
	compacterCmd.Flags().StringVar(&compacterPluginDir, "plugin-dir", "./plugins",
		"directory containing .so plugins (needed for GroupBy Reduce calls)")
	compacterCmd.Flags().StringVar(&compacterCoordinatorAddr, "coordinator", "",
		"coordinator RPC address (host:port); empty = embedded Unix socket mode")

	rootCmd.AddCommand(runCmd, workerCmd, submitCmd, nodeCmd, compacterCmd)
	rootCmd.PersistentFlags().StringVarP(&outputDir, "output", "o", defaultOutputDir(),
		"directory for intermediate and output files")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runPipeline(inputDir, pluginDir, pluginName, outputDir string, nReduce int, withSink bool, listenAddr string) error {
	ds := datasource.FilesDataSource{FilePath: inputDir}
	ppl := pipeline.NewPipeline(&ds)
	ppl.OutputDir = outputDir
	ppl.NReduce = nReduce
	ppl.RPCAddr = listenAddr

	ppl.Map(pluginName).Reduce(pluginName)
	if withSink {
		ppl.Sink(pluginName)
	}

	ppl.Start()
	return nil
}
