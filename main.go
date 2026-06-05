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

// =============================================================================
// ROOT CMD
// =============================================================================

func main() {
	var outputDir string

	rootCmd := &cobra.Command{
		Use:   "go-flink",
		Short: "Distributed streaming MapReduce pipeline",
	}

	rootCmd.PersistentFlags().StringVarP(&outputDir, "output", "o",
		defaultOutputDir(), "directory for intermediate and output files")

	rootCmd.AddCommand(
		newRunCmd(&outputDir),
		newWorkerCmd(&outputDir),
		newSubmitCmd(&outputDir),
		newNodeCmd(&outputDir),
		newCompacterCmd(&outputDir),
	)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// =============================================================================
// SUBCOMMANDS
// =============================================================================

// newRunCmd returns the "run" subcommand.
//
// Purpose:
//
//	Starts the coordinator on this machine and streams input chunks from a
//	local FilesDataSource into the pipeline. Workers must be started separately
//	(via `go-flink worker`) and connect over RPC to pull tasks.
//
// Usage:
//
//	go-flink run --plugin <name> [flags]
//
// Flags:
//
//	--plugin-dir   directory of .so plugins           (default: ./plugins)
//	--plugin       plugin name for Map+Reduce          (required)
//	--dir          input file or directory             (default: ./datasets)
//	--n-reduce     number of reduce partitions         (default: 4)
//	--sink         append a MongoDB Sink stage
//	--listen       TCP addr for remote job submission  (e.g. :8000)
func newRunCmd(outputDir *string) *cobra.Command {
	var pluginDir string
	var pluginName string
	var inputDir string
	var nReduce int
	var withSink bool
	var listenAddr string

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Start coordinator and run a pipeline job (workers must be started separately)",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("[run] plugin-dir=%s plugin=%s input=%s output=%s nReduce=%d\n",
				pluginDir, pluginName, inputDir, *outputDir, nReduce)
			return runPipeline(inputDir, pluginDir, pluginName, *outputDir, nReduce, withSink, listenAddr)
		},
	}
	cmd.Flags().StringVar(&pluginDir, "plugin-dir", "./plugins", "directory containing .so plugins")
	cmd.Flags().StringVar(&pluginName, "plugin", "", "plugin name to use for Map and Reduce stages (required)")
	cmd.Flags().StringVar(&inputDir, "dir", "./datasets", "input file or directory")
	cmd.Flags().IntVar(&nReduce, "n-reduce", 4, "number of reduce partitions")
	cmd.Flags().BoolVar(&withSink, "sink", false, "append a Sink stage that writes results to MongoDB")
	cmd.Flags().StringVar(&listenAddr, "listen", "", "optional TCP address to accept remote job submissions (e.g. :8000)")
	cmd.MarkFlagRequired("plugin")

	return cmd
}

// newWorkerCmd returns the "worker" subcommand.
//
// Purpose:
//
//	Starts a worker process that polls the coordinator for tasks via RPC and
//	executes Map, Filter, Reduce, and SelectKey stages. Plugins are loaded
//	on demand from --plugin-dir as shared objects (.so files).
//
// Usage:
//
//	go-flink worker [flags]
//
// Flags:
//
//	--id           worker ID                                       (default: PID)
//	--plugin-dir   directory of .so plugins                        (default: ./plugins)
//	--coordinator  coordinator RPC address (host:port);
//	               empty = embedded Unix socket mode
func newWorkerCmd(outputDir *string) *cobra.Command {
	var workerID int
	var workerPluginDir string
	var workerCoordinatorAddr string

	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Start a worker process that loads plugins on demand from --plugin-dir",
		Run: func(cmd *cobra.Command, args []string) {
			registry := pipeline.NewPluginRegistry(workerPluginDir)
			fmt.Printf("[worker %d] started, plugin-dir=%s output=%s coordinator=%s\n",
				workerID, workerPluginDir, *outputDir, workerCoordinatorAddr)
			if workerCoordinatorAddr != "" {
				pipeline.StartWorkerRemote(workerID, registry, *outputDir, workerCoordinatorAddr)
			} else {
				pipeline.StartWorker(workerID, registry, *outputDir)
			}
		},
	}
	cmd.Flags().IntVar(&workerID, "id", os.Getpid(), "worker ID")
	cmd.Flags().StringVar(&workerPluginDir, "plugin-dir", "./plugins",
		"directory containing .so plugins (loaded on demand)")
	cmd.Flags().StringVar(&workerCoordinatorAddr, "coordinator", "",
		"coordinator RPC address (host:port); empty = embedded Unix socket mode")

	return cmd
}

// newSubmitCmd returns the "submit" subcommand.
//
// Purpose:
//
//	Sends a job specification to an already-running go-flink cluster over RPC.
//	The cluster coordinator receives the job, builds the pipeline stages, and
//	begins processing. Supports file, s3 (stub), and kafka (stub) data sources.
//
// Usage:
//
//	go-flink submit --plugin <name> [flags]
//
// Flags:
//
//	--cluster      coordinator RPC address             (default: localhost:8000)
//	--plugin       plugin name for Map+Reduce          (required)
//	--dir          input file or directory             (default: ./datasets)
//	--source-type  data source: file | s3 | kafka      (default: file)
//	--n-reduce     number of reduce partitions         (default: 4)
//	--sink         append a MongoDB Sink stage
func newSubmitCmd(outputDir *string) *cobra.Command {
	var clusterAddr string
	var submitPlugin string
	var submitDir string
	var submitSourceType string
	var submitNReduce int
	var submitWithSink bool

	cmd := &cobra.Command{
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
			ppl.OutputDir = *outputDir
			ppl.NReduce = submitNReduce
			ppl.Map(submitPlugin).Reduce(submitPlugin)
			if submitWithSink {
				ppl.Sink(submitPlugin)
			}
			return ppl.Submit(clusterAddr)
		},
	}
	cmd.Flags().StringVar(&clusterAddr, "cluster", "localhost:8000", "cluster RPC address (host:port)")
	cmd.Flags().StringVar(&submitPlugin, "plugin", "", "plugin name to use for Map and Reduce stages (required)")
	cmd.Flags().StringVar(&submitDir, "dir", "./datasets", "input file or directory")
	cmd.Flags().StringVar(&submitSourceType, "source-type", "file", "data source type: file | s3 | kafka")
	cmd.Flags().IntVar(&submitNReduce, "n-reduce", 4, "number of reduce partitions")
	cmd.Flags().BoolVar(&submitWithSink, "sink", false, "append a Sink stage that writes results to MongoDB")
	cmd.MarkFlagRequired("plugin")

	return cmd
}

// newNodeCmd returns the "node" subcommand.
//
// Purpose:
//
//	Starts a homogeneous cluster node. All nodes start equal; Raft consensus
//	elects one as the coordinator (leader) which runs the task scheduler and
//	RPC server, while the rest act as workers. On leader failover the new
//	Raft leader automatically takes over coordination — no manual intervention
//	required. This is the production/K8s entry point.
//
//	Each node also co-locates --compacters compacter goroutines (default 1).
//	On leader election the compacters point to this node's own RPC address;
//	on follower transitions they point to the leader's RPC address.
//
// Usage:
//
//	go-flink node --raft-peers node-0:7000=node-0:8000,node-1:7000=node-1:8000 [flags]
//
// Flags:
//
//	--node-id        unique node identifier               (default: hostname)
//	--raft-bind      host:port for Raft transport         (default: :7000)
//	--raft-advertise host:port advertised to peers        (default: same as --raft-bind)
//	--bind           host:port for worker/submit RPC      (default: :8000)
//	--raft-peers     comma-separated raftAddr=rpcAddr pairs for all cluster members
//	--plugin-dir     directory of .so plugins             (default: ./plugins)
//	--data-dir       directory for Raft WAL and snapshots (default: ./raft-data)
//	--compacters     number of compacter goroutines per node (default: 1)
//	--kafka-brokers  Kafka broker addresses               (reserved, not yet active)
func newNodeCmd(outputDir *string) *cobra.Command {
	var nodeID string
	var raftBind string
	var raftAdvertise string
	var rpcBind string
	var raftPeers string
	var nodePluginDir string
	var nodeDataDir string
	var nodeKafkaBrokers string
	var numCompacters int

	cmd := &cobra.Command{
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
			coord.StartWithRaft(rpcBind, peerRPCAddrs, registry, *outputDir)

			for i := 0; i < numCompacters; i++ {
				id := i
				go pipeline.StartCompacterRemote(id, registry, *outputDir, rpcBind)
			}

			// Block until signalled.
			select {}
		},
	}
	cmd.Flags().StringVar(&nodeID, "node-id", "", "unique node identifier (default: hostname)")
	cmd.Flags().StringVar(&raftBind, "raft-bind", ":7000", "host:port to listen on for Raft transport")
	cmd.Flags().StringVar(&raftAdvertise, "raft-advertise", "", "host:port advertised to Raft peers (default: same as --raft-bind)")
	cmd.Flags().StringVar(&rpcBind, "bind", ":8000", "host:port for worker/submit RPC")
	cmd.Flags().StringVar(&raftPeers, "raft-peers", "",
		"comma-separated raftAddr=rpcAddr pairs for all cluster members (e.g. node-0:7000=node-0:8000,...)")
	cmd.Flags().StringVar(&nodePluginDir, "plugin-dir", "./plugins", "directory containing .so plugins")
	cmd.Flags().StringVar(&nodeDataDir, "data-dir", "./raft-data", "directory for Raft WAL and snapshots")
	cmd.Flags().IntVar(&numCompacters, "compacters", 1, "number of compacter goroutines to co-locate on this node")
	cmd.Flags().StringVar(&nodeKafkaBrokers, "kafka-brokers", "", "comma-separated Kafka broker addresses (reserved for Kafka task queue, not yet active)")

	return cmd
}

// newCompacterCmd returns the "compacter" subcommand.
//
// Purpose:
//
//	Starts a Compacter process dedicated to GroupBy and Sink tasks. Compacters
//	poll the coordinator via AskForCompactTask RPC. GroupBy tasks sort and
//	group reduce output by key; Sink tasks upsert final key-value pairs into
//	MongoDB. Compacters run in a separate pool from workers so heavy GroupBy
//	compaction does not block Map/Reduce progress.
//
// Usage:
//
//	go-flink compacter [flags]
//
// Flags:
//
//	--id           compacter ID                                    (default: PID)
//	--plugin-dir   directory of .so plugins                        (default: ./plugins)
//	--coordinator  coordinator RPC address (host:port);
//	               empty = embedded Unix socket mode
func newCompacterCmd(outputDir *string) *cobra.Command {
	var compacterID int
	var compacterPluginDir string
	var compacterCoordinatorAddr string

	cmd := &cobra.Command{
		Use:   "compacter",
		Short: "Start a Compacter that handles GroupBy and Sink tasks",
		Run: func(cmd *cobra.Command, args []string) {
			registry := pipeline.NewPluginRegistry(compacterPluginDir)
			fmt.Printf("[compacter %d] started, plugin-dir=%s output=%s coordinator=%s\n",
				compacterID, compacterPluginDir, *outputDir, compacterCoordinatorAddr)
			if compacterCoordinatorAddr != "" {
				pipeline.StartCompacterRemote(compacterID, registry, *outputDir, compacterCoordinatorAddr)
			} else {
				pipeline.StartCompacter(compacterID, registry, *outputDir)
			}
		},
	}
	cmd.Flags().IntVar(&compacterID, "id", os.Getpid(), "compacter ID")
	cmd.Flags().StringVar(&compacterPluginDir, "plugin-dir", "./plugins",
		"directory containing .so plugins (needed for GroupBy Reduce calls)")
	cmd.Flags().StringVar(&compacterCoordinatorAddr, "coordinator", "",
		"coordinator RPC address (host:port); empty = embedded Unix socket mode")

	return cmd
}

// =============================================================================
// HELPERS
// =============================================================================

func defaultOutputDir() string {
	wd, err := os.Getwd()
	if err != nil {
		return pipeline.DefaultOutputDir
	}
	return filepath.Join(wd, pipeline.DefaultOutputDir)
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
