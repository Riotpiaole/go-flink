# go-flink

A distributed MapReduce pipeline engine written in Go. Processing stages run as `.so` plugins; the cluster uses Raft for leader election and fault-tolerant task scheduling.

## Modes

| Mode | Command | Use case |
|---|---|---|
| Single-node (embedded) | `go-flink run` | Local testing — coordinator + workers in one process |
| Distributed cluster | `go-flink node` | Multi-node Kubernetes deployment via Raft |
| Remote submission | `go-flink submit` | Send a job to a running cluster |
| Worker-only | `go-flink worker` | Attach additional workers to an existing coordinator |
| Compacter-only | `go-flink compacter` | Attach additional compacters (GroupBy / Sink) to an existing coordinator |

---

## Quick start — single node

### 1. Build

```bash
go build -o go-flink .

# Build the bundled word-count plugin
CGO_ENABLED=1 go build -buildmode=plugin -o plugins/wc.so ./plugin/
```

### 2. Run word count

```bash
./go-flink run --plugin wc --dir ./datasets --n-reduce 4
```

This starts an embedded coordinator, reads all files from `./datasets`, and spins up workers in-process. Results land in `./mr-out/<jobUUID>/` with stage-scoped subdirectories.

### 3. Add workers for more parallelism

In separate terminals (or processes):

```bash
./go-flink worker --plugin-dir ./plugins
./go-flink worker --plugin-dir ./plugins
./go-flink worker --plugin-dir ./plugins
```

Workers register automatically by polling the coordinator's Unix socket. Add or remove them at any time while the job runs.

---

## Distributed cluster

Every node runs the same binary. Raft elects one leader (coordinator); others act as workers. Each node also co-locates one compacter goroutine by default (`--compacters 1`).

```bash
# Node 0
./go-flink node \
  --node-id node-0 \
  --raft-bind :7000 \
  --raft-advertise node-0.go-flink:7000 \
  --raft-peers "node-0.go-flink:7000=node-0.go-flink:8000,node-1.go-flink:7000=node-1.go-flink:8000,node-2.go-flink:7000=node-2.go-flink:8000" \
  --bind :8000 \
  --plugin-dir /plugins \
  --data-dir /data/raft \
  --compacters 1

# Node 1, Node 2 — same flags, different --node-id and addresses
```

Submit a job once the cluster is up:

```bash
./go-flink submit \
  --cluster node-0.go-flink:8000 \
  --plugin wc \
  --dir /data/input \
  --n-reduce 4
```

---

## Kubernetes (minikube)

```bash
# Mount local datasets into minikube
minikube mount ./datasets:/mnt/datasets &

# Build image into minikube's daemon
eval $(minikube docker-env)
docker build -t goflink:v1.0.0 .

# Deploy
kubectl apply -f k8s/output-pvc-minikube.yaml \
              -f k8s/plugins-pvc-minikube.yaml \
              -f k8s/headless-service.yaml \
              -f k8s/rpc-service.yaml \
              -f k8s/statefulset.yaml \
              -f k8s/worker-deployment.yaml \
              -f k8s/compacter-deployment.yaml \
              -f k8s/kafka.yaml

# Wait for cluster to elect a leader
kubectl rollout status statefulset/go-flink

# Submit a job
kubectl port-forward svc/go-flink-rpc 8000:8000 &
./go-flink submit --cluster localhost:8000 --plugin wc --dir /data/input --n-reduce 4
```

See [k8s/](k8s/) for manifest details.

---

## Writing a plugin

A plugin is a Go file compiled with `-buildmode=plugin`. Export exactly:

```go
func Map(filename string, contents string) []pipeline.KeyValue
func Reduce(key string, values []string) string
```

See [plugin/wc.go](plugin/wc.go) for a complete word-count example.

```bash
CGO_ENABLED=1 go build -buildmode=plugin -o plugins/myplugin.so myplugin.go
```

Plugins are loaded on first use via `PluginRegistry.Get(name)` — drop a new `.so` into the plugin directory and the next task that names it will load it automatically. No restart needed.

---

## Pipeline builder

```go
ds := datasource.FilesDataSource{FilePath: "./datasets"}
pipeline.NewPipeline(&ds).
    Map("tokenizer").
    Map("normalizer").
    Reduce("word_count").
    GroupBy("word_count").   // reactive compaction: starts per bucket as Reduce finishes
    Sink("file_sink").
    Start()                  // embedded single-node
    // or .Submit("coordinator:8000") for cluster
```

Each stage names a plugin. Stages chain through intermediate files automatically.

Stage ordering rules:
- `GroupBy` must immediately follow `Reduce`
- `GroupBy` + `Sink` tasks are routed to the Compacter pool
- `Map`, `Filter`, `Reduce`, `SelectKey` tasks are routed to the Worker pool

---

## CLI reference

```
go-flink run
    --plugin <name>        plugin stem (e.g. "wc" for plugins/wc.so)  [required]
    --dir <path>           input directory                              [default: ./datasets]
    --n-reduce <int>       reduce partitions                           [default: 4]
    --plugin-dir <dir>     .so plugin directory                        [default: ./plugins]
    --sink                 append MongoDB sink stage
    --listen <addr>        expose SubmitJob RPC for remote submissions
    -o <dir>               output directory                            [default: ./mr-out]

go-flink worker
    --plugin-dir <dir>     .so plugin directory                        [default: ./plugins]
    --coordinator <addr>   coordinator host:port (empty = Unix socket)
    --id <int>             worker ID                                   [default: PID]
    -o <dir>               output directory

go-flink compacter
    --plugin-dir <dir>     .so plugin directory                        [default: ./plugins]
    --coordinator <addr>   coordinator host:port (empty = Unix socket)
    --id <int>             compacter ID                                [default: PID]
    -o <dir>               output directory

go-flink node
    --node-id <string>      unique identifier                          [default: hostname]
    --raft-bind <addr>      TCP listen address for Raft transport      [default: :7000]
    --raft-advertise <addr> address peers use to reach this node
    --raft-peers <list>     comma-separated raftAddr=rpcAddr pairs
    --bind <addr>           TCP listen address for worker/submit RPC   [default: :8000]
    --data-dir <dir>        Raft WAL + snapshot directory              [default: ./raft-data]
    --plugin-dir <dir>      .so plugin directory                       [default: ./plugins]
    --compacters <int>      compacter goroutines to co-locate          [default: 1]
    -o <dir>                output directory

go-flink submit
    --cluster <addr>       coordinator host:port                       [required]
    --plugin <name>        plugin stem                                  [required]
    --dir <path>           input directory
    --source-type <type>   file | s3 | kafka                           [default: file]
    --n-reduce <int>       reduce partitions                           [default: 4]
    --sink                 append MongoDB sink stage
    -o <dir>               output directory
```

---

## Intermediate file naming

Files are written under a job-scoped directory tree so concurrent and repeated jobs never collide:

```
<outputDir>/<jobID>/<actionType>/mr-<phaseUUID>-<rest>
```

| Stage | Output path | MongoDB collection |
|---|---|---|
| Map / Filter | `<out>/<jobID>/map/mr-<phaseUUID>-<chunkID>-<bucket>` | `map_task` — `"<jobID>:<phaseUUID>:<taskID>"` |
| Reduce | `<out>/<jobID>/reduce/mr-<phaseUUID>-<bucket>` | `reduction` — `"<jobID>:<phaseUUID>:<bucket>"` |
| SelectKey | `<out>/<jobID>/selectkey/mr-<phaseUUID>-<srcBucket>-<newBucket>` | *(not tracked)* |
| GroupBy | `<out>/<jobID>/groupby/mr-<phaseUUID>-<bucket>` | `compaction` — dispatch claim |
| Sink | → MongoDB (`output` collection) | `sink_result` — audit record |

- `jobID` — stable UUID per job (generated in `SubmitJob` or `pipeline.Start()`)
- `phaseUUID` — UUID per phase, replicated through the Raft WAL so it survives leader failover
- MongoDB `_id` format: `"<jobID>:<phaseUUID>:<taskID-or-bucket>"` — globally unique across re-runs

---

## Dependencies

- [hashicorp/raft](https://github.com/hashicorp/raft) — Raft consensus for leader election and log replication
- [hashicorp/raft-boltdb/v2](https://github.com/hashicorp/raft-boltdb) — BoltDB-backed persistent Raft WAL and stable store (replaces in-memory store)
- [spf13/cobra](https://github.com/spf13/cobra) — CLI
- [emirpasic/gods](https://github.com/emirpasic/gods) — priority queue for task scheduling
- [serialx/hashring](https://github.com/serialx/hashring) — consistent hashing for reduce bucket assignment
- [google/uuid](https://github.com/google/uuid) — job / chunk / phase identity
- [go.mongodb.org/mongo-driver/v2](https://pkg.go.dev/go.mongodb.org/mongo-driver/v2) — MongoDB sink + job-keying metadata
