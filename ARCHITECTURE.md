# Architecture

## Overview

go-flink is a distributed MapReduce engine. Every node runs the same binary. Raft elects one leader that acts as the coordinator (task scheduler, RPC server). The remaining nodes act as workers, polling the leader for tasks. When the leader dies, Raft elects a new one and the cluster resumes from replicated state — no human intervention required.

Workers execute Map, Filter, Reduce, and SelectKey stages. A separate Compacter pool handles GroupBy (sort-group compaction) and Sink (MongoDB upsert) stages. In the `node` mode each pod co-locates one compacter goroutine by default alongside its worker/coordinator role.

```
┌─────────────────────────────────────────────────────────────────────┐
│  go-flink node (3 replicas — StatefulSet in k8s)                    │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  Raft consensus layer (hashicorp/raft)                          │  │
│  │  Replicates: inFlight tasks, phaseIdx, phaseDone,              │  │
│  │              taskFiles, chunkStore (nil on followers)           │  │
│  └──────────────────┬─────────────────────────────────────────────┘  │
│                     │                                                 │
│             leader elected                                            │
│                     │                                                 │
│  ┌──────────────────▼─────────────────────────────────────────────┐  │
│  │  Coordinator role (leader only)                                 │  │
│  │  • FilesDataSource / KafkaDataSource → ChunkQueue              │  │
│  │  • Priority task queue (gods/priorityqueue)                    │  │
│  │  • RPC server :8000 (AskForTask, AskForCompactTask,            │  │
│  │    NoticeResult, GetChunk, SubmitJob, IsDone)                  │  │
│  │  • Sweeper goroutine (30 s timeout, up to 3 retries)           │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  Followers: run worker loop → poll leader RPC for tasks              │
│  All nodes: co-locate 1 compacter goroutine → poll AskForCompactTask │
└────────────────────────────┬─────────────────────────────────────────┘
                             │  TCP RPC (host:8000)
            ┌────────────────┴────────────────┐
            │                                 │
     ┌──────▼──────┐                   ┌──────▼──────┐
     │  Worker Pod │        ...        │  Worker Pod │
     │  (external) │                   │  (external) │
     │  PluginReg  │                   │  PluginReg  │
     │  wc.so      │                   │  wc.so      │
     └──────┬──────┘                   └──────┬──────┘
            │                                 │
            └───────────────┬─────────────────┘
                            │
                    ┌───────▼────────┐
                    │  /data/output  │◄──── Compacter pool
                    │  (shared PVC)  │      (GroupBy + Sink)
                    └────────────────┘
```

---

## Components

### Coordinator (`pipeline/coordinator.go`)

The coordinator implements `raft.FSM` directly — there is no separate FSM struct. Every state mutation (task enqueue, dispatch, completion, phase advance) goes through `proposeCmd()` → `raft.Apply()` → `FSM.Apply()` so all three Raft nodes converge to the same state.

The coordinator only serves RPCs when it is the Raft leader. Followers receive `Wait` from `AskForTask` so external workers keep retrying until they land on the leader.

Key responsibilities:
- `listenFromDataSource` — consumes `ChunkQueue`, stores chunk bytes in `chunkStore`, proposes `CmdEnqueueTask` via Raft
- `AskForTask` — dequeues the next `TaskInfo` for Map/Filter/Reduce/SelectKey, marks it in-flight, returns `MessageReply`
- `AskForCompactTask` — reactive dispatch for GroupBy (per completed Reduce bucket) and pre-enqueued Sink tasks
- `NoticeResult` — handles `TaskSuccess` / `TaskFailed` / `TaskContinue` from both workers and compacters
- `SubmitJob` — accepts a `JobSpec` from a remote client, builds a `DataSource`, wires up `ProcessAction` stages, starts streaming chunks
- `sweepTimedOutTasks` — runs every 5 s, re-enqueues tasks whose workers have gone silent for > 30 s
- `transitionToNextPhase` — advances `phaseIdx`, enqueues tasks for the new phase (chunk-parallel for Map/Filter, bucket-parallel for Reduce/SelectKey/Sink; GroupBy is not pre-enqueued)
- `watchLeadership` — reacts to `LeaderCh()`: on becoming leader activates coordinator role; on becoming follower starts a local worker loop pointed at the current leader's RPC address

### Raft FSM (`pipeline/raft_commands.go`)

The Raft log replicates coordinator metadata. Each log entry is a JSON-encoded `RaftCommand`:

| Command             | Effect on all nodes                                                                |
|---------------------|------------------------------------------------------------------------------------|
| `CmdEnqueueTask`    | Add task to `JobStatus` queue; store nil placeholder in `chunkStore` on followers  |
| `CmdDispatchTask`   | Record `DispatchedAt` timestamp in `inFlight`                                      |
| `CmdCompleteTask`   | Remove from `inFlight`, increment `phaseDone`, evict chunk                         |
| `CmdFailTask`       | Remove from `inFlight`, increment retries; re-enqueue or count as done             |
| `CmdAdvancePhase`   | Increment `phaseIdx`, reset `phaseDone` and `inFlight`                             |

`Snapshot` / `Restore` serialize `inFlight`, counters, `taskFiles`, and the queue contents so a new leader can resume without replaying the full log.

### Worker (`pipeline/worker.go`)

Workers are stateless. Each task assignment (`MessageReply`) carries everything needed to execute it:

| Field            | Purpose                                                                      |
|------------------|------------------------------------------------------------------------------|
| `PluginName`     | Which `.so` to load (e.g. `"wc"`)                                            |
| `ActionType`     | `MapTask` / `FilterTask` / `ReduceTask` / `SelectKeyTask`                    |
| `StageIdx`       | Current stage index — used to name output files (`mr-s<N>-…`)                |
| `InputStageIdx`  | Stage whose output files are this task's inputs                              |
| `ChunkID`        | UUID of the raw chunk (for Map stage 0 only)                                 |
| `BucketID`       | Consistent-hash bucket (for Reduce stages)                                   |
| `NReduce`        | Total bucket count (for hashing in Map stages)                               |
| `PhaseIdx`       | Guards against stale reports after a sweeper re-dispatch                     |
| `DispatchedAt`   | Echoed back so the coordinator can reject stale `NoticeResult` calls         |

Workers connect to the coordinator via TCP (`--coordinator host:port`) in distributed mode, or via Unix socket in embedded single-node mode.

### Compacter (`pipeline/compacter.go`)

Compacters are an independent pool — the coordinator has no knowledge of them. They poll `AskForCompactTask` RPC and handle two task types:

**GroupBy** — reactive compaction. The coordinator dispatches one GroupBy task per Reduce bucket the moment that bucket reports `TaskSuccess` (tracked in `reduceDoneBuckets`). Compacters:
1. Glob all `mr-out-s*-<bucket>` files from the shared output dir
2. Parse, sort, and group by key
3. Call `plugin.Reduce(key, values)` for each group
4. Atomically write `mr-out-<bucket>` (write to `.tmp`, then `os.Rename`)

**Sink** — pre-enqueued one-per-bucket tasks. Compacters read the final output files and upsert each key-value pair into MongoDB. MongoDB URI is injected via the `MONGO_URI` environment variable (K8s Secret).

Compacters connect to the coordinator the same way workers do — via TCP or Unix socket — and report results through the shared `NoticeResult` RPC.

In `go-flink node` mode, each pod co-locates `--compacters` (default 1) compacter goroutines started directly in `main.go`, pointing at the node's own RPC address. They are independent of the coordinator's `watchLeadership` loop.

### Plugin Registry (`pipeline/pluginregistry.go`)

`PluginRegistry` wraps `plugin.Open` with a read-write mutex and a lazy-load cache. The first call to `Get("wc")` opens `<pluginDir>/wc.so`, validates the `Map` and `Reduce` symbols, and caches the `PluginFuncs`. Subsequent calls return from cache. Dropping a new `.so` into the plugin directory takes effect on the next task that names it — no node restart required.

### Pipeline Builder (`pipeline/pipeline.go`)

```go
pipeline.NewPipeline(src).
    Map("tokenizer").        // StageSpec{Type: MapTask,       PluginName: "tokenizer"}
    Filter("stopwords").     // StageSpec{Type: FilterTask,    PluginName: "stopwords"}
    Reduce("word_count").    // StageSpec{Type: ReduceTask,    PluginName: "word_count"}
    GroupBy("word_count").   // StageSpec{Type: GroupByTask,   PluginName: "word_count"}
    SelectKey("rekey").      // StageSpec{Type: SelectKeyTask, PluginName: "rekey"}
    Sink("file_sink").       // StageSpec{Type: SinkTask,      PluginName: "file_sink"}
    Start()                  // embedded single-node, or .Submit("addr") for cluster
```

Each builder call appends a `StageSpec` to the `JobSpec`. The coordinator drives through stages as a generic cursor: `phaseIdx` indexes into `Stages[]`, and `transitionToNextPhase` dispatches the right task shape (chunk-parallel for Map/Filter, bucket-parallel for Reduce/SelectKey/Sink; GroupBy is not pre-enqueued and dispatches reactively).

Stage ordering constraints:
- `GroupBy` must immediately follow `Reduce`
- `GroupBy` + `Sink` → Compacter pool
- `Map`, `Filter`, `Reduce`, `SelectKey` → Worker pool

### DataSource (`pipeline/datasource/datasource.go`)

`FilesDataSource` walks the input directory and reads each file in full. Files are pushed as `FileChunk{FileName, Content}` into a `ChunkQueue` — a mutex-protected FIFO with a done flag. The coordinator's `listenFromDataSource` goroutine polls the queue; the datasource goroutine closes it when all files are pushed.

`ChunkQueue` decouples production from consumption: the datasource goroutine never blocks waiting for the coordinator, and the coordinator's RPC server is free to handle worker requests concurrently.

`NewFromConfig` supports `"file"` (active), `"s3"` (stub), and `"kafka"` (stub) source types via the `SubmitJob` RPC path.

### Job types (`pipeline/job.go`)

```go
type JobSpec struct {
    JobID     string
    Source    SourceConfig      // "file" | "s3" | "kafka" + params
    Stages    []StageSpec       // ordered pipeline graph
    OutputDir string
    NReduce   int
}
```

A `JobSpec` is serialized over RPC by `go-flink submit` → `Coordinator.SubmitJob`. The coordinator rebuilds the `DataSource` from `SourceConfig` and sets `ProcessAction` from `Stages`.

---

## Data flow

```
Input (file / Kafka stub / S3 stub)
    │
    ▼
DataSource.StreamChunks(ctx)
    │  FileChunk{FileName, Content} per file
    ▼
ChunkQueue (thread-safe FIFO)
    │
    ▼
Coordinator.listenFromDataSource()
    │  assign UUID ChunkID
    │  store raw bytes in chunkStore (leader) + disk fallback
    │  proposeCmd(CmdEnqueueTask) ──► Raft log ──► FSM.Apply() on all 3 nodes
    ▼
Priority Task Queue  (phase 0 = Map)
    │
    ├── Worker polls AskForTask ──► GetChunk(ChunkID) [stage 0 only]
    │   run plugin.Map(filename, content)
    │   write mr-s0-<chunkID>-<bucket> (one file per hash bucket)
    │   NoticeResult(TaskSuccess)
    │
    ▼  [all Map tasks done → transitionToNextPhase]
Priority Task Queue  (phase 1 = Reduce, NReduce tasks)
    │
    ├── Worker polls AskForTask
    │   glob mr-s0-*-<bucketID> from shared output dir
    │   sort + group by key
    │   run plugin.Reduce(key, values)
    │   write mr-out-s1-<bucketID>
    │   NoticeResult(TaskSuccess)
    │       │
    │       └──► reduceDoneBuckets[bucket]=true
    │            Compacter polls AskForCompactTask (reactive GroupBy)
    │            glob mr-out-s*-<bucket>
    │            sort + group → plugin.Reduce
    │            atomic write mr-out-<bucket>
    │            NoticeResult(TaskSuccess)
    │
    ▼  [all Reduce + GroupBy tasks done]
Pre-enqueued Sink tasks (one per bucket)
    │
    └── Compacter polls AskForCompactTask
        read mr-out-<bucket>  (after GroupBy)
        upsert each KV to MongoDB
        NoticeResult(TaskSuccess)
    │
    ▼
Done() = true  →  workers receive Shutdown
```

---

## Coordinator ↔ Worker/Compacter call sequence

### Map phase

```
Worker                                      Coordinator (leader)
  │                                              │
  │  ── AskForTask(MsgType=AskForTask) ────────► │
  │                                              │  dequeue TaskInfo from JobStatus queue
  │                                              │  mark task in-flight; set DispatchedAt
  │ ◄─ TaskAlloc(ChunkID, StageIdx=0, ─────────  │
  │              PluginName, NReduce,            │
  │              PhaseIdx, DispatchedAt)         │
  │                                              │
  │  ── GetChunk(ChunkID) ─────────────────────► │
  │ ◄─ ChunkReply(Content []byte) ─────────────  │  raw bytes from chunkStore
  │                                              │
  │  [PluginRegistry.Get("wc")]                  │
  │  [run plugin.Map(filename, content)]         │
  │  [write mr-s0-<chunkID>-<bucket> per key]   │
  │                                              │
  │  ── NoticeResult(TaskSuccess, TaskID, ─────► │
  │                  PhaseIdx, DispatchedAt)     │  proposeCmd(CmdCompleteTask)
  │                                              │  phaseDone++; delete chunk from chunkStore
  │                                              │  if all done → transitionToNextPhase()
```

### Reduce phase

```
Worker                                      Coordinator (leader)
  │                                              │
  │  ── AskForTask(MsgType=AskForTask) ────────► │
  │                                              │  dequeue reduce TaskInfo (one per bucket)
  │ ◄─ TaskAlloc(BucketID, StageIdx=1, ────────  │
  │              InputStageIdx=0, PluginName)    │
  │                                              │
  │  [glob mr-s0-*-<BucketID> from output dir]  │
  │  [sort + group by key]                       │
  │  [run plugin.Reduce(key, values)]            │
  │  [write mr-out-s1-<BucketID>]               │
  │                                              │
  │  ── NoticeResult(TaskSuccess, TaskID, ─────► │
  │                  PhaseIdx, DispatchedAt)     │  proposeCmd(CmdCompleteTask)
  │                                              │  reduceDoneBuckets[bucket]=true
  │                                              │  phaseDone++
  │                                              │  if all done → Done() = true
```

### GroupBy phase (reactive, Compacter)

```
Compacter                                   Coordinator (leader)
  │                                              │
  │  ── AskForCompactTask ─────────────────────► │
  │                                              │  scan reduceDoneBuckets for undispatched bucket
  │                                              │  compactDispatched[bucket]=true
  │ ◄─ GroupByTask(BucketID) ──────────────────  │
  │                                              │
  │  [glob mr-out-s*-<BucketID>]                │
  │  [parse, sort, group by key]                 │
  │  [plugin.Reduce(key, values)]               │
  │  [atomic write mr-out-<BucketID>]           │
  │                                              │
  │  ── NoticeResult(TaskSuccess) ─────────────► │  phaseDone++ for GroupBy phase
```

### Sink phase (Compacter)

```
Compacter                                   Coordinator (leader)
  │                                              │
  │  ── AskForCompactTask ─────────────────────► │
  │                                              │  dequeue pre-enqueued Sink task
  │ ◄─ SinkTask(BucketID) ─────────────────────  │
  │                                              │
  │  [read mr-out-<BucketID>]                   │
  │  [upsert each KV to MongoDB]                │
  │                                              │
  │  ── NoticeResult(TaskSuccess) ─────────────► │  phaseDone++; if all done → Done()
```

### Follower handling

```
Worker                              Coordinator (follower)
  │                                       │
  │  ── AskForTask ──────────────────────► │
  │ ◄─ Wait ──────────────────────────────  │  (raftNode.State() != Leader)
  │                                       │
  │  [sleep 200ms, retry]                 │
  │  [next request may land on leader]    │
```

### Task failure and retry

```
Worker                                      Coordinator
  │                                              │
  │  [task fails mid-execution]                  │
  │  ── NoticeResult(TaskFailed, TaskID, ──────► │
  │                  PhaseIdx, DispatchedAt)     │  proposeCmd(CmdFailTask)
  │                                              │  task.Retries++
  │                                              │  if Retries < 3 → re-enqueue
  │                                              │  if Retries >= 3 → phaseDone++ (give up)
```

### Worker stall / crash (sweeper path)

```
Worker                              Coordinator (sweeper, every 5 s)
  │                                       │
  │  [process hangs or dies]              │
  │  [no NoticeResult arrives]            │  now − task.DispatchedAt > 30 s
  │                                       │  task.Retries++
  │                                       │  re-enqueue (or count done after 3 retries)
```

---

## Fault tolerance

| Failure                                | Behaviour                                                                     |
|----------------------------------------|-------------------------------------------------------------------------------|
| Worker crashes mid-task                | Sweeper detects silence after 30 s → re-enqueues (up to 3 retries)            |
| Worker returns TaskFailed              | Re-enqueued immediately via Raft log                                          |
| Leader crashes                         | Raft elects new leader; new leader resumes from replicated `inFlight` + queue |
| GroupBy stalls on leader failover      | `reduceDoneBuckets`/`compactDispatched` are in-memory only (not Raft-replicated) — new leader re-dispatches after ~30 s sweeper timeout |
| Stale report after sweeper re-dispatch | Rejected by `DispatchedAt` token mismatch in `NoticeResult`                   |
| Stale report from old phase            | Rejected by `PhaseIdx` guard                                                  |
| Task exhausts retries                  | Counted as done (partial results), pipeline continues                         |
| Duplicate map output                   | Workers detect existing checkpoint file and skip re-execution                 |

---

## Kubernetes deployment

```
k8s/
├── headless-service.yaml       # ClusterIP: None — stable pod DNS for Raft (go-flink-0.go-flink:7000)
├── rpc-service.yaml            # ClusterIP — load-balanced worker/compacter RPC (go-flink-rpc:8000)
├── statefulset.yaml            # 3 coordinator/node pods (Raft consensus + task scheduling)
├── worker-deployment.yaml      # 3 worker pods (stateless, scale freely)
├── compacter-deployment.yaml   # 2 compacter pods (GroupBy + Sink, scale freely)
├── kafka.yaml                  # Kafka KRaft (apache/kafka:4.0.0): kafka-headless + kafka ClusterIP + StatefulSet
├── output-pvc-minikube.yaml    # ReadWriteMany hostPath PV — shared intermediate files
├── plugins-pvc-minikube.yaml   # ReadWriteOnce — plugin .so volume (minikube)
└── plugins-pvc.yaml            # ReadWriteMany variant for production
```

**Coordinator StatefulSet** — 3 replicas running `go-flink node`. Raft consensus is established at boot; the elected leader handles task scheduling and RPC. Raft WAL stored in per-pod PVC at `/data/raft`. Each pod also co-locates 1 compacter goroutine (started in `main.go`, not by the coordinator itself).

**Worker Deployment** — stateless, scales independently. Init container copies bundled plugins from the image into the shared plugins PVC. Workers mount both the plugins PVC and the shared output PVC.

**Compacter Deployment** — 2 replicas handling GroupBy (reactive) and Sink tasks. Independent of the coordinator — connects over RPC the same way workers do. Mounts plugins PVC and shared output PVC. MongoDB URI injected via K8s Secret (`pipeline-secrets`).

**Kafka (KRaft)** — single-node `apache/kafka:4.0.0` StatefulSet, no ZooKeeper. `kafka-headless` (`clusterIP: None`) gives `kafka-0.kafka-headless` stable DNS required by `KAFKA_CONTROLLER_QUORUM_VOTERS`. `kafka` ClusterIP service exposes port 9092. Data source integration is stubbed.

**Shared output** — `ReadWriteMany` PVC backed by a hostPath PV on minikube (single-node; all pods on the same node). On multi-node clusters, replace with NFS, EFS, or Azure File.

**Raft peer discovery** — StatefulSet gives each pod a stable DNS name (`go-flink-<N>.go-flink`). The `--raft-bind :7000` / `--raft-advertise $(POD_NAME).go-flink:7000` split lets the TCP listener bind immediately while advertising the stable DNS name to peers. A retry loop (up to 60 s) handles DNS propagation delay at pod startup.

---

## File layout

```
.
├── main.go                          # CLI: run / worker / compacter / node / submit (each as new<Name>Cmd)
├── Dockerfile                       # Multi-stage build (builder + alpine runtime)
├── plugin/
│   └── wc.go                        # Word-count plugin example
├── pipeline/
│   ├── pipeline.go                  # Pipeline builder (Map/Filter/Reduce/GroupBy/SelectKey/Sink)
│   ├── coordinator.go               # raft.FSM + task scheduler + RPC server
│   ├── raft_commands.go             # RaftCommand types, Apply, Snapshot, Restore
│   ├── raft_test.go                 # In-memory Raft FSM unit tests
│   ├── worker.go                    # Worker loop, Map/Reduce/SelectKey execution
│   ├── compacter.go                 # Compacter loop, GroupBy (atomic write) + Sink (MongoDB)
│   ├── pluginregistry.go            # Lazy-loading .so plugin cache
│   ├── loadplugin.go                # plugin.Open + symbol validation
│   ├── job.go                       # JobSpec, StageSpec, SourceConfig, KafkaConfig (stub)
│   ├── rpc.go                       # MessageSend/Reply, KeyValue, ChunkRequest/Reply
│   ├── coordinator_constants.go     # TaskStatus enum + priority map
│   ├── interface.go                 # TaskType enum (MapTask, ReduceTask, GroupByTask, …)
│   └── datasource/
│       └── datasource.go            # FilesDataSource, ChunkQueue, NewFromConfig
└── k8s/
    ├── statefulset.yaml
    ├── worker-deployment.yaml
    ├── compacter-deployment.yaml
    ├── headless-service.yaml
    ├── rpc-service.yaml
    ├── kafka.yaml
    ├── output-pvc-minikube.yaml
    ├── plugins-pvc-minikube.yaml
    └── plugins-pvc.yaml
```
