# Architecture

## Overview

go-flink is a distributed MapReduce engine. Every node runs the same binary. Raft elects one leader that acts as the coordinator (task scheduler, RPC server). The remaining nodes act as workers, polling the leader for tasks. When the leader dies, Raft elects a new one and the cluster resumes from replicated state — no human intervention required.

Workers execute Map, Filter, Reduce, and SelectKey stages. A separate Compacter pool handles GroupBy (sort-group compaction) and Sink (MongoDB upsert) stages. In the `node` mode each pod co-locates one compacter goroutine by default alongside its worker/coordinator role.

```
┌─────────────────────────────────────────────────────────────────────┐
│  go-flink node (3 replicas — StatefulSet in k8s)                    │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  Raft consensus layer (hashicorp/raft + BoltDB WAL)             │  │
│  │  Replicates: inFlight dispatches, phaseIdx, phaseUUIDs,        │  │
│  │              phaseDone, taskFiles, jobID, chunkStore (nil on   │  │
│  │              followers); WAL persisted to disk via BoltDB       │  │
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
- `AskForTask` — dequeues the next `TaskInfo`, proposes `CmdDispatchTask` through Raft (persists dispatch in WAL), fills `MessageReply` with `JobID`, `PhaseUUID`, `InputPhaseUUID`, `InputActionType`
- `AskForCompactTask` — reactive GroupBy dispatch (reads `reduceDone` from MongoDB using `reducePhaseUUID`) and pre-enqueued Sink tasks; GroupBy dispatch claimed atomically via MongoDB insert
- `NoticeResult` — handles `TaskSuccess` / `TaskFailed` / `TaskContinue`; on Map/Reduce success persists completion to MongoDB using `c.phaseUUIDs[req.PhaseIdx]`
- `SubmitJob` — accepts a `JobSpec`, assigns `c.jobID` and `c.phaseUUIDs[0]`, builds `DataSource`, starts streaming
- `sweepTimedOutTasks` — runs every 5 s, re-enqueues tasks silent for > 30 s (up to 3 retries)
- `transitionToNextPhase` — generates a new `phaseUUID`, proposes `CmdAdvancePhase{PhaseUUID}` through Raft, enqueues tasks for the new phase
- `watchLeadership` — on becoming leader activates coordinator role; on becoming follower starts a local worker loop pointed at the current leader's RPC address

### Raft FSM (`pipeline/raft_commands.go`)

The Raft log replicates coordinator metadata. Each log entry is a JSON-encoded `RaftCommand`:

| Command             | Effect on all nodes                                                                         |
|---------------------|---------------------------------------------------------------------------------------------|
| `CmdEnqueueTask`    | Add task to `JobStatus` queue; store nil placeholder in `chunkStore` on followers           |
| `CmdDispatchTask`   | Add full `TaskInfo` to `inFlight` with `DispatchedAt`; remove from queue on WAL replay      |
| `CmdCompleteTask`   | Remove from `inFlight`, increment `phaseDone`, evict chunk                                  |
| `CmdFailTask`       | Remove from `inFlight`, increment retries; re-enqueue or count as done                      |
| `CmdAdvancePhase`   | Increment `phaseIdx`, store new `phaseUUID`, reset `phaseDone` and `inFlight`               |

`Snapshot` / `Restore` serialize `inFlight`, counters, `taskFiles`, `jobID`, `phaseUUIDs`, and the queue contents so a new leader can resume without replaying the full log. The Raft log and stable stores are backed by BoltDB (`<dataDir>/raft.db`) — not in-memory — so WAL entries survive pod restarts.

### Worker (`pipeline/worker.go`)

Workers are stateless. Each task assignment (`MessageReply`) carries everything needed to execute it:

| Field             | Purpose                                                                                     |
|-------------------|---------------------------------------------------------------------------------------------|
| `PluginName`      | Which `.so` to load (e.g. `"wc"`)                                                           |
| `ActionType`      | `MapTask` / `FilterTask` / `ReduceTask` / `SelectKeyTask`                                   |
| `JobID`           | UUID of the current job — scopes output directory and MongoDB keys                          |
| `PhaseUUID`       | UUID of the current phase — used as filename prefix (`mr-<phaseUUID>-…`)                    |
| `InputPhaseUUID`  | UUID of the input phase — used to glob input files                                          |
| `InputActionType` | `ActionType` of the input phase — resolves the `inDir()` subfolder                         |
| `ChunkID`         | UUID of the raw chunk (for Map stage 0 only)                                                |
| `BucketID`        | Consistent-hash bucket (for Reduce stages)                                                  |
| `NReduce`         | Total bucket count (for hashing in Map stages)                                              |
| `PhaseIdx`        | Guards against stale reports after a sweeper re-dispatch                                    |
| `DispatchedAt`    | Echoed back so the coordinator can reject stale `NoticeResult` calls                        |
| `StageIdx`        | *(legacy)* Stage index — kept for backward compat; prefer `PhaseUUID` for path construction |
| `InputStageIdx`   | *(legacy)* Input stage index — kept for backward compat; prefer `InputPhaseUUID`            |

Workers connect to the coordinator via TCP (`--coordinator host:port`) in distributed mode, or via Unix socket in embedded single-node mode.

### Compacter (`pipeline/compacter.go`)

Compacters are an independent pool — the coordinator has no knowledge of them. They poll `AskForCompactTask` RPC and handle two task types:

**GroupBy** — reactive compaction. The coordinator dispatches one GroupBy task per Reduce bucket the moment that bucket reports `TaskSuccess`. Completion is tracked in MongoDB (`reduction` collection, keyed `"<jobID>:<phaseUUID>:<bucket>"`), so dispatch state survives leader failover without a ~30 s sweeper delay. Atomic dispatch is claimed via a MongoDB insert into `compaction` — two compacters can never process the same bucket. Compacters:
1. Glob all `mr-*-<bucket>` files in the `<jobID>/reduce/` subfolder
2. Parse, sort, and group by key
3. Call `plugin.Reduce(key, values)` for each group
4. Atomically write `<jobID>/groupby/mr-<phaseUUID>-<bucket>` (write to `.tmp`, then `os.Rename`)

**Sink** — pre-enqueued one-per-bucket tasks. Compacters read the final GroupBy (or Reduce) output files and upsert each key-value pair into MongoDB. On success, `MarkSinkDone` writes an audit record to the `sink_result` collection. MongoDB URI is injected via the `MONGO_URI` environment variable (K8s Secret).

Compacters hold a `*CompactedBucketStore` (field `store`) initialized at startup and connected to MongoDB. `jobID` is set lazily from the first `TaskAlloc` reply. Compacters connect to the coordinator via TCP or Unix socket and report results through `NoticeResult` RPC.

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
    │   write <jobID>/map/mr-<p0uuid>-<chunkID>-<bucket>
    │   NoticeResult(TaskSuccess) ──► MarkMapTaskDone(p0uuid, taskID) → MongoDB map_task
    │
    ▼  [all Map tasks done → transitionToNextPhase → generates p1uuid]
Priority Task Queue  (phase 1 = Reduce, NReduce tasks)
    │
    ├── Worker polls AskForTask (reply carries InputPhaseUUID=p0uuid)
    │   glob <jobID>/map/mr-p0uuid-*-<bucketID>
    │   sort + group by key
    │   run plugin.Reduce(key, values)
    │   write <jobID>/reduce/mr-<p1uuid>-<bucketID>
    │   NoticeResult(TaskSuccess) ──► MarkReduceDone(p1uuid, bucket) → MongoDB reduction
    │       │
    │       └──► Compacter polls AskForCompactTask (reactive GroupBy)
    │            reads MongoDB reduction[p1uuid, bucket] → IsReduceDone=true
    │            claims MongoDB compaction[p2uuid, bucket] atomically
    │            glob <jobID>/reduce/mr-*-<bucket>
    │            sort + group → plugin.Reduce
    │            atomic write <jobID>/groupby/mr-<p2uuid>-<bucket>
    │            NoticeResult(TaskSuccess)
    │
    ▼  [all Reduce + GroupBy tasks done]
Pre-enqueued Sink tasks (one per bucket)
    │
    └── Compacter polls AskForCompactTask (reply carries InputPhaseUUID=p2uuid, InputActionType=GroupByTask)
        read <jobID>/groupby/mr-<p2uuid>-<bucket>
        upsert each KV to MongoDB output collection
        MarkSinkDone(p3uuid, bucket) → MongoDB sink_result
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
  │              JobID, PhaseUUID,               │
  │              PluginName, NReduce,            │
  │              PhaseIdx, DispatchedAt)         │
  │                                              │
  │  ── GetChunk(ChunkID) ─────────────────────► │
  │ ◄─ ChunkReply(Content []byte) ─────────────  │  raw bytes from chunkStore
  │                                              │
  │  [PluginRegistry.Get("wc")]                  │
  │  [run plugin.Map(filename, content)]         │
  │  [write <jobID>/map/mr-<phaseUUID>-<chunkID>-<bucket>] │
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
  │              JobID, PhaseUUID,               │
  │              InputPhaseUUID, InputActionType,│
  │              PluginName)                     │
  │                                              │
  │  [glob <jobID>/map/mr-<InputPhaseUUID>-*-<BucketID>] │
  │  [sort + group by key]                       │
  │  [run plugin.Reduce(key, values)]            │
  │  [write <jobID>/reduce/mr-<PhaseUUID>-<BucketID>]   │
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
  │                                              │  IsReduceDone(reducePhaseUUID, bucket) → MongoDB
  │                                              │  ClaimCompactDispatch(groupByPhaseUUID, bucket) → MongoDB
  │ ◄─ GroupByTask(BucketID, PhaseUUID, ───────  │
  │               InputPhaseUUID,               │
  │               InputActionType=ReduceTask)   │
  │                                              │
  │  [glob <jobID>/reduce/mr-*-<BucketID>]      │
  │  [parse, sort, group by key]                 │
  │  [plugin.Reduce(key, values)]               │
  │  [atomic write <jobID>/groupby/mr-<phaseUUID>-<BucketID>] │
  │                                              │
  │  ── NoticeResult(TaskSuccess) ─────────────► │  phaseDone++ for GroupBy phase
```

### Sink phase (Compacter)

```
Compacter                                   Coordinator (leader)
  │                                              │
  │  ── AskForCompactTask ─────────────────────► │
  │                                              │  dequeue pre-enqueued Sink task
  │ ◄─ SinkTask(BucketID, PhaseUUID, ──────────  │
  │              InputPhaseUUID,                │
  │              InputActionType=GroupByTask)   │
  │                                              │
  │  [read <jobID>/groupby/mr-<InputPhaseUUID>-<BucketID>] │
  │  [upsert each KV to MongoDB]                │
  │  [MarkSinkDone → sink_result collection]    │
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

| Failure                                | Behaviour                                                                                                     |
|----------------------------------------|---------------------------------------------------------------------------------------------------------------|
| Worker crashes mid-task                | Sweeper detects silence after 30 s → re-enqueues (up to 3 retries)                                           |
| Worker returns TaskFailed              | Re-enqueued immediately via Raft log                                                                          |
| Leader crashes                         | Raft elects new leader; resumes from BoltDB WAL + snapshot (`inFlight`, queue, `phaseUUIDs`, `jobID`)        |
| GroupBy stalls on leader failover      | **Fixed** — `reduceDone` and dispatch claims are in MongoDB (keyed by `phaseUUID`); new leader reads them immediately, no sweeper wait |
| Stale report after sweeper re-dispatch | Rejected by `DispatchedAt` token mismatch in `NoticeResult`                                                   |
| Stale report from old phase            | Rejected by `PhaseIdx` guard                                                                                  |
| Task exhausts retries                  | Counted as done (partial results), pipeline continues                                                         |
| Duplicate map output                   | Workers detect existing checkpoint file (glob on `mr-<phaseUUID>-<chunkID>-*`) and skip re-execution         |
| Concurrent jobs overwriting files      | **Fixed** — each job writes to `<outputDir>/<jobID>/…`; re-runs and parallel jobs never collide              |

---

## MongoDB collections (`CompactedBucketStore`)

All documents use `_id = "<jobID>:<phaseUUID>:<taskID-or-bucket>"`, making them globally unique across jobs and re-runs.

| Collection    | Written by                     | Purpose                                                                 |
|---------------|--------------------------------|-------------------------------------------------------------------------|
| `map_task`    | Coordinator `NoticeResult`     | Map task completion — `chunkID` + `fileName` for post-failover recovery |
| `reduction`   | Coordinator `NoticeResult`     | Reduce bucket completion — drives reactive GroupBy dispatch             |
| `compaction`  | Coordinator `AskForCompactTask`| Atomic GroupBy dispatch claim — prevents double-dispatch across compacters |
| `sink_result` | Compacter `sinkErr`            | Sink bucket audit — records `records_written`, `database`, `collection` |

All methods degrade gracefully when `MONGO_URI` is unset or MongoDB is unreachable: writes are no-ops, reads return `false`/`nil`, and the coordinator falls back to in-process state for GroupBy dispatch.

---

## Observability

### Service metrics (`[SERVICE_METRIC]`)

Emitted by workers and compacters via `M(txnID, event).Set(k, v).Emit()`:

```
[SERVICE_METRIC] txn=<jobID>:<phaseUUID> map_task
  chunk_id             = c0a
  file_name            = part-0.txt
  total_latency_ms     = 250
  chunk_fetch_ms       = 45
  process_ms           = 198
  kvs_produced         = 3847
  buckets_written      = 3
  success              = true
```

Events: `map_task`, `reduce_task`, `selectkey_task`, `groupby_task`, `sink_task`.

### Application metrics (`[APP_METRIC]`)

Emitted on errors with a stack trace via `EmitError(err)`:

```
[APP_METRIC] ERROR txn=<jobID>:<phaseUUID> map_task err=... | trace=goroutine 1 [running]:...
```

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
│   ├── rpc.go                       # MessageSend/Reply (incl. JobID/PhaseUUID/InputPhaseUUID/InputActionType), KeyValue, ChunkRequest/Reply
│   ├── compacted_bucket_store.go    # MongoDB job-keying: map_task/reduction/compaction/sink_result collections; phaseUUID-scoped keys
│   ├── service_metric.go            # Structured observability: Metric/M/Set/Emit/EmitError, txnID, appLog, actionDir
│   ├── observability.go             # serviceLog, elapsed helpers
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
