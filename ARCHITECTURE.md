# Architecture

## Overview

go-flink is a MapReduce engine where the coordinator and workers are separate OS processes that communicate over a Unix-domain RPC socket. Processing logic lives in `.so` plugins loaded at startup — swapping algorithms requires only a different `.so`, no recompilation of the engine itself.

```
┌─────────────────────────────────────────────────────────────┐
│                        Coordinator                          │
│                                                             │
│  FilesDataSource ──► ChunkQueue ──► Priority Task Queue     │
│                                           │                 │
│                            ┌──────────────┘                 │
│                            │  Unix RPC socket               │
│                            │  /var/tmp/5840-mr-<uid>        │
└────────────────────────────┼────────────────────────────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
    ┌─────▼──────┐    ┌──────▼─────┐    ┌──────▼─────┐
    │  Worker 0  │    │  Worker 1  │    │  Worker N  │
    │  (PID 123) │    │  (PID 456) │    │  (PID 789) │
    │            │    │            │    │            │
    │  wc.so     │    │  wc.so     │    │  wc.so     │
    │  Map()     │    │  Map()     │    │  Map()     │
    │  Reduce()  │    │  Reduce()  │    │  Reduce()  │
    └────────────┘    └────────────┘    └────────────┘
          │                  │                  │
          └──────────────────┼──────────────────┘
                             │
                     ┌───────▼────────┐
                     │  mr-out/ dir   │
                     │  (shared disk) │
                     └────────────────┘
```

## Components

### DataSource (`pipeline/datasource/datasource.go`)

`FilesDataSource` walks the input directory and reads each file in 10 MB chunks. Chunks are pushed into a `ChunkQueue` — a mutex-protected FIFO. The producer goroutine calls `Close()` when done; consumers poll `Done()` to know when all chunks have been delivered.

### Coordinator (`pipeline/coordinator.go`)

The coordinator is the single point of control. It:

1. Registers itself as an RPC server on a Unix socket.
2. Consumes the `ChunkQueue` in a background goroutine, assigning each chunk a UUID (`ChunkID`) and enqueuing a `TaskInfo` into a priority queue.
3. Stores raw chunk bytes in `chunkStore` keyed by `ChunkID` so workers can fetch them on demand.
4. Responds to two RPC calls from workers:
   - `AskForTask` — dequeues the next task and returns its metadata.
   - `NoticeResult` — handles `TaskSuccess`, `TaskFailed`, and `TaskContinue` reports.
5. Runs a sweeper goroutine every 5 seconds that re-enqueues tasks whose workers have gone silent for more than 30 seconds (up to 3 retries before a task is abandoned).
6. Advances through phases (Map → Reduce → …) once all tasks in the current phase complete.

### Worker (`pipeline/worker.go`)

Each worker is an independent OS process. Workers:

1. Load a `.so` plugin via `LoadPlugin` and register its `Map`/`Reduce` functions.
2. Loop: call `AskForTask` → execute the assigned action → report `TaskSuccess` or `TaskFailed`.
3. For **Map**: fetch chunk bytes from the coordinator via `GetChunk` RPC, call the plugin's `Map` function, write `[]KeyValue` as JSON to `mr-<chunkID>-<bucket>`.
4. For **Reduce**: glob all `mr-<chunkID>-*` intermediate files, sort by key, group, call the plugin's `Reduce` function, write results to `mr-out-<chunkID>`.
5. Exit on `Shutdown` reply.

Worker ID defaults to `os.Getpid()`, making each process uniquely identifiable in logs without manual coordination.

### Plugin system (`pipeline/loadplugin.go`)

`LoadPlugin` opens a `.so` with `plugin.Open`, looks up the `Map` and `Reduce` symbols, validates their signatures, and wraps them into the variadic `func(args ...any) any` interface the pipeline engine uses internally. This decoupling means the engine never needs to be rebuilt to change the processing algorithm.

### RPC types (`pipeline/rpc.go`)

All coordinator↔worker communication is typed via `MessageSend` / `MessageReply`. Key fields:

| Field | Direction | Purpose |
|---|---|---|
| `ChunkID` | coord → worker | UUID identifying the chunk in `chunkStore` |
| `ActionIndex` | coord → worker | Which phase action to execute |
| `PhaseIdx` | both | Guards against stale reports from a previous phase |
| `NextOffset` | worker → coord | Used with `TaskContinue` when a file spans multiple chunks |
| `BucketID` | coord → worker | Consistent-hash bucket for reduce partitioning |

## Data flow

```
Input files
    │
    ▼
FilesDataSource.StreamChunks()
    │  10 MB FileChunk per read
    ▼
ChunkQueue (thread-safe FIFO)
    │
    ▼
Coordinator.listenFromDataSource()
    │  assigns UUID ChunkID, stores bytes in chunkStore
    ▼
Priority Task Queue (phase 0 = Map tasks)
    │
    ├── Worker polls AskForTask ──► fetches chunk bytes via GetChunk RPC
    │                               runs plugin.Map(filename, contents)
    │                               writes mr-<chunkID>-<bucket> JSON files
    │                               reports TaskSuccess
    │
    ▼
Phase transition (all map tasks done)
    │
    ▼
Priority Task Queue (phase 1 = Reduce tasks, one per ChunkID)
    │
    ├── Worker polls AskForTask ──► globs mr-<chunkID>-* intermediate files
    │                               sorts + groups by key
    │                               runs plugin.Reduce(key, values)
    │                               writes mr-out-<chunkID>
    │                               reports TaskSuccess
    │
    ▼
All phases complete → Coordinator.Done() = true → workers receive Shutdown
```

## Scaling the MapReduce

Workers are completely stateless between tasks. To increase throughput:

```bash
# Start N workers against the same coordinator and output dir
for i in $(seq 1 8); do
  ./go-flink worker --plugin wc.so -o mr-out &
done
```

Each additional worker process adds parallel capacity for both map and reduce phases. The coordinator's task queue naturally load-balances work across however many workers are connected — slow or failed workers have their tasks reclaimed by the sweeper and redistributed.

## Fault tolerance

| Failure | Behaviour |
|---|---|
| Worker crashes mid-task | Sweeper detects no heartbeat after 30 s, re-enqueues task |
| Worker returns TaskFailed | Re-enqueued immediately, up to 3 retries |
| Task exhausts retries | Counted as done (partial results), pipeline continues |
| Stale phase report | Ignored via `PhaseIdx` guard in `NoticeResult` |
| Duplicate map output | Workers check for existing checkpoint files and skip re-execution |

## File layout

```
.
├── main.go                          # CLI (coordinator run + worker subcommand)
├── plugin/
│   └── wc.go                        # Example word-count plugin
└── pipeline/
    ├── pipeline.go                  # Pipeline builder (Map/Reduce/Sink chaining)
    ├── coordinator.go               # Task scheduler, RPC server, phase management
    ├── worker.go                    # Worker loop, Map/Reduce execution
    ├── loadplugin.go                # .so plugin loader
    ├── rpc.go                       # RPC message types, KeyValue, IntermediateStore
    └── datasource/
        └── datasource.go            # FilesDataSource, ChunkQueue
```
