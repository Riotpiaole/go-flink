# TODO

## SelectKey

`SelectKey` re-keys the intermediate stream by extracting a new key from each `KeyValue` produced by the Map phase. Similar to Flink's `keyBy` — it determines how records are grouped before the Reduce phase.

- [ ] Define `SelectKeyTask` dispatch in `Coordinator.transitionToNextPhase` — enqueue one task per map output chunk (same pattern as ReduceTask)
- [ ] Implement `Worker.SelectKey` in `worker.go`: read `mr-<chunkID>-*` intermediate files, apply the selector function `(key, value) → newKey`, write re-keyed records to `mr-sk-<chunkID>-<bucket>`
- [ ] Wire up consistent-hash bucketing using `buildRing` so re-keyed records are routed to the right reduce bucket
- [ ] Add `Pipeline.SelectKey(fn)` builder method in `pipeline.go` (same chaining pattern as `Map`/`Reduce`)
- [ ] Uncomment and implement `Filter` comment stub in `interface.go` after SelectKey is proven — Filter can share the same phase infrastructure
- [ ] Update `ARCHITECTURE.md` data-flow diagram to show the SelectKey phase between Map and Reduce

---

## Filter

`Filter` drops records that don't satisfy a predicate before they reach the Reduce phase. Can be applied after Map or after SelectKey.

- [ ] Add `FilterTask` dispatch in `Coordinator.transitionToNextPhase` — one task per map/selectkey output chunk
- [ ] Implement `Worker.Filter` in `worker.go`: read intermediate files for the chunk, call `filterFunc(key, value) bool`, write passing records to `mr-f-<chunkID>-<bucket>`, discard the rest
- [ ] Decide whether Filter emits a new intermediate file set or mutates in-place (new file set is safer for retries and checkpointing)
- [ ] Add `Pipeline.Filter(fn)` builder method in `pipeline.go`
- [ ] Ensure the coordinator's `currentPhaseComplete` logic covers FilterTask the same way it covers ReduceTask (non-Map phases wait for `phaseDone == NumTasks`)
- [ ] Add checkpoint check in `Worker.Filter` (same `filepath.Glob` pattern used in `mapErr` and `reduceErr`) so retried filter tasks are skipped

---

## SinkTask

`Sink` is the terminal phase — workers flush final `KeyValue` results to a secondary destination (file, database, object store, etc.). Currently `Worker.Sink` and `Pipeline.Sink` are stubs that panic.

- [ ] Define the `Sink` action signature: `func(key string, value string) error` — simpler than Map/Reduce since there is no output to shuffle
- [ ] Implement `Worker.Sink` / `Worker.sinkErr` in `worker.go`: read `mr-out-<chunkID>` result files, call `sinkFunc` per record, report `TaskSuccess` or `TaskFailed`
- [ ] Add `SinkTask` dispatch in `Coordinator.transitionToNextPhase` — one task per reduce output file
- [ ] Make `Pipeline.Sink(fn)` builder method functional in `pipeline.go` (remove the panic, append a `SinkTask` action)
- [ ] Decide on at-least-once vs exactly-once delivery guarantee for the sink; at minimum add idempotency documentation to `interface.go`
- [ ] Add a built-in `FileSink` implementation that consolidates all `mr-out-*` files into a single output file for convenience
- [ ] Update `ARCHITECTURE.md` to reflect the full Map → [SelectKey] → [Filter] → Reduce → Sink pipeline
