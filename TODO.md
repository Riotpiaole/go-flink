# Distributed Datalakehouse — Implementation Checklist

---

## Layer 1 — Ingestion (Streaming MapReduce)

### Core infrastructure
- [x] `ChunkQueue` — thread-safe FIFO (Push/Pop/Close/Done)
- [x] `FilesDataSource` — directory walk with 10 MB chunk boundaries, async producer
- [x] `ChunkRequest/ChunkReply` RPC — on-demand chunk bytes served by coordinator

### Coordinator
- [x] Unix-socket RPC server
- [x] Priority task queue (emirpasic/gods)
- [x] Phase management (Map → Reduce → Done)
- [x] Fault-tolerance sweeper (30s timeout, 3 retries, PhaseIdx stale-report guard)
- [x] `AskForTask` / `NoticeResult` RPC handlers
- [ ] Ring-based routing in coordinator (currently simple bucket hash in worker)
- [ ] Kafka partition source (currently file-only)
- [ ] gRPC transport — replace Unix socket so workers can run on separate hosts
  - [ ] Define `coordinator.proto` with `AskForTask`, `GetChunk`, `NoticeResult`, `Shutdown` RPCs
  - [ ] Replace `net/rpc` server with `grpc.Server`
  - [ ] Replace worker client calls with generated gRPC stubs
  - [ ] Add mutual TLS option for multi-host deployments
  - [ ] Update CLI: replace socket path flag with `--coordinator-addr host:port`

### Worker
- [x] Continuous poll loop with 200ms backoff
- [x] Map execution: fetch chunk → plugin Map → consistent-hash partition → write `mr-<chunkID>-<bucket>` files
- [x] Reduce execution: glob intermediate files → sort → plugin Reduce → write `mr-out-<chunkID>`
- [x] Checkpoint idempotency (skip if output file exists)
- [ ] `SelectKey` task execution (currently panics "unimplemented")
  - [ ] Dispatch `SelectKeyTask` in `Coordinator.transitionToNextPhase` — one task per map output chunk
  - [ ] `Worker.SelectKey`: read `mr-<chunkID>-*`, apply `(key, value) → newKey`, write `mr-sk-<chunkID>-<bucket>`
  - [ ] Wire consistent-hash bucketing via `buildRing` for re-keyed records
  - [ ] Add `Pipeline.SelectKey(fn)` builder method
- [ ] `Filter` task execution (defined in constants, no worker logic)
  - [ ] Dispatch `FilterTask` in `Coordinator.transitionToNextPhase` — one task per chunk
  - [ ] `Worker.Filter`: read intermediates, apply `filterFunc(key, value) bool`, write passing records to `mr-f-<chunkID>-<bucket>`
  - [ ] Add checkpoint check (same `filepath.Glob` pattern as `mapErr`/`reduceErr`)
  - [ ] Add `Pipeline.Filter(fn)` builder method
- [ ] `GroupBy` task execution (defined in constants, no worker logic)
- [ ] `Sink` task execution (currently panics "unimplemented")
  - [ ] Define sink action signature: `func(key, value string) error`
  - [ ] `Worker.Sink` / `Worker.sinkErr`: read `mr-out-<chunkID>`, call `sinkFunc` per record
  - [ ] Dispatch `SinkTask` in `Coordinator.transitionToNextPhase` — one task per reduce output
  - [ ] Make `Pipeline.Sink(fn)` functional (remove panic)
  - [ ] Built-in `FileSink` that consolidates all `mr-out-*` into a single file

### Plugin / streaming source
- [x] `.so` dynamic plugin loader with `Map`/`Reduce` signature validation
- [ ] `Listen` / `ListenRawBytes` streaming source entry points (currently panic)
- [ ] `KafkaDataSource` — consume from Kafka topic via `segmentio/kafka-go`; each message → one `FileChunk`
- [ ] `S3DataSource` — stream objects from S3-compatible store (`aws-sdk-go-v2`); supports prefix filtering
- [ ] `ImageDataSource` — walk directory for PNG/JPEG/TIFF/WebP; embed raw bytes + attach MIME type metadata
- [ ] `VideoDataSource` — extract keyframes and audio via `ffmpeg` subprocess; emit as separate chunks
- [ ] `GRPCStreamDataSource` — accept push from an external gRPC stream (webhook-style ingestion)

### Format normalizer (new phase before Map)
- [ ] Add `NormalizeTask` phase
- [ ] `Worker.Normalize`: detect chunk MIME type → dispatch to normalizer
  - Text/CSV/JSON → pass through
  - Image → call embedding sidecar (CLIP/BLIP2 via gRPC) → `{key: filename, value: base64_embedding}`
  - Audio → call transcription sidecar (Whisper via gRPC) → `{key: filename, value: transcript}`
  - Binary → extract metadata only → `{key: filename, value: json_metadata}`
- [ ] Define proto contracts for embedding and transcription sidecars
- [ ] Add `Pipeline.Normalize()` builder method

### Intermediate format
- [ ] Replace JSON `KeyValue` intermediate files with Apache Arrow IPC record batches (`apache/arrow/go/v17`)
  - [ ] `Worker.mapErr`: Arrow IPC `FileWriter` → `mr-<chunkID>-<bucket>.arrow`
  - [ ] `Worker.reduceErr`: Arrow IPC `FileReader`
  - [ ] Update all glob patterns from `mr-<chunkID>-*` to `mr-<chunkID>-*.arrow`
  - [ ] Benchmark Map→Reduce intermediate I/O throughput; target ≥5× improvement over JSON

---

## Layer 2 — Write Path & Storage

### Hot store (sub-millisecond reads)
- [ ] Define `HotStore` interface: `Put(chunkID string, batch arrow.Record)`, `Get`, `Evict`
- [ ] `RedisHotStore` backed by DragonflyDB (`go-redis/redis/v9`): serialize Arrow batches via IPC at key `hot:<chunkID>`
- [ ] Wire coordinator `chunkStore` to write-through to `HotStore` on ingest
- [ ] TTL-based eviction: chunks older than configurable threshold promoted to cold store
- [ ] Benchmark round-trip latency; target <1ms for a 1 MB Arrow batch read

### WAL & MemTable
- [ ] WAL — append-only, durable before any MemTable write
- [ ] MemTable — columnar in-memory accumulator per Reduce worker
- [ ] Flush trigger — size threshold + 30s timer

### Cold store (Parquet / Iceberg)
- [ ] Parquet writer — flush MemTable → Parquet file on S3 (`parquet-go`)
- [ ] `IcebergSink` built-in plugin: write Parquet files, call Iceberg REST catalog to register new snapshot
- [ ] Partition spec: partition by `ingestion_date` (daily) and `source_type` (file/image/stream)
- [ ] Catalog notification on flush
- [ ] Time-travel: `--as-of <timestamp>` CLI flag resolves to correct Iceberg snapshot
- [ ] Hot→cold tiering: background goroutine promotes evicted hot-store chunks to Iceberg

### Compaction
- [ ] Compaction coordinator — watch catalog, dispatch when small-file count exceeds threshold
- [ ] Compaction worker — read N small files → merge + sort → write 1 large file → update catalog

### Consensus
- [ ] Coordinator Raft consensus (3-node leader election, WAL-based follower catch-up)

---

## Layer 3 — Table Catalog

- [ ] Snapshot tree — immutable linked list of table versions (Iceberg-style)
- [ ] File list per snapshot
- [ ] Partition statistics — min/max/null counts per column per file
- [ ] Schema registry — Avro / Protobuf / Arrow schema versions with compatibility check
  - [ ] `SchemaEntry`: table name, field list (name + Arrow type + nullable), version, timestamps
  - [ ] Backed by `BadgerDB`; methods: `Register`, `GetLatest`, `GetVersion`, `Diff`
  - [ ] Block breaking changes (field removal, type narrowing) unless `--force`
  - [ ] First-class multimodal field types: `EmbeddingVector(dim int)`, `ImageRef`, `AudioRef`
  - [ ] Iceberg REST catalog endpoints (`/v1/namespaces`, `/v1/namespaces/{ns}/tables`)
- [ ] Committed watermark — latest reducer offset visible to queries
- [ ] Column statistics store
  - [ ] Compute per flush: min, max, null count, NDV (HyperLogLog), histogram (equi-depth, 100 buckets)
  - [ ] Store in `BadgerDB` at key `stats:<table>:<partition>:<column>`
  - [ ] Global rollup: merge per-partition stats into table-level stats on snapshot commit
- [ ] B-tree index over partition keys for O(log N) pruning
- [ ] Snapshot pinning for concurrent query isolation
- [ ] Semantic embedding index
  - [ ] Embed table description + column names on schema registration (BGE-small via ONNX or remote API)
  - [ ] HNSW ANN index (`hnswlib` via CGo or `usearch`); persist to disk, reload on startup
  - [ ] `CatalogSearch(query string, topK int) []SchemaEntry`: embed query → ANN lookup → ranked tables
- [ ] Data lineage graph
  - [ ] `LineageNode` (source / transform / table) and `LineageEdge` stored in `BadgerDB` (OpenLineage spec)
  - [ ] Record lineage on every pipeline run
  - [ ] API: `GET /v1/lineage/{tableID}/upstream` and `/downstream`
- [ ] Catalog API (gRPC + REST gateway): schema CRUD, stats lookup, semantic search, lineage query
- [ ] Hive Metastore compatibility shim (Thrift) for Spark / Trino

---

## Layer 4 — Distributed Query Engine

- [ ] SQL parser → logical plan
- [ ] Query optimizer → physical DAG (Scan, Filter, HashJoin, Agg, Exchange nodes)
- [ ] Embed DuckDB in-process via `marcboeker/go-duckdb`; register Arrow batches as virtual tables
- [ ] `HotQuery(sql string) (arrow.Record, error)`: SQL over hot-store Arrow batches; target <2ms for 10M-row scan
- [ ] `ColdQuery(sql string, snapshot string) (arrow.Record, error)`: Iceberg Parquet scan via DuckDB `iceberg_scan`
- [ ] `VectorQuery(embedding []float32, topK int) ([]string, error)`: HNSW ANN search over embedding index
- [ ] Fragment scheduler — split DAG by partition, route to worker pool via consistent hash
- [ ] Scan worker — catalog lookup → S3 GET with column + row pruning → Arrow record batches
- [ ] Predicate pushdown — WHERE filters pushed to Parquet reader (row-group min/max skip)
- [ ] Column projection — only requested columns read from S3
- [ ] Parallel scans — one goroutine per partition
- [ ] Hybrid query planner: scalar predicates + vector predicates executed in parallel, results merged via RRF
- [ ] Distributed hash join — `[]map[uint64][]Row` with per-shard mutex
- [ ] Grace hash join — partition both sides to disk for joins exceeding memory
- [ ] Apache Arrow Flight SQL endpoint (`arrowflight/flightsql`) for external clients

---

## Layer 5 — Serving Layer

- [ ] Load balancer — round-robin across stateless gateway pods
- [ ] SQL gateway — parse SQL, session management, per-client rate limiting
- [ ] RBAC engine
  - [ ] Table-level access (query rejected at gateway)
  - [ ] Column masking (projection rewrite injected into plan)
  - [ ] Row-level filter injection (`WHERE region = 'US'` style)
- [ ] LRU result cache — keyed on SQL + snapshot ID + user role; invalidate on new snapshot
- [ ] Arrow IPC zero-copy streaming to client
- [ ] `X-Data-Freshness` watermark header on responses
- [ ] LLM RAG query layer
  - [ ] `NLPlanner`: NL question → `CatalogSearch` → inject schema + stats into LLM prompt → SQL plan + optional ANN plan (`tmc/langchaingo`)
  - [ ] `RAGAgent` loop: NLPlanner → `HotQuery`/`VectorQuery` → RRF fusion → cross-encoder re-rank → LLM grounding pass → streaming answer
  - [ ] Multi-hop retrieval (max 3 hops) when first query returns sparse results
  - [ ] Grounding check: verify LLM answer cites only values present in retrieved rows
  - [ ] `POST /v1/query` REST endpoint with SSE streaming response
  - [ ] Prompt cache: cache catalog context embeddings to skip re-embedding on repeated table queries

---

## Cross-cutting Concerns

- [ ] Schema evolution — add/remove columns without rewriting historical Parquet
- [ ] Time travel — query table as of a past snapshot ID
- [ ] Exactly-once semantics — WAL offsets preventing duplicate / lost records
- [ ] Backpressure — ChunkQueue depth signals Map workers to throttle ingestion rate
- [ ] Monitoring — per-layer metrics
  - [ ] Ingest lag
  - [ ] Flush latency
  - [ ] Compaction backlog
  - [ ] Cache hit rate
  - [ ] Query fragment execution time
- [ ] Structured logging (`slog`) throughout; replace all `fmt.Printf` calls
- [ ] Prometheus metrics: tasks dispatched/completed/failed per phase, query latency histograms, hot-store hit/miss rate
- [ ] Distributed tracing (OpenTelemetry) across coordinator → worker → sink → catalog
- [ ] Health check: `GET /healthz` returning coordinator state (phases, tasks in flight, workers connected)
- [ ] Graceful shutdown: drain in-flight tasks; persist coordinator state to disk for restart recovery
- [ ] Integration tests: end-to-end pipeline run (ingest → map → reduce → sink → query) against `datasets/`
- [ ] Benchmark suite: throughput (MB/s ingest), latency (ms per query), scalability (workers 1→16)