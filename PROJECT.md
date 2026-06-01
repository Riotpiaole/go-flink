# go-flink — Project State

Distributed stream-processing engine (Go) with MapReduce-over-plugins, Raft consensus, and a roadmap toward a full datalakehouse.

---

## Layer Status

| Layer                        | Status | Note                                            |
|------------------------------|--------|-------------------------------------------------|
| 1 — Ingestion / MapReduce    | ~55%   | Core done; operators + Kafka source outstanding |
| 2 — Write Path & Storage     | 0%     | Not started                                     |
| 3 — Table Catalog            | 0%     | Not started                                     |
| 4 — Distributed Query Engine | 0%     | Not started                                     |
| 5 — Serving Layer            | 0%     | Not started                                     |

---

## Done

**Layer 1 — Core pipeline**
- `ChunkQueue` (thread-safe FIFO), `FilesDataSource` (10 MB chunked directory walk)
- `ChunkRequest/ChunkReply` RPC — coordinator serves chunk bytes on demand
- Coordinator: Unix-socket RPC, priority task queue (gods), Map→Reduce→Done phases, fault-tolerance sweeper (30s / 3 retries)
- Worker: continuous poll loop, Map execution, Reduce execution, checkpoint idempotency
- `.so` dynamic plugin loader with Map/Reduce signature validation

**Infrastructure**
- Raft consensus via `hashicorp/raft` — leader election, K8s StatefulSet, Raft command log
- K8s manifests: coordinator StatefulSet, worker Deployment, PVCs, headless service, RPC service
- Kafka 4.0 KRaft (no ZooKeeper): StatefulSet + kafka-headless (clusterIP: None) + kafka ClusterIP services
- Makefile targets: `dev`, `build`, `deploy`, `undeploy`, `submit`, `logs`, `clean`

---

## In Progress

- **Kafka DNS fix** (uncommitted) — `k8s/kafka.yaml` now includes `kafka-headless` and `kafka` services; fixes `UnknownHostException: kafka-0.kafka-headless` in KRaft quorum voters

---

## TODO.md Drift

`TODO.md` marks Raft consensus as `[ ]` (Layer 2) but it is implemented. Should be updated to `[x]`.

---

## Next Priorities

1. **`Sink` operator** — unblocks end-to-end pipeline completion; currently panics "unimplemented"
2. **`KafkaDataSource`** — `segmentio/kafka-go`, each message → one `FileChunk`; enables streaming ingestion once Kafka K8s is stable
3. **gRPC transport** — replace `net/rpc` + Unix socket so workers can run cross-host; requires `coordinator.proto` + generated stubs
