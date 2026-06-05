# go-flink Change Log

---

[2026-06-05 00:00] | Implement job UUID scoping, phaseUUID file paths, MongoDB job-keying, and structured observability
VERIFIED: go build ./pipeline/... && go build .
METRICS:
  serviceMetric: map_task, reduce_task, selectkey_task, groupby_task, sink_task
  applicationMetric: map/reduce/selectkey/groupby/sink errors surfaced via EmitError with stack trace

[2026-06-05 00:01] | Wire embedded-mode jobID/phaseUUID; add MarkSinkDone in compacter; add sink_task error metric
VERIFIED: go build ./pipeline/... && go build .
METRICS:
  serviceMetric: sink_task now emits on error (EmitError) in addition to success
  applicationMetric: [APP_METRIC] ERROR emitted for SinkTask failures with stack trace
