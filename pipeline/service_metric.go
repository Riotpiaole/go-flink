package pipeline

import (
	"fmt"
	"log"
	"runtime/debug"
	"strings"
)

// Metric is a structured [SERVICE_METRIC] log block, built with a fluent API.
type Metric struct {
	txn    string
	event  string
	fields []string // alternating key, stringified-value pairs
}

// M creates a new Metric scoped to the given transaction ID and event name.
func M(txn, event string) *Metric {
	return &Metric{txn: txn, event: event}
}

// Set adds a key=value pair to the metric. Returns m for chaining.
func (m *Metric) Set(key string, value interface{}) *Metric {
	m.fields = append(m.fields, key, fmt.Sprintf("%v", value))
	return m
}

// Emit writes a [SERVICE_METRIC] block to the logger.
//
// Output format:
//
//	[SERVICE_METRIC] txn=<txn> <event>
//	  <key>  = <value>
//	  ...
func (m *Metric) Emit() {
	var sb strings.Builder
	fmt.Fprintf(&sb, "[SERVICE_METRIC] txn=%s %s\n", m.txn, m.event)
	for i := 0; i+1 < len(m.fields); i += 2 {
		fmt.Fprintf(&sb, "  %-20s = %s\n", m.fields[i], m.fields[i+1])
	}
	log.Print(sb.String())
}

// EmitError writes a [APP_METRIC] ERROR line with a stack trace.
func (m *Metric) EmitError(err error) {
	log.Printf("[APP_METRIC] ERROR txn=%s %s err=%v | trace=%s",
		m.txn, m.event, err, debug.Stack())
}

// txnID returns "<jobID>:<phaseUUID>" from a MessageReply for use as the txn
// field in Metric and appLog. Falls back to "local" in embedded single-node mode.
func txnID(reply *MessageReply) string {
	if reply.JobID == "" {
		return "local"
	}
	return reply.JobID + ":" + reply.PhaseUUID
}

// appLog emits a standalone [APP_METRIC] line without a stack trace.
// Use EmitError for error-level events that should include a trace.
func appLog(txn, severity, msg string) {
	log.Printf("[APP_METRIC] %s txn=%s %s", severity, txn, msg)
}

// actionDir maps a TaskType to the output-directory subfolder name used in
// job-scoped file paths: <outputDir>/<jobID>/<actionDir>/mr-<phaseUUID>-...
func actionDir(t TaskType) string {
	switch t {
	case MapTask:
		return "map"
	case FilterTask:
		return "filter"
	case ReduceTask:
		return "reduce"
	case GroupByTask:
		return "groupby"
	case SelectKeyTask:
		return "selectkey"
	case SinkTask:
		return "sink"
	default:
		return "unknown"
	}
}
