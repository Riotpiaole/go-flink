package pipeline

import (
	"log"
	"time"
)

// serviceLog emits an aggregatable KPI metric.
// Format: [SERVICE_METRIC] key=value unit
func serviceLog(key string, value interface{}, unit string) {
	log.Printf("[SERVICE_METRIC] %s=%v %s", key, value, unit)
}


// elapsed returns a func that, when called, emits serviceLog with the duration
// in milliseconds since elapsed was invoked. Use with defer:
//
//	defer elapsed("worker.map.latency")()
func elapsed(key string) func() {
	start := time.Now()
	return func() {
		serviceLog(key, time.Since(start).Milliseconds(), "ms")
	}
}
