# go-flink

A distributed MapReduce pipeline engine written in Go. Processing logic is compiled into `.so` plugins and loaded at runtime, letting you scale out by launching as many worker processes as needed — each one just needs the same `.so` file.

## How it works

The coordinator reads input files, splits them into 10 MB chunks, and distributes map and reduce tasks to workers over a Unix-domain RPC socket. Workers are independent processes — you can start one or a hundred on the same machine. Each loads the same plugin and registers with the coordinator automatically.

## Quick start

### 1. Build the coordinator/worker binary

```bash
go build -o go-flink .
```

### 2. Build a plugin

```bash
cd plugin
go build -buildmode=plugin -o wc.so wc.go
```

### 3. Start the coordinator

```bash
./go-flink <input-dir> wc.so -o mr-out
```

### 4. Start workers (in separate terminals or processes)

```bash
# Each worker process picks up its own PID as --id by default
./go-flink worker --plugin wc.so
./go-flink worker --plugin wc.so
./go-flink worker --plugin wc.so
```

Spin up as many workers as you need. The coordinator hands out tasks as fast as workers ask for them. When all tasks are done the coordinator sends a shutdown signal and all workers exit cleanly.

## Scaling

Throughput scales with the number of worker processes. Workers are stateless — they fetch chunk content from the coordinator via RPC (`GetChunk`), run the plugin's `Map` or `Reduce` function locally, and write intermediate files to the shared output directory. To scale:

- Add more `./go-flink worker --plugin <your>.so` processes.
- Point them all at the same output directory (`-o`).
- No other configuration is needed.

There is no upper limit enforced by the framework. The coordinator's priority queue and timeout sweeper handle stragglers and crashed workers automatically (up to 3 retries per task).

## Writing a plugin

A plugin is a regular Go file compiled with `-buildmode=plugin`. It must export exactly two functions:

```go
func Map(filename string, contents string) []pipeline.KeyValue
func Reduce(key string, values []string) string
```

See [plugin/wc.go](plugin/wc.go) for a complete word-count example.

Build it:
```bash
go build -buildmode=plugin -o myplugin.so myplugin.go
```

## CLI reference

```
go-flink <input-dir> <plugin.so> [-o output-dir]
    Start the coordinator and begin streaming input files.

go-flink worker --plugin <plugin.so> [--id <int>] [-o output-dir]
    Start a worker process. --id defaults to the process PID.
    Run this command in parallel across as many processes as desired.
```

## Output

Intermediate files are written as `mr-<chunkID>-<bucket>` and final results as `mr-out-<chunkID>` inside the output directory (default: `mr-out/`).

## Dependencies

- [cobra](https://github.com/spf13/cobra) — CLI
- [gods](https://github.com/emirpasic/gods) — priority queue for task scheduling
- [hashring](https://github.com/serialx/hashring) — consistent hashing for reduce partitioning
- [uuid](https://github.com/google/uuid) — chunk identity
