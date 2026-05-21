package pipeline

/**

func main() {
	// Create the list of functions
	funcList := []StreamProcessAction{
		// Function 1: Expects (int, int), returns (sum int, product int)
		func(args ...any) []any {
			if len(args) < 2 {
				return []any{0, 0}
			}
			a, ok1 := args[0].(int)
			b, ok2 := args[1].(int)
			if !ok1 || !ok2 {
				return []any{0, 0}
			}
			return []any{a + b, a * b}
		},

		// Function 2: Expects (string, int), returns (repeated string)
		func(args ...any) []any {
			if len(args) < 2 {
				return []any{""}
			}
			str, ok1 := args[0].(string)
			count, ok2 := args[1].(int)
			if !ok1 || !ok2 {
				return []any{""}
			}
			result := ""
			for i := 0; i < count; i++ {
				result += str
			}
			return []any{result}
		},
	}

	// Execute the math function
	mathResults := funcList[0](10, 5)
	fmt.Printf("Math Outputs: Sum = %v, Product = %v\n", mathResults[0], mathResults[1])

	// Execute the string function
	strResults := funcList[1]("Go", 3)
	fmt.Printf("String Output: %v\n", strResults[0])
}
**/

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	SelectKeyTask
	FilterTask
	SinkTask
)

type StreamProcessAction struct {
	Action     func(args ...any) any
	ActionType TaskType
}

// apache flink similar API to process streaming data,
// we can implement a simple version of it for our log processing system
type StreamProcess interface {
	// Filter(validateFunc StreamProcessAction) StreamProcess
	Map(mapFunc StreamProcessAction) []KeyValue // map is group the values into key upto nreduce
	Reduce(reduceFunc StreamProcessAction) any  // reduce is run by pick one key but return one of the key
	SelectKey(groupFunc StreamProcessAction) any
	Sink(sinkFunc StreamProcessAction) error // a flusking function where worker flush this change to an secondary source
}

type StreamListener interface {
	Listen(source <-chan string)
	ListenRawBytes(source <-chan []byte)

	// implementing more interface IE s3 or more
}

// ds = DataPipeline(SOURCE, WINDOW_SIZE, PARTITION_FUNC)
// ds = pipeline.Filter()
// So this define what the worker have to do each stage, and coordinater assign them to it
// This is a high level API
// And from there we proceed to lower level API, which is more close to the worker implementation,
// and coordinater will assign them to it
// Pipeline -> Coordinator -> Worker
//  Message received upon channel
// > ds.Listen(SOME_URL_SOURCE)
