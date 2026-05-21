package pipeline

import (
	"fmt"
	"plugin"
)

type PluginFuncs struct {
	Map    func(args ...any) any
	Reduce func(args ...any) any
}

// LoadPlugin opens a compiled .so plugin and returns wrapped Map and Reduce
// functions ready to pass to Pipeline.Map / Pipeline.Reduce.
//
// The plugin must export:
//
//	func Map(filename string, contents string) []pipeline.KeyValue
//	func Reduce(key string, values []string) string
func LoadPlugin(soPath string) (*PluginFuncs, error) {
	p, err := plugin.Open(soPath)
	if err != nil {
		return nil, fmt.Errorf("open plugin %s: %w", soPath, err)
	}

	mapSym, err := p.Lookup("Map")
	if err != nil {
		return nil, fmt.Errorf("plugin missing Map symbol: %w", err)
	}
	mapFn, ok := mapSym.(func(string, string) []KeyValue)
	if !ok {
		return nil, fmt.Errorf("Map has wrong signature: got %T", mapSym)
	}

	reduceSym, err := p.Lookup("Reduce")
	if err != nil {
		return nil, fmt.Errorf("plugin missing Reduce symbol: %w", err)
	}
	reduceFn, ok := reduceSym.(func(string, []string) string)
	if !ok {
		return nil, fmt.Errorf("Reduce has wrong signature: got %T", reduceSym)
	}

	return &PluginFuncs{
		Map: func(args ...any) any {
			filename, _ := args[0].(string)
			contents, _ := args[1].(string)
			return mapFn(filename, contents)
		},
		Reduce: func(args ...any) any {
			key, _ := args[0].(Key)
			values, _ := args[1].([]string)
			return reduceFn(string(key), values)
		},
	}, nil
}
