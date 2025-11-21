package engine

import (
	"expvar"
	"os"
	"strings"
	"testing"
)

// resetEngineExpvars clears or resets published expvar variables that belong
// to the engine namespace so tests start with a clean metrics state.
func resetEngineExpvars() {
	var keys []string
	expvar.Do(func(kv expvar.KeyValue) {
		keys = append(keys, kv.Key)
	})
	for _, name := range keys {
		// Only touch engine-scoped variables to avoid interfering with other packages.
		if strings.HasPrefix(name, "engine_") {
			v := expvar.Get(name)
			switch vv := v.(type) {
			case *expvar.Int:
				vv.Set(0)
			case *expvar.Float:
				vv.Set(0)
			case *expvar.Map:
				vv.Init()
			}
		}
	}
}

// TestMain resets engine expvar state before running package tests to avoid
// flakiness caused by leftover global metrics from other tests.
func TestMain(m *testing.M) {
	resetEngineExpvars()
	os.Exit(m.Run())
}
