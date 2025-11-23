package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
)

func main() {
	var dir string
	flag.StringVar(&dir, "dir", "", "data directory to load series mappings from")
	flag.Parse()
	if dir == "" {
		log.Fatal("Please provide -dir path")
	}
	s := indexer.NewSeriesIDStore(nil, hooks.NewHookManager(nil))
	if err := s.LoadFromFile(dir); err != nil {
		log.Fatalf("LoadFromFile failed: %v", err)
	}
	ss := indexer.NewStringStore(nil, hooks.NewHookManager(nil))
	if err := ss.LoadFromFile(dir); err != nil {
		log.Fatalf("StringStore LoadFromFile failed: %v", err)
	}
	// Print in id order
	for id := uint64(1); ; id++ {
		key, ok := s.GetKey(id)
		if !ok {
			break
		}
		metricID, tags, err := core.DecodeSeriesKey([]byte(key))
		if err != nil {
			fmt.Printf("%d: (decode error) %q\n", id, key)
			continue
		}
		metricName, _ := ss.GetString(metricID)
		fmt.Printf("%d: metric=%s(%d) tags=", id, metricName, metricID)
		for i, p := range tags {
			if i > 0 {
				fmt.Print(", ")
			}
			k, _ := ss.GetString(p.KeyID)
			v, _ := ss.GetString(p.ValueID)
			fmt.Printf("%s=%s", k, v)
		}
		fmt.Print("\n")
	}
}
