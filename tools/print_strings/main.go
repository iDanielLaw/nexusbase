package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
)

func main() {
	var dir string
	flag.StringVar(&dir, "dir", "", "data directory to load string mappings from")
	flag.Parse()
	if dir == "" {
		fmt.Println("Please provide -dir path")
		os.Exit(2)
	}
	ss := indexer.NewStringStore(nil, hooks.NewHookManager(nil))
	if err := ss.LoadFromFile(dir); err != nil {
		fmt.Fprintf(os.Stderr, "LoadFromFile failed: %v\n", err)
		os.Exit(1)
	}
	syms := ss.Symbols()
	for _, s := range syms {
		id, _ := ss.GetID(s)
		fmt.Printf("%d: %s\n", id, s)
	}
}
