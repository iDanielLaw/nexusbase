package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/INLOpen/nexusbase/engine"
)

func main() {
	var dir string
	flag.StringVar(&dir, "dir", "", "data directory")
	flag.Parse()
	if dir == "" {
		log.Fatal("provide -dir")
	}
	opts := engine.StorageEngineOptions{DataDir: dir}
	engIface, err := engine.NewStorageEngine(opts)
	if err != nil {
		log.Fatalf("NewStorageEngine failed: %v", err)
	}
	if err := engIface.Start(); err != nil {
		log.Fatalf("Start failed: %v", err)
	}

	series, err := engine.GetActiveSeriesSnapshot(engIface)
	if err != nil {
		log.Fatalf("GetActiveSeriesSnapshot failed: %v", err)
	}
	fmt.Printf("activeSeries count=%d\n", len(series))
	for _, k := range series {
		fmt.Printf("- %q\n", k)
	}

	if err := engIface.Close(); err != nil {
		log.Printf("warning: Close returned error: %v", err)
	}
}
