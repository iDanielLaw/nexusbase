package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/INLOpen/nexusbase/wal"
)

func main() {
	var dir string
	flag.StringVar(&dir, "dir", "", "wal directory path")
	flag.Parse()
	if dir == "" {
		log.Fatal("provide -dir")
	}
	opts := wal.Options{Dir: dir}
	w, entries, err := wal.Open(opts)
	if err != nil {
		log.Fatalf("wal.Open failed: %v", err)
	}
	fmt.Printf("Recovered %d entries\n", len(entries))
	for i, e := range entries {
		fmt.Printf("%03d: type=%v seq=%d key_len=%d value_len=%d\n", i, e.EntryType, e.SeqNum, len(e.Key), len(e.Value))
	}
	w.Close()
}
