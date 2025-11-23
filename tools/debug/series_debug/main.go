package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/INLOpen/nexusbase/core"
)

func processFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	fmt.Printf("File: %s\n", path)
	start := 0
	lineNo := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			if i > start {
				seg := data[start:i]
				metricID, tags, derr := core.DecodeSeriesKey(seg)
				if derr != nil {
					fmt.Printf("%03d: len=%d error decoding: %v\n", lineNo, len(seg), derr)
				} else {
					fmt.Printf("%03d: metricID=%d tags=[", lineNo, metricID)
					for j, p := range tags {
						if j > 0 {
							fmt.Printf(", ")
						}
						fmt.Printf("(%d=%d)", p.KeyID, p.ValueID)
					}
					fmt.Printf("]\n")
				}
			} else {
				fmt.Printf("%03d: (empty line)\n", lineNo)
			}
			lineNo++
			start = i + 1
		}
	}
	// Last segment
	if start < len(data) {
		seg := data[start:]
		metricID, tags, derr := core.DecodeSeriesKey(seg)
		if derr != nil {
			fmt.Printf("%03d: len=%d error decoding: %v\n", lineNo, len(seg), derr)
		} else {
			fmt.Printf("%03d: metricID=%d tags=[", lineNo, metricID)
			for j, p := range tags {
				if j > 0 {
					fmt.Printf(", ")
				}
				fmt.Printf("(%d=%d)", p.KeyID, p.ValueID)
			}
			fmt.Printf("]\n")
		}
	}
	return nil
}

func main() {
	temp := os.TempDir()
	files := []string{
		filepath.Join(temp, "nexus_debug_series_reads.log"),
		filepath.Join(temp, "nexus_debug_series_writes.log"),
	}
	for _, p := range files {
		if _, err := os.Stat(p); err == nil {
			if err := processFile(p); err != nil {
				fmt.Fprintf(os.Stderr, "failed to process %s: %v\n", p, err)
			}
		} else {
			fmt.Printf("missing %s\n", p)
		}
	}
}
