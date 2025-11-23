package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/INLOpen/nexusbase/core"
)

func main() {
	var path string
	flag.StringVar(&path, "file", "", "path to binary series log file")
	flag.Parse()
	if path == "" {
		fmt.Println("provide -file path")
		os.Exit(2)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read failed: %v\n", err)
		os.Exit(1)
	}
	start := 0
	idx := 0
	for i := 0; i < len(b); i++ {
		if b[i] == '\n' {
			if i > start {
				seg := b[start:i]
				metricID, tags, derr := core.DecodeSeriesKey(seg)
				if derr != nil {
					fmt.Printf("%03d: len=%d decodeErr=%v\n", idx, len(seg), derr)
				} else {
					fmt.Printf("%03d: metricID=%d tags=", idx, metricID)
					for j, p := range tags {
						if j > 0 {
							fmt.Print(",")
						}
						fmt.Printf("(%d=%d)", p.KeyID, p.ValueID)
					}
					fmt.Print("\n")
				}
			} else {
				fmt.Printf("%03d: empty line\n", idx)
			}
			idx++
			start = i + 1
		}
	}
	if start < len(b) {
		seg := b[start:]
		metricID, tags, derr := core.DecodeSeriesKey(seg)
		if derr != nil {
			fmt.Printf("%03d: len=%d decodeErr=%v\n", idx, len(seg), derr)
		} else {
			fmt.Printf("%03d: metricID=%d tags=", idx, metricID)
			for j, p := range tags {
				if j > 0 {
					fmt.Print(",")
				}
				fmt.Printf("(%d=%d)", p.KeyID, p.ValueID)
			}
			fmt.Print("\n")
		}
	}
}
