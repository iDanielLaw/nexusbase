package main

import (
	"fmt"
	"log/slog"
	"io"
	"sort"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
)

func main() {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hookManager := hooks.NewHookManager(nil)
	stringStore := indexer.NewStringStore(logger, hookManager)

	metric := "cpu.usage"
	tags := map[string]string{"host": "server1"}

	fmt.Println("--- Step 1: Populating StringStore ---")
	metricID, _ := stringStore.GetOrCreateID(metric)
	fmt.Printf("Metric '%s' -> ID: %d\n", metric, metricID)

	var encodedTags []core.EncodedSeriesTagPair
	for k, v := range tags {
		kID, _ := stringStore.GetOrCreateID(k)
		vID, _ := stringStore.GetOrCreateID(v)
		fmt.Printf("Tag '%s' -> ID: %d\n", k, kID)
		fmt.Printf("Tag Value '%s' -> ID: %d\n", v, vID)
		encodedTags = append(encodedTags, core.EncodedSeriesTagPair{KeyID: kID, ValueID: vID})
	}
	sort.Slice(encodedTags, func(i, j int) bool {
		return encodedTags[i].KeyID < encodedTags[j].KeyID
	})
	fmt.Printf("Encoded Tags (by ID): %+v\n", encodedTags)

	fmt.Println("\n--- Step 2: Encoding Series Key ---")
	seriesKeyBuf := core.GetBuffer()
	defer core.PutBuffer(seriesKeyBuf)
	core.EncodeSeriesKeyToBuffer(seriesKeyBuf, metricID, encodedTags)
	seriesKeyBytes := seriesKeyBuf.Bytes()
	fmt.Printf("Encoded Series Key (bytes): %x\n", seriesKeyBytes)

	fmt.Println("\n--- Step 3: Decoding Series Key ---")
	decodedMetricID, decodedTagPairs, err := core.DecodeSeriesKey(seriesKeyBytes)
	if err != nil {
		fmt.Printf("ERROR decoding series key: %v\n", err)
		return
	}
	fmt.Printf("Decoded Metric ID: %d\n", decodedMetricID)
	fmt.Printf("Decoded Tag Pairs: %+v\n", decodedTagPairs)

	fmt.Println("\n--- Step 4: Verifying with StringStore ---")
	retrievedMetric, ok := stringStore.GetString(decodedMetricID)
	if !ok {
		fmt.Printf("ERROR: Metric ID %d not found in StringStore!\n", decodedMetricID)
	} else {
		fmt.Printf("Retrieved Metric for ID %d: '%s'\n", decodedMetricID, retrievedMetric)
	}

	for _, pair := range decodedTagPairs {
		retrievedKey, okK := stringStore.GetString(pair.KeyID)
		retrievedVal, okV := stringStore.GetString(pair.ValueID)
		if !okK || !okV {
			fmt.Printf("ERROR: Tag IDs %d or %d not found!\n", pair.KeyID, pair.ValueID)
		} else {
			fmt.Printf("Retrieved Tag Pair: '%s'='%s'\n", retrievedKey, retrievedVal)
		}
	}
}