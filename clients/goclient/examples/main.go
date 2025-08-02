package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	client "github.com/INLOpen/nexusbase/clients/goclient"
)

func main() {
	// Connect to the server
	c, err := client.NewClient("10.1.1.1:50051")
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	metric := "cpu.usage"
	tags := map[string]string{"host": "server-1", "region": "us-east"}
	timestamp := time.Now().UnixNano()

	// 1. Put a data point
	dp := client.DataPoint{
		Metric:    metric,
		Tags:      tags,
		Timestamp: timestamp,
		Fields: map[string]interface{}{
			"value": 85.5,
			"core":  "core-0",
		},
	}
	if err := c.Put(ctx, dp); err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	fmt.Println("Successfully put data point.")

	// 2. Get the data point back
	retrievedFields, err := c.Get(ctx, metric, tags, timestamp)
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("Successfully retrieved data point: %+v\n", retrievedFields)

	// 3. Query for the data
	queryParams := client.QueryParams{
		Metric:    "btcusdt", // metric,
		Tags:      nil,       // tags,
		Limit:     10,
		StartTime: timestamp - time.Minute.Nanoseconds(),
		EndTime:   timestamp + time.Minute.Nanoseconds(),
	}
	iter, err := c.Query(ctx, queryParams)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	fmt.Println("Query results:")
	for {
		item, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error during query iteration: %v", err)
		}
		fmt.Printf("- %+v\n", item)
	}
}
