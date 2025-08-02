package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	// 1. Import the client library from its new location
	nbql "github.com/INLOpen/nexusbase/clients/nbql/golang"
)

func main() {
	// --- Parse command-line flags for server details ---
	host := flag.String("host", "10.1.1.1:50052", "Server address (host:port)")
	user := flag.String("user", "", "Username for authentication")
	pass := flag.String("password", "", "Password for authentication")
	flag.Parse()

	// --- 2. Set up connection options ---
	opts := nbql.Options{
		Address:  *host,
		Username: *user,
		Password: *pass,
		// Logger is optional, defaults to a no-op logger.
	}

	// Use a context with a timeout for the connection attempt.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// --- 3. Connect to the server ---
	client, err := nbql.Connect(ctx, opts)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	// Ensure the connection is closed when main() exits.
	defer client.Close()

	fmt.Println("✅ Connection successful.")

	// --- 4. Push a new data point ---
	fmt.Println("\n--- Pushing a new data point ---")
	err = doPush(ctx, client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Push failed: %v\n", err)
	} else {
		fmt.Println("✅ Push successful.")
	}

	// --- 5. Execute a query ---
	fmt.Println("\n--- Executing a query ---")
	err = doQuery(ctx, client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Query failed: %v\n", err)
	}
}

// doPush demonstrates how to write a single data point to the server.
func doPush(ctx context.Context, client *nbql.Client) error {
	metric := "system.load"
	tags := map[string]string{
		"host":   "client-host-01",
		"region": "us-west",
	}
	// Fields can contain multiple key-value pairs of different types.
	fields := map[string]interface{}{
		"load1":  1.25,
		"load5":  0.85,
		"load15": 0.55,
		"active": true,
	}
	timestamp := time.Now().UnixNano()

	fmt.Printf("Pushing metric '%s' with tags %v and fields %v\n", metric, tags, fields)
	return client.Push(ctx, metric, tags, fields, timestamp)
}

// doQuery demonstrates how to query data and process the results.
func doQuery(ctx context.Context, client *nbql.Client) error {
	// A query to get the average load over 1-minute intervals for the last hour.
	// Using parameterized queries is the recommended and safest way to execute queries.
	// It prevents NBQL injection attacks by properly quoting and escaping parameters.
	startTime := time.Now().Add(-time.Hour).UnixNano()
	endTime := time.Now().UnixNano()
	queryTemplate := `QUERY ? FROM ? TO ? TAGGED (region=?) AGGREGATE BY 1m (avg(load1), max(load5));`
	metricName := "system.load"
	region := "us-west"

	fmt.Printf("Executing query: '%s' with params: (%s, %d, %d, %s)\n", queryTemplate, metricName, startTime, endTime, region)
	result, err := client.Query(ctx, queryTemplate, metricName, startTime, endTime, region)
	if err != nil {
		return err
	}

	// Print results
	fmt.Printf("\n--- Query Results ---\n")
	for i, row := range result.Rows {
		fmt.Printf("Row %d:\n", i+1)
		fmt.Printf("  Timestamp:  %s\n", time.Unix(0, row.Timestamp).Format(time.RFC3339))
		if row.IsAggregated {
			fmt.Printf("  Aggregated: %v\n", row.AggregatedValues)
		} else {
			// This block would run for non-aggregated queries.
			fmt.Printf("  Tags:       %v\n", row.Tags)
			fmt.Printf("  Fields:     %v\n", row.Fields)
		}
	}
	fmt.Printf("\n✅ Total rows returned: %d\n", result.TotalRows)
	return nil
}
