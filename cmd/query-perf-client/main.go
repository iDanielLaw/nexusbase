package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/INLOpen/nexusbase/api/tsdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// basicAuthCreds implements credentials.PerRPCCredentials for Basic Auth.
type basicAuthCreds struct {
	username string
	password string
}

func (c basicAuthCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	auth := c.username + ":" + c.password
	enc := base64.StdEncoding.EncodeToString([]byte(auth))
	return map[string]string{"authorization": "Basic " + enc}, nil
}

func (c basicAuthCreds) RequireTransportSecurity() bool {
	return false // Set to true in production with TLS
}

func main() {
	// --- Configuration Flags ---
	serverAddr := flag.String("addr", "localhost:50051", "The gRPC server address")
	tlsEnabled := flag.Bool("tls", false, "Enable TLS for the connection")
	caFile := flag.String("cacert", "certs/server.crt", "The CA certificate file to trust")
	username := flag.String("username", "", "Username for authentication")
	password := flag.String("password", "", "Password for authentication")

	// Benchmark flags (PERF-002)
	numQueries := flag.Int("queries", 100000, "Total number of queries to execute")
	numSeries := flag.Int("series", 100, "Number of unique time series that exist in the DB (for random selection)")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent queriers (goroutines)")
	queryRange := flag.Duration("range", time.Hour, "The time range for each query (e.g., '1h', '15m')")
	metric := flag.String("metric", "perf.test.metric", "The metric name to query")

	flag.Parse()

	// --- gRPC Connection Setup ---
	var opts []grpc.DialOption
	if *tlsEnabled {
		log.Println("--- Connecting with secure TLS ---")
		cert, err := os.ReadFile(*caFile)
		if err != nil {
			log.Fatalf("Failed to read CA certificate file: %v", err)
		}
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(cert) {
			log.Fatalf("Failed to append CA certificate to pool")
		}
		creds := credentials.NewTLS(&tls.Config{
			ServerName: "localhost",
			RootCAs:    certPool,
		})
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		log.Println("--- Connecting with TLS disabled (insecure) ---")
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if *username != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(basicAuthCreds{username: *username, password: *password}))
	}

	conn, err := grpc.NewClient(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	client := tsdb.NewTSDBServiceClient(conn)

	// --- Benchmark Execution ---
	var wg sync.WaitGroup
	var totalQueries int64
	queriesPerWorker := *numQueries / *concurrency

	latencies := make([]time.Duration, 0, *numQueries)
	var latenciesMu sync.Mutex

	log.Printf("Starting benchmark with %d concurrent queriers, running %d total queries...", *concurrency, *numQueries)
	start := time.Now()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < queriesPerWorker; j++ {
				// Select a random series to query
				seriesIndex := rand.Intn(*numSeries)
				tags := map[string]string{
					"host":   fmt.Sprintf("host-%d", seriesIndex),
					"region": fmt.Sprintf("region-%d", seriesIndex%5),
				}

				// Define time range for the query
				endTime := time.Now()
				startTime := endTime.Add(-*queryRange)

				req := &tsdb.QueryRequest{
					Metric:    *metric,
					StartTime: startTime.UnixNano(),
					EndTime:   endTime.UnixNano(),
					Tags:      tags,
				}

				reqStart := time.Now()

				stream, err := client.Query(context.Background(), req)
				if err != nil {
					log.Printf("Query failed: %v", err)
					continue
				}

				// Drain the stream to get the full latency of receiving all data
				for {
					_, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						log.Printf("Stream Recv failed: %v", err)
						break
					}
				}

				reqDuration := time.Since(reqStart)

				latenciesMu.Lock()
				latencies = append(latencies, reqDuration)
				latenciesMu.Unlock()
				atomic.AddInt64(&totalQueries, 1)
			}
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	// --- Results ---
	queriesPerSec := float64(totalQueries) / duration.Seconds()

	// --- Calculate Latency Percentiles ---
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	var p50, p90, p99 time.Duration
	if len(latencies) > 0 {
		p50 = latencies[int(float64(len(latencies))*0.50)]
		p90 = latencies[int(float64(len(latencies))*0.90)]
		p99 = latencies[int(float64(len(latencies))*0.99)]
	}

	fmt.Println("\n--- Benchmark Results (PERF-002) ---")
	fmt.Printf("Total Queries Executed: %d\n", totalQueries)
	fmt.Printf("Total Time Taken:       %.2f seconds\n", duration.Seconds())
	fmt.Printf("Query Throughput:       %.2f queries/sec\n", queriesPerSec)
	fmt.Println("\n--- Query Latency Distribution (Raw Data) ---")
	fmt.Printf("P50 (Median): %v\n", p50)
	fmt.Printf("P90:          %v\n", p90)
	fmt.Printf("P99:          %v\n", p99)
	fmt.Println("------------------------------------")
	fmt.Println("\n--- Cache Hit Rate ---")
	fmt.Println("Cache Hit Rate is a server-side metric.")
	fmt.Println("Please monitor the server's /debug/vars endpoint while the test is running.")
	fmt.Println("Calculate it using: cache_hits / (cache_hits + cache_misses)")
	fmt.Println("Example: curl http://localhost:50051/debug/vars")
}
