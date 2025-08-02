package main

import (
	"context" // Added for structured printing
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"flag"
	"io"
	"log" // Added for random number generation
	"os"
	"time"

	// Added for seeding rand
	"github.com/INLOpen/nexusbase/api/tsdb" // Import the generated gRPC code
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure" // For insecure connection (no TLS)
)

const (
	address = "localhost:50051" // Address and port of your gRPC server
)

// basicAuthCreds implements credentials.PerRPCCredentials to send a Basic Auth token.
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
	return false // Allow sending credentials with insecure connections for local testing. Set to true for production.
}

func main() {

	subscribeMode := flag.Bool("subscribe", false, "Run in subscribe mode to listen for real-time updates")
	// Define command-line flags for TLS configuration
	tlsEnabled := flag.Bool("tls", false, "Enable TLS for the connection")
	caFile := flag.String("cacert", "certs/server.crt", "The CA certificate file to trust")
	insecureSkipVerify := flag.Bool("insecure-tls", false, "Enable TLS but skip server certificate verification (INSECURE, for development only)")
	username := flag.String("username", "", "Username for authentication")
	password := flag.String("password", "", "Password for authentication")

	flag.Parse()

	// Set up a connection to the gRPC server.
	var opts []grpc.DialOption
	if *tlsEnabled {
		// Load the server's self-signed certificate to trust it as a root CA.
		tlsConfig := &tls.Config{
			ServerName: "localhost", // Must match the CN in the server's certificate
		}

		if *insecureSkipVerify {
			// Insecure TLS: For development, allow skipping server certificate verification.
			// WARNING: This is vulnerable to man-in-the-middle attacks. Do not use in production.
			log.Println("--- WARNING: Connecting with TLS but skipping server certificate verification (INSECURE) ---")
			tlsConfig.InsecureSkipVerify = true
		} else {
			// Secure TLS: Load the server's self-signed certificate to trust it as a root CA.
			// This is the recommended approach for secure TLS.
			log.Println("--- Connecting with secure TLS (verifying server certificate) ---")
			cert, err := os.ReadFile(*caFile)
			if err != nil {
				log.Fatalf("Failed to read CA certificate file: %v", err)
			}
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(cert) {
				log.Fatalf("Failed to append CA certificate to pool")
			}
			tlsConfig.RootCAs = certPool
		}
		creds := credentials.NewTLS(tlsConfig)
		opts = append(opts, grpc.WithTransportCredentials(creds))
		log.Println("--- Connecting with TLS enabled ---")
	} else {
		// Use an insecure connection if TLS is not enabled.
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		log.Println("--- Connecting with TLS disabled (insecure) ---")
	}

	// WithBlock makes the connection synchronous and waits until it's established.
	opts = append(opts, grpc.WithBlock())

	// Add Basic Auth credentials if provided
	if *username != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(basicAuthCreds{username: *username, password: *password}))
	}

	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	// Create a client stub for the TSDBService.
	client := tsdb.NewTSDBServiceClient(conn)

	// Context for RPC calls with a timeout.
	/*ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()*/
	ctx := context.Background()

	if *subscribeMode {
		log.Println("\n--- Running in Subscribe Mode ---")
		subscribeReq := &tsdb.SubscribeRequest{
			Metric: "cpu.usage", // Subscribe to a specific metric
		}

		subStream, err := client.Subscribe(ctx, subscribeReq)
		if err != nil {
			log.Fatalf("Could not subscribe: %v", err)
		}

		log.Println("Subscription active. Waiting for real-time updates for metric 'cpu.usage'...")
		for {
			update, err := subStream.Recv()
			if err == io.EOF {
				log.Println("Subscription stream ended by server.")
				return
			}
			if err != nil {
				log.Printf("Error receiving subscription update: %v", err)
				return
			}
			log.Printf("  [REAL-TIME UPDATE] Type: %s, Metric: %s, Tags: %v, Timestamp: %d, Value: %f",
				update.GetUpdateType(),
				update.GetMetric(),
				update.GetTags(),
				update.GetTimestamp(),
				update.GetValue(),
			)
		}
	}

	// --- The rest of the main function runs in normal (non-subscribe) mode ---
	if *subscribeMode {
		return // Exit if in subscribe mode
	}

	log.Println("--- Performing PutBatch ---")
	/*tsBatch := time.Now().UnixNano()

	fieldsStruct, err := structpb.NewStruct(map[string]interface{}{
		"value": 123.45,
	})

	if _, err := client.Put(ctx, &tsdb.PutRequest{
		Metric:    "btcusdt",
		Tags:      map[string]string{"provider": "binance", "type": "close"},
		Timestamp: tsBatch,
		Fields:    fieldsStruct,
	}); err != nil {
		log.Printf("Could not put data point: %v", err)
	}

	batchPoints := []*tsdb.DataPoint{
		{
			Metric:    "cpu.usage",
			Tags:      map[string]string{"host": "server1", "region": "us-east"},
			Timestamp: tsBatch,
			Value:     55.5,
		},
		{
			Metric:    "cpu.usage",
			Tags:      map[string]string{"host": "server1", "region": "us-east"},
			Timestamp: tsBatch + 1000,
			Value:     56.2,
		},
		{
			Metric:    "memory.usage",
			Tags:      map[string]string{"host": "server1", "region": "us-east"},
			Timestamp: tsBatch,
			Value:     8192.0,
		},
	}
	if _, err := client.PutBatch(ctx, &tsdb.PutBatchRequest{Points: batchPoints}); err != nil {
		log.Printf("Could not put batch data points: %v", err)
	} else {
		log.Println("PutBatch successful.")
	}
	*/
	log.Println("\n--- Performing Query (Streaming) ---")
	agg := make([]*tsdb.AggregationSpec, 0)
	agg = append(agg, &tsdb.AggregationSpec{
		Function: tsdb.AggregationSpec_COUNT,
		Field:    "open",
	})
	agg = append(agg, &tsdb.AggregationSpec{
		Function: tsdb.AggregationSpec_FIRST,
		Field:    "open",
	})
	agg = append(agg, &tsdb.AggregationSpec{
		Function: tsdb.AggregationSpec_LAST,
		Field:    "close",
	})
	agg = append(agg, &tsdb.AggregationSpec{
		Function: tsdb.AggregationSpec_MAX,
		Field:    "high",
	})
	agg = append(agg, &tsdb.AggregationSpec{
		Function: tsdb.AggregationSpec_MIN,
		Field:    "low",
	})
	agg = append(agg, &tsdb.AggregationSpec{
		Function: tsdb.AggregationSpec_SUM,
		Field:    "vol",
	})
	queryReq := &tsdb.QueryRequest{
		Metric:    "__runtime.memory.sys_bytes",
		StartTime: time.Now().Add(-time.Minute * 90).UnixNano(), // Last hour
		EndTime:   time.Now().UnixNano(),
		// AggregationSpecs:   agg,
		// DownsampleInterval: "1m",
	}

	// For streaming RPCs, we get a client stream.
	stream, err := client.Query(ctx, queryReq)
	if err != nil {
		log.Fatalf("Could not query: %v", err)
	}

	log.Println("Query results:")
	for {
		result, err := stream.Recv()
		if err == io.EOF {
			break // End of stream
		}
		if err != nil {
			log.Fatalf("Error receiving query result: %v", err)
		}

		// Process the new QueryResult structure
		if result.GetIsAggregated() {
			log.Printf("  Aggregated Result - Metric: %s, Tags: %v, Window: [%d, %d], Values: %v",
				result.GetMetric(),
				result.GetTags(),
				result.GetWindowStartTime(),
				result.GetWindowEndTime(),
				result.GetAggregatedValues(),
			)
		} else {
			log.Printf("  Raw Data Point - Metric: %s, Tags: %v, Timestamp: %d, Value: %v",
				result.GetMetric(),
				result.GetTags(),
				result.GetTimestamp(),
				result.GetFields().AsMap(),
			)
		}
	}
	log.Println("Query stream finished.")

	log.Println("\n--- Performing GetSeriesByTags ---")
	// Example 1: Get all series for a specific metric
	getSeriesReq1 := &tsdb.GetSeriesByTagsRequest{
		Metric: "__runtime.memory.alloc_bytes",
		// Tags:   map[string]string{"provider": "binance"}, // No tags filter
	}
	getSeriesResp1, err := client.GetSeriesByTags(ctx, getSeriesReq1)
	if err != nil {
		log.Printf("Could not get series by metric: %v", err)
	} else {
		log.Printf("GetSeriesByTags (metric only) successful. Found %d series: %v", len(getSeriesResp1.GetSeriesKeys()), getSeriesResp1.GetSeriesKeys())
	}

	// Example 2: Get all series with a specific tag
	getSeriesReq2 := &tsdb.GetSeriesByTagsRequest{
		Metric: "", // No metric filter
		Tags:   map[string]string{"host": "server1"},
	}
	getSeriesResp2, err := client.GetSeriesByTags(ctx, getSeriesReq2)
	if err != nil {
		log.Printf("Could not get series by tag: %v", err)
	} else {
		log.Printf("GetSeriesByTags (tag only) successful. Found %d series: %v", len(getSeriesResp2.GetSeriesKeys()), getSeriesResp2.GetSeriesKeys())
	}

	log.Println("\n--- Client operations finished ---")
}
