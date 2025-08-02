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
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/INLOpen/nexusbase/api/tsdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

// parseTags converts a comma-separated string of key=value pairs into a map.
func parseTags(tagsStr string) map[string]string {
	if tagsStr == "" {
		return nil
	}
	tags := make(map[string]string)
	pairs := strings.Split(tagsStr, ",")
	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			tags[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return tags
}

func main() {
	// --- Flag definitions ---
	serverAddr := flag.String("addr", "localhost:50051", "The gRPC server address")
	metric := flag.String("metric", "", "The metric to subscribe to. Supports suffix wildcard (e.g., 'cpu.*'). If empty, subscribes to all metrics.")
	tags := flag.String("tags", "", "Comma-separated tags to filter by. Supports suffix wildcard on values (e.g., 'host=server-*,region=us-east')")
	tlsEnabled := flag.Bool("tls", false, "Enable TLS for the connection")
	caFile := flag.String("cacert", "certs/server.crt", "The CA certificate file to trust")
	insecureSkipVerify := flag.Bool("insecure-tls", false, "Enable TLS but skip server certificate verification (INSECURE)")
	username := flag.String("username", "", "Username for authentication")
	password := flag.String("password", "", "Password for authentication")
	flag.Parse()

	// --- gRPC Connection Setup ---
	var opts []grpc.DialOption
	if *tlsEnabled {
		tlsConfig := &tls.Config{ServerName: "localhost"}
		if *insecureSkipVerify {
			log.Println("--- WARNING: Connecting with TLS but skipping server certificate verification (INSECURE) ---")
			tlsConfig.InsecureSkipVerify = true
		} else {
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
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	opts = append(opts, grpc.WithBlock())

	if *username != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(basicAuthCreds{username: *username, password: *password}))
	}

	conn, err := grpc.NewClient(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	client := tsdb.NewTSDBServiceClient(conn)

	// --- Context and Signal Handling for Graceful Shutdown ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\nInterrupt signal received, shutting down subscriber...")
		cancel()
	}()

	// --- Subscription Logic ---
	tagsMap := parseTags(*tags)
	req := &tsdb.SubscribeRequest{
		Metric: *metric,
		Tags:   tagsMap,
	}

	stream, err := client.Subscribe(ctx, req)
	if err != nil {
		log.Fatalf("Could not subscribe: %v", err)
	}

	log.Printf("Subscribing to metric: '%s', tags: %v. Press Ctrl+C to exit.", *metric, tagsMap)

	for {
		update, err := stream.Recv()
		if err != nil {
			// Check if the error is due to the context being canceled (e.g., Ctrl+C)
			if status.Code(err) == codes.Canceled || err == io.EOF {
				log.Println("Subscription stream closed.")
				return
			}
			log.Printf("Error receiving subscription update: %v", err)
			return
		}

		// Format and print the received update
		var updateTypeStr string
		switch update.GetUpdateType() {
		case tsdb.DataPointUpdate_PUT:
			updateTypeStr = "PUT"
		case tsdb.DataPointUpdate_DELETE:
			updateTypeStr = "DELETE"
		default:
			updateTypeStr = "UNKNOWN"
		}

		fmt.Printf("[%s] [%s] Metric: %s, Tags: %v, Timestamp: %d, Value: %.2f\n",
			time.Now().Format(time.RFC3339),
			updateTypeStr,
			update.GetMetric(),
			update.GetTags(),
			update.GetTimestamp(),
			update.GetValue(),
		)
	}
}
