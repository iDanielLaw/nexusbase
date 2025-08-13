package main

import (
	"context"
	"fmt"
	"log"

	client "github.com/INLOpen/nexusbase/clients/goclient"
)

func main() {
	// Connect to the server
	c, err := client.NewClient("127.0.0.1:50051")
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	err = c.CreateSnapshot(ctx, "data/snapshots")
	if err != nil {
		fmt.Println(err)
	}
}
