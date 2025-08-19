package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexusbase/replication"
)

func main() {
	// In a real application, load this from a config file.
	leaderAddr := "localhost:50052" // The address of the leader's replication service
	dataDir := "./follower-data"
	bootstrapThreshold := uint64(1_000_000) // Example: if we are more than 1M entries behind, bootstrap.

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Use default engine options for the follower. These should also be configurable.
	// This is a conceptual function. You would need to create it based on your engine's needs.
	engineOpts := engine.DefaultStorageEngineOptions()
	engineOpts.DataDir = dataDir
	engineOpts.Logger = logger

	// Create the follower instance.
	follower, err := replication.NewFollower(leaderAddr, bootstrapThreshold, engineOpts, logger)
	if err != nil {
		logger.Error("Failed to create follower", "error", err)
		os.Exit(1)
	}

	// Start the follower's replication loop.
	if err := follower.Start(); err != nil {
		logger.Error("Failed to start follower", "error", err)
		os.Exit(1)
	}

	// Wait for termination signal.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutdown signal received, stopping follower...")
	follower.Stop()
	logger.Info("Follower has been shut down.")
}
