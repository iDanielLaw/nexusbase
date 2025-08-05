package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/engine/snapshot"
)

func main() {
	// Define command-line flags
	snapshotDir := flag.String("snapshot-dir", "", "Path to the snapshot directory to restore from (required)")
	targetDataDir := flag.String("target-dir", "", "Path to the new data directory where the database will be restored (required)")
	logLevel := flag.String("log-level", "info", "Logging level (debug, info, warn, error)")
	logOutput := flag.String("log-output", "stdout", "Log output (stdout, file, none)")
	logFile := flag.String("log-file", "restore-util.log", "Path to log file if output is 'file'")
	flag.Parse()

	// Validate required flags
	if *snapshotDir == "" || *targetDataDir == "" {
		fmt.Println("Usage: restore-util -snapshot-dir <path_to_snapshot> -target-dir <path_to_new_data_dir>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// --- Logger Setup ---
	var level slog.Level
	switch strings.ToLower(*logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		fmt.Printf("Invalid log level: %s. Defaulting to info.\n", *logLevel)
		level = slog.LevelInfo
	}

	var output io.Writer = os.Stdout
	switch strings.ToLower(*logOutput) {
	case "stdout":
		// Already set
	case "file":
		file, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			slog.Error("Failed to open log file", "path", *logFile, "error", err)
			os.Exit(1)
		}
		defer file.Close()
		output = file
	case "none":
		output = io.Discard
	}
	logger := slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: level}))

	if err := run(*snapshotDir, *targetDataDir, logger); err != nil {
		logger.Error("Restore failed", "error", err)
		os.Exit(1)
	}
}

func run(snapshotDir, targetDataDir string, logger *slog.Logger) error {
	logger.Info("Starting restore from snapshot...", "snapshot_dir", snapshotDir, "target_dir", targetDataDir)
	// Check if target directory already exists and is not empty
	if _, err := os.Stat(targetDataDir); !os.IsNotExist(err) {
		// Check if directory is empty
		dir, err := os.ReadDir(targetDataDir)
		if err != nil {
			return fmt.Errorf("failed to read target directory %s: %w", targetDataDir, err)
		}
		if len(dir) > 0 {
			return fmt.Errorf("target directory %s already exists and is not empty. Please specify a new or empty directory", targetDataDir)
		}
	}

	// We need a default engine configuration to pass to RestoreFromSnapshot.
	// The function primarily needs the target DataDir. Other options are not used
	// during the file copy process but are required by the function signature.
	// We can load a default config to get reasonable values.
	_, err := config.LoadConfig("") // Load defaults
	if err != nil {
		return fmt.Errorf("failed to load default configuration: %w", err)
	}

	// Create snapshot restore options with the target data directory and logger.
	restoreOpts := snapshot.RestoreOptions{
		DataDir: targetDataDir,
		Logger:  logger,
	}

	// Call the restore function from the snapshot package
	if err := snapshot.RestoreFromFull(restoreOpts, snapshotDir); err != nil {
		return fmt.Errorf("failed to restore from snapshot: %w", err)
	}

	logger.Info("Restore completed successfully.")
	logger.Info("You can now start the TSDB server with the data directory set to:", "data_dir", targetDataDir)
	return nil
}
