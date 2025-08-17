package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal" // New import
	"strings"
	"syscall" // New import
	"time"

	// Generated gRPC code
	// Core utilities
	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine" // StorageEngine
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/hooks/listeners"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/wal"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"

	// For sstable.DefaultBlockSize and ErrNotFound
	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/server" // Your gRPC server implementation
)

// createLogger creates a slog.Logger based on the provided configuration.
func createLogger(cfg config.LoggingConfig) (*slog.Logger, io.Closer, error) {
	var level slog.Level
	switch strings.ToLower(cfg.Level) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, nil, fmt.Errorf("invalid log level: %s", cfg.Level)
	}

	var output io.Writer
	var closer io.Closer
	switch strings.ToLower(cfg.Output) {
	case "stdout":
		output = os.Stdout
	case "file":
		if cfg.File == "" {
			return nil, nil, fmt.Errorf("log output is 'file' but no file path is specified")
		}
		file, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open log file %s: %w", cfg.File, err)
		}
		output = file
		closer = file // The file handle is the closer.
	case "none":
		output = io.Discard
	default:
		return nil, nil, fmt.Errorf("invalid log output: %s", cfg.Output)
	}

	logger := slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: level}))
	return logger, closer, nil
}

// initTracerProvider creates and configures an OpenTelemetry TracerProvider.
// It sets up an exporter based on the configuration to send traces to a collector.
func initTracerProvider(cfg config.TracingConfig, logger *slog.Logger) (*sdktrace.TracerProvider, func(), error) {
	if !cfg.Enabled {
		logger.Info("Distributed tracing is disabled.")
		// Return a no-op provider and an empty cleanup function.
		return sdktrace.NewTracerProvider(), func() {}, nil
	}

	logger.Info("Initializing distributed tracing...", "protocol", cfg.Protocol, "endpoint", cfg.Endpoint)

	ctx := context.Background()
	var exporter sdktrace.SpanExporter
	var err error

	// Create an OTLP exporter (gRPC or HTTP)
	switch strings.ToLower(cfg.Protocol) {
	case "http":
		exporter, err = otlptrace.New(ctx, otlptracehttp.NewClient(otlptracehttp.WithEndpoint(cfg.Endpoint), otlptracehttp.WithInsecure()))
	case "grpc":
		exporter, err = otlptrace.New(ctx, otlptracegrpc.NewClient(otlptracegrpc.WithEndpoint(cfg.Endpoint), otlptracegrpc.WithInsecure()))
	default:
		return nil, nil, fmt.Errorf("unsupported tracing protocol: %q", cfg.Protocol)
	}

	// Define the service resource
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceNameKey.String("nexusbase")))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create trace resource: %w", err)
	}

	// Create the TracerProvider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	// Set the global TracerProvider
	otel.SetTracerProvider(tp)

	cleanup := func() {
		logger.Info("Shutting down tracer provider...")
		// Create a context with a timeout to prevent shutdown from hanging.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(shutdownCtx); err != nil {
			logger.Error("Error shutting down tracer provider", "error", err)
		}
	}

	return tp, cleanup, nil
}

func main() {
	// Define a command-line flag for the config file path
	configPath := flag.String("config", "config.yaml", "Path to the configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		// Use a temporary logger for pre-config errors
		slog.Error("Failed to load configuration", "path", *configPath, "error", err)
		os.Exit(1)
	}

	// Create the logger based on the loaded configuration
	logger, logCloser, err := createLogger(cfg.Logging)
	if err != nil {
		slog.Error("Failed to create logger", "error", err)
		os.Exit(1)
	}
	// Defer closing the log file if one was opened.
	if logCloser != nil {
		defer logCloser.Close()
	}

	// The engine will now create its own subdirectories. We just need to ensure the base dir is specified.
	if cfg.Engine.DataDir == "" {
		logger.Error("Engine data_dir must be specified in the configuration file.")
		os.Exit(1)
	}
	logger.Info("Using data directory", "path", cfg.Engine.DataDir)

	// Select compressor based on config
	var sstCompressor core.Compressor
	switch cfg.Engine.SSTable.Compression {
	case "lz4":
		sstCompressor = &compressors.LZ4Compressor{}
		logger.Info("Using LZ4 compression for SSTables.")
	case "zstd":
		sstCompressor = compressors.NewZstdCompressor()
		logger.Info("Using ZSTD compression for SSTables.")
	case "snappy":
		sstCompressor = &compressors.SnappyCompressor{}
		logger.Info("Using Snappy compression for SSTables.")
	case "none":
		sstCompressor = &compressors.NoCompressionCompressor{}
		logger.Info("Using no compression for SSTables.")
	default:
		logger.Error("Invalid sstable_compression value in config.", "value", cfg.Engine.SSTable.Compression)
		os.Exit(1)
	}
	var metricSrv *server.MetricsServer
	if cfg.Debug.Enabled {
		metricSrv = server.NewMetricsServer(&cfg.Debug, logger)
		go func() {
			if err := metricSrv.Start(); err != nil {
				logger.Error("Failed to start metrics server", "error", err)
			}
		}()
	}

	// Initialize the TracerProvider
	tp, tracerCleanup, err := initTracerProvider(cfg.Tracing, logger)
	if err != nil {
		logger.Error("Failed to initialize tracer provider", "error", err)
		os.Exit(1)
	}

	// Parse durations from config strings
	memtableFlushInterval := config.ParseDuration(cfg.Engine.Memtable.FlushInterval, 0, logger)
	compactionInterval := config.ParseDuration(cfg.Engine.Compaction.CheckInterval, 120*time.Second, logger)
	metadataSyncInterval := config.ParseDuration(cfg.Engine.MetadataSyncInterval, 60*time.Second, logger)
	checkpointInterval := config.ParseDuration(cfg.Engine.CheckpointInterval, 300*time.Second, logger)
	walFlushInterval := config.ParseDuration(cfg.Engine.WAL.FlushInterval, 1000*time.Millisecond, logger)
	selfMonitoringInterval := config.ParseDuration(cfg.SelfMonitoring.Interval, 5*time.Second, logger)
	indexFlushInterval := config.ParseDuration(cfg.Engine.Index.FlushInterval, 60*time.Second, logger)
	indexCompactionInterval := config.ParseDuration(cfg.Engine.Index.CompactionCheckInterval, 20*time.Second, logger)

	// Parse CompactionFallbackStrategy
	var fallbackStrategy levels.CompactionFallbackStrategy
	switch strings.ToLower(cfg.Engine.Compaction.FallbackStrategy) {
	case "picklargest":
		fallbackStrategy = levels.PickLargest
	case "picksmallest":
		fallbackStrategy = levels.PickSmallest
	case "pickmostkeys":
		fallbackStrategy = levels.PickMostKeys
	case "picksmallestavgkeysize":
		fallbackStrategy = levels.PickSmallestAvgKeySize
	case "pickoldestbytimestamp":
		fallbackStrategy = levels.PickOldestByTimestamp
	case "pickfewestkeys":
		fallbackStrategy = levels.PickFewestKeys
	case "pickrandom":
		fallbackStrategy = levels.PickRandom
	case "picklargestavgkeysize":
		fallbackStrategy = levels.PickLargestAvgKeySize
	case "pickoldest":
		fallthrough
	default:
		fallbackStrategy = levels.PickOldest
	}

	// Configure StorageEngine options
	opts := engine.StorageEngineOptions{
		DataDir:                        cfg.Engine.DataDir,
		MemtableThreshold:              cfg.Engine.Memtable.SizeThresholdBytes,
		MemtableFlushIntervalMs:        int(memtableFlushInterval.Milliseconds()),
		BlockCacheCapacity:             cfg.Engine.Cache.BlockCacheCapacity,
		L0CompactionTriggerSize:        cfg.Engine.Compaction.L0TriggerSizeBytes,
		MaxL0Files:                     cfg.Engine.Compaction.L0TriggerFileCount,
		TargetSSTableSize:              cfg.Engine.Compaction.TargetSSTableSizeBytes,
		LevelsTargetSizeMultiplier:     cfg.Engine.Compaction.LevelsSizeMultiplier,
		MaxLevels:                      cfg.Engine.Compaction.MaxLevels,
		BloomFilterFalsePositiveRate:   cfg.Engine.SSTable.BloomFilterFPRate,
		SSTableDefaultBlockSize:        int(cfg.Engine.SSTable.BlockSizeBytes),
		CompactionIntervalSeconds:      int(compactionInterval.Seconds()),
		TracerProvider:                 tp, // Pass the configured provider
		MetadataSyncIntervalSeconds:    int(metadataSyncInterval.Seconds()),
		CheckpointIntervalSeconds:      int(checkpointInterval.Seconds()),
		SSTableCompressor:              sstCompressor,
		WALSyncMode:                    wal.WALSyncMode(cfg.Engine.WAL.SyncMode),
		WALBatchSize:                   cfg.Engine.WAL.BatchSize,
		WALFlushIntervalMs:             int(walFlushInterval.Milliseconds()),
		WALMaxSegmentSize:              cfg.Engine.WAL.MaxSegmentSizeBytes,
		WALPurgeKeepSegments:           cfg.Engine.WAL.PurgeKeepSegments,
		RetentionPeriod:                cfg.Engine.RetentionPeriod,
		SelfMonitoringEnabled:          cfg.SelfMonitoring.Enabled,
		SelfMonitoringIntervalMs:       int(selfMonitoringInterval.Milliseconds()),
		IndexMemtableThreshold:         cfg.Engine.Index.MemtableThreshold,
		IndexFlushIntervalMs:           int(indexFlushInterval.Milliseconds()),
		IndexCompactionIntervalSeconds: int(indexCompactionInterval.Seconds()),
		IndexMaxL0Files:                cfg.Engine.Index.L0TriggerFileCount,
		IndexBaseTargetSize:            cfg.Engine.Index.BaseTargetSizeBytes,
		CompactionFallbackStrategy:     fallbackStrategy,
		Logger:                         logger,
	}

	// Create the storage engine instance first.
	dbEngine, err := engine.NewStorageEngine(opts)
	if err != nil {
		logger.Error("Failed to create storage engine", "error", err)
		os.Exit(1)
	}

	// --- Register Hooks ---
	// 1. Create listener instances
	waListener := listeners.NewWriteAmplificationListener(logger)
	cardinalityListener := listeners.NewCardinalityAlerterListener(logger)
	// Example outlier detection rules
	outlierRules := []listeners.OutlierRule{
		{
			MetricName: "http.requests.latency",
			FieldName:  "ms",
			Thresholds: listeners.Thresholds{Min: 0, Max: 2000}, // Alert if latency > 2 seconds
		},
		{
			MetricName: "environment.temperature",
			FieldName:  "celsius",
			Thresholds: listeners.Thresholds{Min: -10, Max: 50}, // Alert if temp is outside this range
		},
	}
	outlierListener := listeners.NewOutlierDetectionListener(logger, outlierRules)
	hookManager := dbEngine.GetHookManager()

	//Register the listener for the PostCompaction event
	hookManager.Register(hooks.EventPostCompaction, waListener)
	hookManager.Register(hooks.EventOnSeriesCreate, cardinalityListener)
	hookManager.Register(hooks.EventPrePutBatch, outlierListener)
	logger.Info("Registered WriteAmplificationListener for PostCompaction events.")
	logger.Info("Registered CardinalityAlerterListener for OnSeriesCreate events.")
	// --- End Register Hooks ---

	// --- System Metrics Collector ---
	// The collector will publish metrics that are automatically exposed by the /metrics endpoint.
	// We use the engine's data directory as the path to monitor for disk usage.
	systemCollector := server.NewSystemCollector(cfg.Engine.DataDir, 2*time.Second, logger)
	systemCollector.Start()
	// --- End System Metrics Collector ---

	if err := dbEngine.Start(); err != nil {
		logger.Error("Failed to start storage engine", "error", err)
		os.Exit(1)
	}

	// Create and initialize the application server
	appServer, err := server.NewAppServer(dbEngine, cfg, logger)
	if err != nil {
		logger.Error("Failed to create application server", "error", err) // Ensure engine is closed if app server creation fails
		dbEngine.Close()
		os.Exit(1)
	}

	// Keep the main goroutine alive to allow the gRPC server to run
	// In a real application, you'd typically have a signal handler here
	// to gracefully shut down the server on SIGINT/SIGTERM.
	logger.Info("Application running. Press Ctrl+C to exit.")

	// Graceful shutdown: Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	serverErrChan := make(chan error, 1)
	go func() {
		serverErrChan <- appServer.Start()
	}()

	select {
	case err := <-serverErrChan:
		logger.Error("Server exited with an error", "error", err)
	case <-quit:
		logger.Info("Shutdown signal received. Stopping server...")
		appServer.Stop() // 1. ส่งสัญญาณให้ AppServer หยุด (ไม่ block)

		// 2. รอจนกว่า goroutine ของ AppServer จะทำงานเสร็จสิ้น (block)
		<-serverErrChan

		// 3. เมื่อ Server หยุดสนิทแล้ว จึงสั่งปิด Database Engine (block จนกว่าจะ flush เสร็จ)
		dbEngine.Close()

		tracerCleanup() // Shutdown the tracer provider
		systemCollector.Stop()
		if metricSrv != nil {
			metricSrv.Stop()
		}

		logger.Info("Application exited gracefully.")
	}
}
