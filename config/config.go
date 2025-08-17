package config

import (
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

// EngineConfig mirrors engine.StorageEngineOptions for YAML unmarshaling.
type EngineConfig struct {
	DataDir                        string  `yaml:"data_dir"`
	MemtableThresholdBytes         int64   `yaml:"memtable_threshold_bytes"`
	MemtableFlushIntervalMs        int     `yaml:"memtable_flush_interval_ms"`
	BlockCacheCapacity             int     `yaml:"block_cache_capacity"`
	L0CompactionTriggerSizeBytes   int64   `yaml:"l0_compaction_trigger_size_bytes"`
	MaxL0Files                     int     `yaml:"max_l0_files"`
	TargetSSTableSizeBytes         int64   `yaml:"target_sstable_size_bytes"`
	LevelsTargetSizeMultiplier     int     `yaml:"levels_target_size_multiplier"`
	MaxLevels                      int     `yaml:"max_levels"`
	CompactionFallbackStrategy     string  `yaml:"compaction_fallback_strategy"`
	BloomFilterFalsePositiveRate   float64 `yaml:"bloom_filter_false_positive_rate"`
	SSTableDefaultBlockSizeBytes   int64   `yaml:"sstable_default_block_size_bytes"`
	CompactionIntervalSeconds      int     `yaml:"compaction_interval_seconds"`
	CheckpointIntervalSeconds      int     `yaml:"checkpoint_interval_seconds"`
	MetadataSyncIntervalSeconds    int     `yaml:"metadata_sync_interval_seconds"`
	IndexMemtableThreshold         int64   `yaml:"index_memtable_threshold"`
	IndexFlushIntervalMs           int     `yaml:"index_flush_interval_ms"`
	IndexCompactionIntervalSeconds int     `yaml:"index_compaction_interval_seconds"`
	IndexMaxL0Files                int     `yaml:"index_max_l0_files"`
	IndexBaseTargetSizeBytes       int64   `yaml:"index_base_target_size_bytes"`
	WALSyncMode                    string  `yaml:"wal_sync_mode"`
	WALBatchSize                   int     `yaml:"wal_batch_size"`
	WALFlushIntervalMs             int     `yaml:"wal_flush_interval_ms"`
	WALMaxSegmentSize              int64   `yaml:"wal_max_segment_size"`
	WALPurgeKeepSegments           int     `yaml:"wal_purge_keep_segments"`
	SSTableCompression             string  `yaml:"sstable_compression"`
	RetentionPeriod                string  `yaml:"retention_period"`
}

// LoggingConfig holds logging-specific configurations.
type LoggingConfig struct {
	Level  string `yaml:"level"`  // e.g., "debug", "info", "warn", "error"
	Output string `yaml:"output"` // e.g., "stdout", "file", "none"
	File   string `yaml:"file"`   // Path to the log file, used if output is "file"
}

// SecurityConfig holds security-related configurations like auth.
type SecurityConfig struct {
	Enabled      bool   `yaml:"enabled"`
	UserFilePath string `yaml:"user_file_path"`
}

// ServerConfig holds server-specific configurations.
type ServerConfig struct {
	GRPCPort int `yaml:"grpc_port"`
	// Port for the plain TCP server (NBQL).
	TCPPort                    int    `yaml:"tcp_port"`
	TLSEnabled                 bool   `yaml:"tls_enabled"`
	HealthCheckIntervalSeconds int    `yaml:"health_check_interval_seconds"`
	CertFile                   string `yaml:"cert_file"`
	KeyFile                    string `yaml:"key_file"`
}

type QueryServerConfig struct {
	Enabled       bool   `yaml:"enabled"`
	ListenAddress string `yaml:"listen_address"`
}

type DebugMode struct {
	Enabled          bool   `yaml:"enabled"`
	ListenAddress    string `yaml:"listen_address"`
	EnabledPProf     bool   `yaml:"enabled_pprof"`
	EnabledMetrics   bool   `yaml:"enabled_metrics"`
	EnabledTracing   bool   `yaml:"enabled_tracing"`
	EnabledProfiling bool   `yaml:"enabled_profiling"`
	EnabledMonitorUI bool   `yaml:"enabled_monitor_ui"`
}

type SelfMonitoringConfig struct {
	Enabled         bool   `yaml:"enabled"`
	IntervalSeconds int    `yaml:"interval_seconds"`
	MetricPrefix    string `yaml:"metric_prefix"`
}

// TracingConfig holds configuration for distributed tracing.
type TracingConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Endpoint string `yaml:"endpoint"` // e.g., "localhost:4317" for gRPC OTLP collector
	Protocol string `yaml:"protocol"` // "grpc" or "http"
}

// Config is the top-level configuration struct.
type Config struct {
	Server         ServerConfig         `yaml:"server"`
	DebugMode      DebugMode            `yaml:"debug_mode"`
	Engine         EngineConfig         `yaml:"engine"`
	Logging        LoggingConfig        `yaml:"logging"`
	Security       SecurityConfig       `yaml:"security"`
	SelfMonitoring SelfMonitoringConfig `yaml:"self_monitoring"`
	Tracing        TracingConfig        `yaml:"tracing"`
	QueryServer    QueryServerConfig    `yaml:"query_server"`
}

// Load reads configuration from an io.Reader.
// This is the core logic, separated for testability.
func Load(r io.Reader) (*Config, error) {
	// Set default values
	cfg := &Config{
		Server: ServerConfig{
			GRPCPort:                   50051,
			TCPPort:                    50052, // Default NBQL port
			TLSEnabled:                 false, // Default to disabled for ease of local testing
			HealthCheckIntervalSeconds: 5,     // Default to 5 seconds
			CertFile:                   "certs/server.crt",
			KeyFile:                    "certs/server.key",
		},
		DebugMode: DebugMode{
			Enabled:          false,
			ListenAddress:    ":8080",
			EnabledPProf:     false,
			EnabledMetrics:   false,
			EnabledTracing:   false,
			EnabledProfiling: false,
			EnabledMonitorUI: false, // Default to disabled
		},
		Engine: EngineConfig{
			DataDir:                        "./data",
			MemtableThresholdBytes:         4 * 1024 * 1024, // 4 MiB
			MemtableFlushIntervalMs:        0,
			BlockCacheCapacity:             1024,
			L0CompactionTriggerSizeBytes:   16 * 1024 * 1024, // 16 MiB
			MaxL0Files:                     4,
			TargetSSTableSizeBytes:         16 * 1024 * 1024, // 16 MiB
			LevelsTargetSizeMultiplier:     5,
			MaxLevels:                      7,
			CompactionFallbackStrategy:     "PickOldest",
			BloomFilterFalsePositiveRate:   0.01,
			SSTableDefaultBlockSizeBytes:   8 * 1024, // 8 KiB
			CompactionIntervalSeconds:      120,
			CheckpointIntervalSeconds:      300,
			MetadataSyncIntervalSeconds:    60,
			IndexMemtableThreshold:         10000,
			IndexFlushIntervalMs:           60000,
			IndexCompactionIntervalSeconds: 20,
			IndexMaxL0Files:                4,
			IndexBaseTargetSizeBytes:       2 * 1024 * 1024, // 2 MiB
			WALSyncMode:                    "interval",
			WALBatchSize:                   1,
			WALFlushIntervalMs:             1000,
			WALMaxSegmentSize:              33554432,
			WALPurgeKeepSegments:           4,
			SSTableCompression:             "snappy",
			RetentionPeriod:                "", // Default to no retention
		},
		Logging: LoggingConfig{
			Level:  "info",
			Output: "stdout",
			File:   "nexusbase.log",
		},
		Security: SecurityConfig{
			Enabled:      false, // Default to disabled for ease of use
			UserFilePath: "users.db",
		},
		SelfMonitoring: SelfMonitoringConfig{
			Enabled:         true,
			IntervalSeconds: 15,
			MetricPrefix:    "__",
		},
		QueryServer: QueryServerConfig{
			Enabled:       false,
			ListenAddress: ":8088",
		},
		Tracing: TracingConfig{
			Enabled:  false, // Disabled by default
			Endpoint: "localhost:4317",
			Protocol: "grpc",
		},
	}

	// If the reader is nil, it's like an empty file, return defaults.
	if r == nil {
		return cfg, nil
	}

	// Read all data from the reader
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read config data: %w", err)
	}

	// If data is empty, return defaults.
	if len(data) == 0 {
		return cfg, nil
	}

	// Unmarshal YAML into the config struct, overwriting defaults
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config yaml: %w", err)
	}

	return cfg, nil
}

// LoadConfig reads configuration from a YAML file by path.
func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// If file doesn't exist, return default config by calling Load with a nil reader.
			return Load(nil)
		}
		return nil, fmt.Errorf("failed to open config file %s: %w", path, err)
	}
	defer file.Close()

	return Load(file)
}
