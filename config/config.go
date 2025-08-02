package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// EngineConfig mirrors engine.StorageEngineOptions for YAML unmarshaling.
type EngineConfig struct {
	DataDir                        string  `yaml:"data_dir"`
	MemtableThresholdMB            int64   `yaml:"memtable_threshold_mb"` // Deprecated: use memtable_threshold_bytes
	MemtableFlushIntervalMs        int     `yaml:"memtable_flush_interval_ms"`
	BlockCacheCapacity             int     `yaml:"block_cache_capacity"`
	L0CompactionTriggerSizeMB      int64   `yaml:"l0_compaction_trigger_size_mb"` // New: Size-based trigger for L0 compaction
	MaxL0Files                     int     `yaml:"max_l0_files"`
	TargetSSTableSizeMB            int64   `yaml:"target_sstable_size_mb"`
	LevelsTargetSizeMultiplier     int     `yaml:"levels_target_size_multiplier"`
	MaxLevels                      int     `yaml:"max_levels"`
	BloomFilterFalsePositiveRate   float64 `yaml:"bloom_filter_false_positive_rate"`
	SSTableDefaultBlockSizeKB      int     `yaml:"sstable_default_block_size_kb"`
	CompactionIntervalSeconds      int     `yaml:"compaction_interval_seconds"`
	CheckpointIntervalSeconds      int     `yaml:"checkpoint_interval_seconds"` // New
	MetadataSyncIntervalSeconds    int     `yaml:"metadata_sync_interval_seconds"`
	IndexMemtableThreshold         int64   `yaml:"index_memtable_threshold"`
	IndexFlushIntervalMs           int     `yaml:"index_flush_interval_ms"`
	IndexCompactionIntervalSeconds int     `yaml:"index_compaction_interval_seconds"`
	IndexMaxL0Files                int     `yaml:"index_max_l0_files"`
	WALSyncMode                    string  `yaml:"wal_sync_mode"`
	WALBatchSize                   int     `yaml:"wal_batch_size"`
	WALFlushIntervalMs             int     `yaml:"wal_flush_interval_ms"`
	WALMaxSegmentSize              int64   `yaml:"wal_max_segment_size"`
	WALPurgeKeepSegments           int     `yaml:"wal_purge_keep_segments"` // New
	SSTableCompression             string  `yaml:"sstable_compression"`     // "none" or "snappy"
	RetentionPeriod                string  `yaml:"retention_period"`        // e.g., "30d", "1y". If empty, no retention.
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

// LoadConfig reads configuration from a YAML file.
func LoadConfig(path string) (*Config, error) {
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
			MemtableThresholdMB:            4,
			MemtableFlushIntervalMs:        0,
			BlockCacheCapacity:             100,
			L0CompactionTriggerSizeMB:      16, // Default to 16MB
			MaxL0Files:                     4,
			TargetSSTableSizeMB:            4,
			LevelsTargetSizeMultiplier:     5,
			MaxLevels:                      7,
			BloomFilterFalsePositiveRate:   0.01,
			SSTableDefaultBlockSizeKB:      4,
			CompactionIntervalSeconds:      10,
			CheckpointIntervalSeconds:      300, // Default to 5 minutes
			MetadataSyncIntervalSeconds:    60,
			IndexMemtableThreshold:         10000, // Default to 10,000 unique tag pairs
			IndexFlushIntervalMs:           60000, // Default to 60 seconds
			IndexCompactionIntervalSeconds: 20,    // Default to 20 seconds
			IndexMaxL0Files:                4,     // Default to 4 L0 index files
			WALSyncMode:                    "always",
			WALBatchSize:                   1,
			WALFlushIntervalMs:             0,
			WALMaxSegmentSize:              0,
			WALPurgeKeepSegments:           2, // Default to keeping 2 segments as a safety buffer
			SSTableCompression:             "none",
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

	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		// If file doesn't exist, return default config
		return cfg, nil
	}

	// Unmarshal YAML into the config struct, overwriting defaults
	return cfg, yaml.Unmarshal(data, cfg)
}
