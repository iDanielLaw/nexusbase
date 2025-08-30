package config

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// TLSConfig holds TLS-specific configurations.
type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// ServerConfig holds server-specific configurations.
type ServerConfig struct {
	GRPCPort            int       `yaml:"grpc_port"`
	TCPPort             int       `yaml:"tcp_port"`
	HealthCheckInterval string    `yaml:"health_check_interval"`
	TLS                 TLSConfig `yaml:"tls"`
}

// MemtableConfig holds memtable-specific configurations.
type MemtableConfig struct {
	SizeThresholdBytes int64  `yaml:"size_threshold_bytes"`
	FlushInterval      string `yaml:"flush_interval"`
}

// SSTableConfig holds sstable-specific configurations.
type SSTableConfig struct {
	BlockSizeBytes    int64   `yaml:"block_size_bytes"`
	Compression       string  `yaml:"compression"`
	BloomFilterFPRate float64 `yaml:"bloom_filter_fp_rate"`
}

// CacheConfig holds cache-specific configurations.
type CacheConfig struct {
	BlockCacheCapacity int `yaml:"block_cache_capacity"`
}

// CompactionConfig holds compaction-specific configurations.
type CompactionConfig struct {
	L0TriggerFileCount     int    `yaml:"l0_trigger_file_count"`
	L0TriggerSizeBytes     int64  `yaml:"l0_trigger_size_bytes"`
	TargetSSTableSizeBytes int64  `yaml:"target_sstable_size_bytes"`
	LevelsSizeMultiplier   int    `yaml:"levels_size_multiplier"`
	MaxLevels              int    `yaml:"max_levels"`
	CheckInterval          string `yaml:"check_interval"`
	FallbackStrategy     string  `yaml:"fallback_strategy"`
	TombstoneWeight      float64 `yaml:"tombstone_weight"`
	OverlapPenaltyWeight float64 `yaml:"overlap_penalty_weight"`
	IntraL0TriggerFileCount int   `yaml:"intra_l0_trigger_file_count"`
	IntraL0MaxFileSizeBytes int64 `yaml:"intra_l0_max_file_size_bytes"`
}
// WALConfig holds Write-Ahead Log specific configurations.
type WALConfig struct {
	SyncMode            string `yaml:"sync_mode"`
	BatchSize           int    `yaml:"batch_size"`
	FlushInterval       string `yaml:"flush_interval"`
	MaxSegmentSizeBytes int64  `yaml:"max_segment_size_bytes"`
	PurgeKeepSegments   int    `yaml:"purge_keep_segments"`
}

// IndexConfig holds tag index specific configurations.
type IndexConfig struct {
	MemtableThreshold       int64  `yaml:"memtable_threshold"`
	FlushInterval           string `yaml:"flush_interval"`
	CompactionCheckInterval string `yaml:"compaction_check_interval"`
	L0TriggerFileCount      int    `yaml:"l0_trigger_file_count"`
	BaseTargetSizeBytes     int64  `yaml:"base_target_size_bytes"`
}

// EngineConfig holds all engine-related configurations, grouped logically.
type EngineConfig struct {
	DataDir              string           `yaml:"data_dir"`
	RetentionPeriod      string           `yaml:"retention_period"`
	MetadataSyncInterval string           `yaml:"metadata_sync_interval"`
	CheckpointInterval   string           `yaml:"checkpoint_interval"` // Added for completeness
	Memtable             MemtableConfig   `yaml:"memtable"`
	SSTable              SSTableConfig    `yaml:"sstable"`
	Cache                CacheConfig      `yaml:"cache"`
	Compaction           CompactionConfig `yaml:"compaction"`
	WAL                  WALConfig        `yaml:"wal"`
	Index                IndexConfig      `yaml:"index"`
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

// DebugConfig holds debugging-related configurations.
type DebugConfig struct {
	Enabled          bool   `yaml:"enabled"`
	ListenAddress    string `yaml:"listen_address"`
	PProfEnabled     bool   `yaml:"pprof_enabled"`
	MetricsEnabled   bool   `yaml:"metrics_enabled"`
	MonitorUIEnabled bool   `yaml:"monitor_ui_enabled"`
}

type QueryServerConfig struct {
	Enabled       bool   `yaml:"enabled"`
	ListenAddress string `yaml:"listen_address"`
}

type SelfMonitoringConfig struct {
	Enabled      bool   `yaml:"enabled"`
	Interval     string `yaml:"interval"`
	MetricPrefix string `yaml:"metric_prefix"`
}

// TracingConfig holds configuration for distributed tracing.
type TracingConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Endpoint string `yaml:"endpoint"` // e.g., "localhost:4317" for gRPC OTLP collector
	Protocol string `yaml:"protocol"` // "grpc" or "http"
}

// ReplicationConfig holds the configuration for the replication system.
type ReplicationConfig struct {
	Mode          string   `yaml:"mode"`           // "leader", "follower", or "disabled"
	ListenAddress string   `yaml:"listen_address"` // Address for this node's gRPC service
	Followers     []string `yaml:"followers"`      // List of follower addresses (for leader)
	LeaderAddress string   `yaml:"leader_address"` // Address of the leader (for follower)
}

// Config is the top-level configuration struct.
type Config struct {
	Server         ServerConfig         `yaml:"server"`
	Debug          DebugConfig          `yaml:"debug"`
	Engine         EngineConfig         `yaml:"engine"`
	Logging        LoggingConfig        `yaml:"logging"`
	Security       SecurityConfig       `yaml:"security"`
	SelfMonitoring SelfMonitoringConfig `yaml:"self_monitoring"`
	Tracing        TracingConfig        `yaml:"tracing"`
	QueryServer    QueryServerConfig    `yaml:"query_server"`
	Replication    ReplicationConfig    `yaml:"replication"`
}

// ParseDuration parses a duration string. Returns the default duration if the string is empty or invalid.
// Logs a warning if the string is invalid but not empty.
func ParseDuration(durationStr string, defaultDuration time.Duration, logger *slog.Logger) time.Duration {
	if durationStr == "" || durationStr == "0" {
		return defaultDuration
	}
	d, err := time.ParseDuration(durationStr)
	if err != nil {
		if logger != nil {
			logger.Warn("Invalid duration format, using default", "input", durationStr, "default", defaultDuration.String(), "error", err)
		}
		return defaultDuration
	}
	return d
}

// Load reads configuration from an io.Reader.
// This is the core logic, separated for testability.
func Load(r io.Reader) (*Config, error) {
	// Set default values
	cfg := &Config{
		Server: ServerConfig{
			GRPCPort:            50051,
			TCPPort:             50052,
			HealthCheckInterval: "5s",
			TLS: TLSConfig{
				Enabled:  false,
				CertFile: "certs/server.crt",
				KeyFile:  "certs/server.key",
			},
		},
		Engine: EngineConfig{
			DataDir:              "./data",
			RetentionPeriod:      "",
			MetadataSyncInterval: "60s",
			CheckpointInterval:   "300s", // Default to 5 minutes
			Memtable: MemtableConfig{
				SizeThresholdBytes: 4 * 1024 * 1024, // 4 MiB
				FlushInterval:      "1s",
			},
			SSTable: SSTableConfig{
				BlockSizeBytes:    8 * 1024, // 8 KiB
				Compression:       "snappy",
				BloomFilterFPRate: 0.01,
			},
			Cache: CacheConfig{
				BlockCacheCapacity: 1024,
			},
			Compaction: CompactionConfig{
				L0TriggerFileCount:     4,
				L0TriggerSizeBytes:     16 * 1024 * 1024, // 16 MiB
				TargetSSTableSizeBytes: 16 * 1024 * 1024, // 16 MiB
				LevelsSizeMultiplier:   5,
				MaxLevels:              7,
				CheckInterval:          "120s",
				FallbackStrategy:         "PickOldest",
				TombstoneWeight:          1.5, // Default weight for tombstone priority
				OverlapPenaltyWeight:     1.0, // Default weight for overlap penalty
				IntraL0TriggerFileCount: 8, // Default to triggering with 8 small files
				IntraL0MaxFileSizeBytes: 2 * 1024 * 1024, // Default to 2MB for "small" files
			},
			WAL: WALConfig{
				SyncMode:            "interval",
				BatchSize:           1,
				FlushInterval:       "1000ms",
				MaxSegmentSizeBytes: 32 * 1024 * 1024, // 32 MiB
				PurgeKeepSegments:   4,
			},
			Index: IndexConfig{
				MemtableThreshold:       10000,
				FlushInterval:           "60s",
				CompactionCheckInterval: "20s",
				L0TriggerFileCount:      4,
				BaseTargetSizeBytes:     2 * 1024 * 1024, // 2 MiB
			},
		},
		Logging: LoggingConfig{
			Level:  "debug",
			Output: "stdout",
			File:   "nexusbase.log",
		},
		Security: SecurityConfig{
			Enabled:      false,
			UserFilePath: "users.db",
		},
		SelfMonitoring: SelfMonitoringConfig{
			Enabled:      true,
			Interval:     "15s",
			MetricPrefix: "__",
		},
		QueryServer: QueryServerConfig{
			Enabled:       true,
			ListenAddress: ":8088",
		},
		Tracing: TracingConfig{
			Enabled:  false,
			Endpoint: "localhost:4317",
			Protocol: "grpc",
		},
		Debug: DebugConfig{
			Enabled:          true,
			ListenAddress:    "0.0.0.0:6060",
			PProfEnabled:     true,
			MetricsEnabled:   true,
			MonitorUIEnabled: true,
		},
		Replication: ReplicationConfig{
			Mode:          "disabled",
			ListenAddress: ":50052",
			Followers:     nil,
			LeaderAddress: "",
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