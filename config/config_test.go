package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_ValidConfig(t *testing.T) {
	yamlContent := `
server:
  grpc_port: 9999
engine:
  data_dir: "/tmp/test_data"
  memtable_threshold_bytes: 8388608 # 8 MiB
`
	reader := strings.NewReader(yamlContent)
	cfg, err := Load(reader)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, 9999, cfg.Server.GRPCPort)
	assert.Equal(t, "/tmp/test_data", cfg.Engine.DataDir)
	assert.Equal(t, int64(8388608), cfg.Engine.MemtableThresholdBytes)
	// Check a default value that was not overridden
	assert.Equal(t, 4, cfg.Engine.MaxL0Files)
}

func TestLoad_PartialConfig(t *testing.T) {
	yamlContent := `
engine:
  max_levels: 5
`
	reader := strings.NewReader(yamlContent)
	cfg, err := Load(reader)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// Check overridden value
	assert.Equal(t, 5, cfg.Engine.MaxLevels)
	// Check default values are still there
	assert.Equal(t, 50051, cfg.Server.GRPCPort)
	assert.Equal(t, "./data", cfg.Engine.DataDir)
}

func TestLoad_EmptyReader(t *testing.T) {
	// Test with nil reader
	cfg, err := Load(nil)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, 50051, cfg.Server.GRPCPort) // Check a default value

	// Test with empty string reader
	reader := strings.NewReader("")
	cfg, err = Load(reader)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, 50051, cfg.Server.GRPCPort) // Check a default value
}

func TestLoad_InvalidYAML(t *testing.T) {
	yamlContent := `
server:
  grpc_port: 9999
engine:
  data_dir: "/tmp/test_data"
  memtable_threshold_bytes: 8388608
  this: is: invalid: yaml
`
	reader := strings.NewReader(yamlContent)
	_, err := Load(reader)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal config yaml")
}

// TestLoadConfig_FileIntegration is a small integration test to ensure
// the original LoadConfig function still works correctly with the filesystem.
func TestLoadConfig_FileIntegration(t *testing.T) {
	t.Run("FileExists", func(t *testing.T) {
		yamlContent := `
server:
  grpc_port: 12345
`
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "config.yaml")
		err := os.WriteFile(configPath, []byte(yamlContent), 0644)
		require.NoError(t, err)

		cfg, err := LoadConfig(configPath)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, 12345, cfg.Server.GRPCPort)
	})

	t.Run("FileDoesNotExist", func(t *testing.T) {
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "non_existent_config.yaml")

		cfg, err := LoadConfig(configPath)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		// Should return default value
		assert.Equal(t, 50051, cfg.Server.GRPCPort)
	})
}
