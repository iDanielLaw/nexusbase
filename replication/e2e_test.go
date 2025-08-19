package replication

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexusbase/server"
)

// setupE2ETest sets up a full Leader-Follower environment for end-to-end testing.
// It returns the leader's AppServer, the follower instance, and a cleanup function.
func setupE2ETest(t *testing.T) (leaderServer *server.AppServer, follower *Follower, cleanup func()) {
	t.Helper()

	// --- Leader Setup ---
	leaderDir := t.TempDir()
	leaderGRPCPort := findFreePort(t)
	leaderReplicationPort := findFreePort(t)

	leaderCfg := &config.Config{
		Server: config.ServerConfig{
			GRPCPort: leaderGRPCPort,
			// This is a placeholder, we need to add ReplicationPort to the config struct.
			// For now, we'll manually adjust the address.
		},
		Engine: config.EngineConfig{
			DataDir: leaderDir,
			// Use small WAL segments to test rotation and purging
			WAL: config.WALConfig{MaxSegmentSizeBytes: 1024, PurgeKeepSegments: 2},
			// Enable synchronous replication for some tests
			ReplicationSyncTimeoutMs: 2000, // 2 second timeout
		},
		Logging: config.LoggingConfig{Level: "error", Output: "none"},
	}
	// This is a hack because the config doesn't support a separate replication port yet.
	// We will manually create the replication listener on the correct port.
	leaderCfg.Server.TCPPort = leaderReplicationPort // Using TCPPort as a stand-in for ReplicationPort

	leaderLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", "leader")
	leaderEngineOpts := engine.DefaultStorageEngineOptions()
	leaderEngineOpts.DataDir = leaderDir
	leaderEngineOpts.Logger = leaderLogger
	leaderEngineOpts.WALMaxSegmentSize = 1024
	leaderEngineOpts.WALPurgeKeepSegments = 2
	leaderEngineOpts.ReplicationSyncTimeoutMs = 2000

	leaderEngine, err := engine.NewStorageEngine(leaderEngineOpts)
	require.NoError(t, err)
	require.NoError(t, leaderEngine.Start())

	// Manually create the AppServer, but we will adjust the listener for replication
	leaderServer, err = server.NewAppServer(leaderEngine, leaderCfg, leaderLogger)
	require.NoError(t, err)

	// Start the leader server in a goroutine
	leaderErrChan := make(chan error, 1)
	go func() {
		// Start() is blocking, so it runs in a goroutine.
		if err := leaderServer.Start(); err != nil {
			leaderErrChan <- err
		}
		close(leaderErrChan)
	}()

	// --- Follower Setup ---
	followerDir := t.TempDir()
	followerLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", "follower")
	followerEngineOpts := engine.DefaultStorageEngineOptions()
	followerEngineOpts.DataDir = followerDir
	followerEngineOpts.Logger = followerLogger

	// The follower connects to the leader's replication port
	leaderReplicationAddr := fmt.Sprintf("127.0.0.1:%d", leaderReplicationPort)
	follower, err = NewFollower(leaderReplicationAddr, 1_000_000, followerEngineOpts, followerLogger)
	require.NoError(t, err)

	cleanup = func() {
		follower.Stop()
		leaderServer.Stop()
		// Wait for leader to shut down
		select {
		case err := <-leaderErrChan:
			require.NoError(t, err, "Leader server exited with an unexpected error")
		case <-time.After(2 * time.Second):
			t.Log("Timed out waiting for leader server to stop")
		}
		leaderEngine.Close()
	}

	return leaderServer, follower, cleanup
}

func TestReplication_E2E_BootstrapAndCatchup(t *testing.T) {
	leaderServer, follower, cleanup := setupE2ETest(t)
	defer cleanup()

	leaderEngine := leaderServer.GetEngine()
	ctx := context.Background()

	// 1. Put some data on the leader BEFORE the follower starts.
	// This data will be part of the initial snapshot.
	for i := 0; i < 10; i++ {
		dp := core.DataPoint{
			Metric:    "bootstrap.metric",
			Tags:      map[string]string{"id": fmt.Sprintf("%d", i)},
			Timestamp: int64(i),
			Fields:    mustNewFieldValues(t, map[string]interface{}{"value": float64(i)}),
		}
		require.NoError(t, leaderEngine.Put(ctx, dp))
	}
	// Force a flush to create an SSTable on the leader
	require.NoError(t, leaderEngine.ForceFlush(ctx, true))

	// 2. Start the follower. It should perform a snapshot bootstrap.
	require.NoError(t, follower.Start())

	// 3. Wait for the follower to catch up.
	// We can verify this by checking if the data from step 1 exists on the follower.
	require.Eventually(t, func() bool {
		follower.mu.RLock()
		defer follower.mu.RUnlock()
		if follower.engine == nil {
			return false
		}
		_, err := follower.engine.Get(ctx, "bootstrap.metric", map[string]string{"id": "9"}, 9)
		return err == nil
	}, 5*time.Second, 100*time.Millisecond, "Follower did not bootstrap data from snapshot in time")

	// 4. Put more data on the leader AFTER the follower has started.
	// This data should be replicated via WAL streaming.
	dpLive := core.DataPoint{
		Metric:    "live.metric",
		Tags:      map[string]string{"status": "streaming"},
		Timestamp: 1000,
		Fields:    mustNewFieldValues(t, map[string]interface{}{"value": 123.0}),
	}
	require.NoError(t, leaderEngine.Put(ctx, dpLive))

	// 5. Wait for the live data to be replicated.
	require.Eventually(t, func() bool {
		follower.mu.RLock()
		defer follower.mu.RUnlock()
		if follower.engine == nil {
			return false
		}
		_, err := follower.engine.Get(ctx, "live.metric", map[string]string{"status": "streaming"}, 1000)
		return err == nil
	}, 5*time.Second, 100*time.Millisecond, "Follower did not replicate live data via WAL stream in time")

	// 6. Verify follower's sequence number matches leader's
	leaderSeq := leaderEngine.GetSequenceNumber()
	followerSeq := follower.engine.GetSequenceNumber()
	assert.Equal(t, leaderSeq, followerSeq, "Follower sequence number should match leader's after catch-up")
}

// Helper to find a free TCP port.
func findFreePort(t *testing.T) int {
	t.Helper()
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	l, err := net.ListenTCP("tcp", addr)
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// Helper to create FieldValues for tests, failing the test on error.
func mustNewFieldValues(t *testing.T, data map[string]interface{}) core.FieldValues {
	t.Helper()
	fv, err := core.NewFieldValuesFromMap(data)
	require.NoError(t, err)
	return fv
}