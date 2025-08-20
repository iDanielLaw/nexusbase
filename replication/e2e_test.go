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
	"github.com/INLOpen/nexusbase/sstable"
)

func mustNewFieldValues(t *testing.T, data map[string]interface{}) core.FieldValues {
	fv, err := core.NewFieldValuesFromMap(data)
	require.NoError(t, err)
	return fv
}

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
			GRPCPort:        leaderGRPCPort,
			ReplicationPort: leaderReplicationPort,
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
			Metric:    "initial.data",
			Tags:      map[string]string{"id": fmt.Sprintf("pre-start-%d", i)},
			Timestamp: int64(i + 1),
			Fields:    mustNewFieldValues(t, map[string]interface{}{"value": float64(i + 1)}),
		}
		// Use PutBatch here as Put is a convenience wrapper.
		require.NoError(t, leaderEngine.PutBatch(ctx, []core.DataPoint{dp}))
	}
	// Force a flush to create an SSTable on the leader
	require.NoError(t, leaderEngine.ForceFlush(ctx, true))
	leaderInitialSeq := leaderEngine.GetSequenceNumber()
	require.Greater(t, leaderInitialSeq, uint64(0), "Leader should have a non-zero sequence number after initial writes")

	// 2. Start the follower. It should perform a snapshot bootstrap.
	require.NoError(t, follower.Start())

	// 3. Wait for the follower to finish bootstrapping and catch up to the leader's initial state.
	require.Eventually(t, func() bool {
		follower.mu.RLock()
		defer follower.mu.RUnlock()
		if follower.engine == nil {
			return false
		}
		// Check that the follower's sequence number has caught up to where the leader was when we started.
		return follower.engine.GetSequenceNumber() >= leaderInitialSeq
	}, 5*time.Second, 100*time.Millisecond, "Follower did not bootstrap and catch up to initial state in time")

	// 4. Put more data on the leader AFTER the follower has started.
	// This data should be replicated via WAL streaming.
	dpLive := core.DataPoint{
		Metric:    "live.metric",
		Tags:      map[string]string{"status": "streaming"},
		Timestamp: 1000,
		Fields:    mustNewFieldValues(t, map[string]interface{}{"value": 123.0}),
	}
	require.NoError(t, leaderEngine.Put(ctx, dpLive), "Put should not fail with a running follower")

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

func TestReplication_E2E_WithDeletes(t *testing.T) {
	leaderServer, follower, cleanup := setupE2ETest(t)
	defer cleanup()

	leaderEngine := leaderServer.GetEngine()
	ctx := context.Background()

	// --- 1. Put initial data on the leader ---
	// Series A: will have a point deleted
	dpA1 := mustNewDataPoint(t, "replication.test", map[string]string{"id": "A"}, 100, map[string]interface{}{"value": 10.0})
	dpA2 := mustNewDataPoint(t, "replication.test", map[string]string{"id": "A"}, 200, map[string]interface{}{"value": 20.0})
	// Series B: will be deleted entirely
	dpB1 := mustNewDataPoint(t, "replication.test", map[string]string{"id": "B"}, 150, map[string]interface{}{"value": 15.0})
	// Series C: will remain untouched, then have a point added
	dpC1 := mustNewDataPoint(t, "replication.test", map[string]string{"id": "C"}, 180, map[string]interface{}{"value": 18.0})

	require.NoError(t, leaderEngine.PutBatch(ctx, []core.DataPoint{dpA1, dpA2, dpB1, dpC1}))
	require.NoError(t, leaderEngine.ForceFlush(ctx, true))
	leaderInitialSeq := leaderEngine.GetSequenceNumber()

	// --- 2. Start follower and wait for bootstrap ---
	require.NoError(t, follower.Start())
	require.Eventually(t, func() bool {
		follower.mu.RLock()
		defer follower.mu.RUnlock()
		return follower.engine != nil && follower.engine.GetSequenceNumber() >= leaderInitialSeq
	}, 5*time.Second, 100*time.Millisecond, "Follower did not bootstrap in time")

	// --- 3. Perform live deletions and a put on the leader ---
	// Point delete on Series A
	require.NoError(t, leaderEngine.Delete(ctx, dpA1.Metric, dpA1.Tags, dpA1.Timestamp))
	// Series delete on Series B
	require.NoError(t, leaderEngine.DeleteSeries(ctx, dpB1.Metric, dpB1.Tags))
	// New point on Series C
	dpC2 := mustNewDataPoint(t, "replication.test", map[string]string{"id": "C"}, 280, map[string]interface{}{"value": 28.0})
	require.NoError(t, leaderEngine.Put(ctx, dpC2))

	finalLeaderSeq := leaderEngine.GetSequenceNumber()

	// --- 4. Wait for follower to catch up to all changes ---
	require.Eventually(t, func() bool {
		follower.mu.RLock()
		defer follower.mu.RUnlock()
		return follower.engine != nil && follower.engine.GetSequenceNumber() >= finalLeaderSeq
	}, 5*time.Second, 100*time.Millisecond, "Follower did not replicate deletions and new put in time")

	followerEngine := follower.engine
	require.NotNil(t, followerEngine)

	// --- 5. Verify state on the follower ---
	// Series A: Point at ts=100 should be gone, point at ts=200 should exist
	_, err := followerEngine.Get(ctx, dpA1.Metric, dpA1.Tags, dpA1.Timestamp)
	assert.ErrorIs(t, err, sstable.ErrNotFound, "Point A1 should be deleted on follower")

	valA2, err := followerEngine.Get(ctx, dpA2.Metric, dpA2.Tags, dpA2.Timestamp)
	require.NoError(t, err, "Point A2 should exist on follower")
	assert.Equal(t, 20.0, mustGetFloat(t, valA2, "value"))

	// Series B: Should be completely gone
	_, err = followerEngine.Get(ctx, dpB1.Metric, dpB1.Tags, dpB1.Timestamp)
	assert.ErrorIs(t, err, sstable.ErrNotFound, "Point B1 should be gone due to series delete")

	// Series C: Should have both old and new points
	valC1, err := followerEngine.Get(ctx, dpC1.Metric, dpC1.Tags, dpC1.Timestamp)
	require.NoError(t, err, "Point C1 should exist on follower")
	assert.Equal(t, 18.0, mustGetFloat(t, valC1, "value"))

	valC2, err := followerEngine.Get(ctx, dpC2.Metric, dpC2.Tags, dpC2.Timestamp)
	require.NoError(t, err, "Point C2 (live put) should exist on follower")
	assert.Equal(t, 28.0, mustGetFloat(t, valC2, "value"))
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
func mustNewDataPoint(t *testing.T, metric string, tags map[string]string, ts int64, fields map[string]interface{}) core.DataPoint {
	t.Helper()
	dp, err := core.NewSimpleDataPoint(metric, tags, ts, fields)
	require.NoError(t, err)
	return *dp
}

func mustGetFloat(t *testing.T, fv core.FieldValues, fieldName string) float64 {
	t.Helper()
	val, ok := fv[fieldName]
	require.True(t, ok, "field '%s' not found", fieldName)
	floatVal, ok := val.ValueFloat64()
	require.True(t, ok, "field '%s' is not a float64", fieldName)
	return floatVal
}
