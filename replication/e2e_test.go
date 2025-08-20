package replication

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
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

	// 1. Start the follower. It will connect and initiate the bootstrap process.
	require.NoError(t, follower.Start())

	// 2. Put some data on the leader.
	// This data will be part of the initial snapshot.
	for i := 0; i < 10; i++ {
		dp := mustNewDataPoint(t, "initial.data", map[string]string{"id": fmt.Sprintf("pre-start-%d", i)}, int64(i+1), map[string]interface{}{"value": float64(i+1)})
		// This write is synchronous and will now wait for any running followers to acknowledge.
		require.NoError(t, leaderEngine.PutBatch(ctx, []core.DataPoint{dp}), "PutBatch should succeed with a running follower")
	}
	// Force a flush to create an SSTable on the leader
	require.NoError(t, leaderEngine.ForceFlush(ctx, true))
	leaderInitialSeq := leaderEngine.GetSequenceNumber()
	require.Greater(t, leaderInitialSeq, uint64(0), "Leader should have a non-zero sequence number after initial writes")

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
	dpLive := mustNewDataPoint(t, "live.metric", map[string]string{"status": "streaming"}, 1000, map[string]interface{}{"value": 123.0})
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

	// --- 2. Start follower and wait for bootstrap ---
	require.NoError(t, follower.Start())

	// Now write the initial data, which will be replicated synchronously.
	require.NoError(t, leaderEngine.PutBatch(ctx, []core.DataPoint{dpA1, dpA2, dpB1, dpC1}))
	require.NoError(t, leaderEngine.ForceFlush(ctx, true))
	leaderInitialSeq := leaderEngine.GetSequenceNumber()

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

func TestReplication_E2E_BootstrapFromSnapshot_WhenWALIsPurged(t *testing.T) {
	// This test simulates a follower that is so far behind that the leader has
	// already purged the WAL segments it needs. This forces the follower to
	// bootstrap using a full snapshot.

	// 1. Setup: Use a custom setup to enable frequent checkpointing and purging.
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
		},
		Logging: config.LoggingConfig{Level: "error", Output: "none"},
	}

	leaderLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", "leader")
	leaderEngineOpts := engine.DefaultStorageEngineOptions()
	leaderEngineOpts.DataDir = leaderDir
	leaderEngineOpts.Logger = leaderLogger
	leaderEngineOpts.WALMaxSegmentSize = 1024
	leaderEngineOpts.WALPurgeKeepSegments = 2
	leaderEngineOpts.CheckpointIntervalSeconds = 1 // <-- Key change for this test

	leaderEngine, err := engine.NewStorageEngine(leaderEngineOpts)
	require.NoError(t, err)
	require.NoError(t, leaderEngine.Start())

	leaderServer, err := server.NewAppServer(leaderEngine, leaderCfg, leaderLogger)
	require.NoError(t, err)

	leaderErrChan := make(chan error, 1)
	go func() {
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

	leaderReplicationAddr := fmt.Sprintf("127.0.0.1:%d", leaderReplicationPort)
	// Set bootstrap threshold low to ensure it triggers if lag is detected.
	follower, err := NewFollower(leaderReplicationAddr, 1, followerEngineOpts, followerLogger)
	require.NoError(t, err)

	cleanup := func() {
		follower.Stop()
		leaderServer.Stop()
		select {
		case err := <-leaderErrChan:
			require.NoError(t, err, "Leader server exited with an unexpected error")
		case <-time.After(2 * time.Second):
			t.Log("Timed out waiting for leader server to stop")
		}
		leaderEngine.Close()
	}
	defer cleanup()

	ctx := context.Background()

	// 2. Populate leader with enough data to rotate the WAL several times.
	t.Log("Populating leader to force WAL rotation...")
	for i := 0; i < 60; i++ {
		dp := mustNewDataPoint(t, "initial.data.for.purge", map[string]string{"id": fmt.Sprintf("purge-test-%d", i)}, int64(i+1), map[string]interface{}{"value": "some-long-string-to-fill-up-the-wal-segment-file-quickly"})
		require.NoError(t, leaderEngine.Put(ctx, dp))
	}
	require.NoError(t, leaderEngine.ForceFlush(ctx, true))

	// 3. Wait for checkpointing to run and purge old WALs.
	t.Log("Waiting for checkpoint and WAL purge...")
	time.Sleep(2 * time.Second) // Wait for at least one checkpoint cycle

	// Optional: Verify that the first WAL segment is actually gone.
	walDir := filepath.Join(leaderDir, "wal")
	firstSegmentPath := filepath.Join(walDir, core.FormatSegmentFileName(1))
	_, err = os.Stat(firstSegmentPath)
	require.ErrorIs(t, err, os.ErrNotExist, "WAL segment 1 should have been purged by the leader")
	t.Log("Verified that initial WAL segment has been purged.")

	leaderSeqNumBeforeFollowerStart := leaderEngine.GetSequenceNumber()

	// 4. Start the follower. It should detect it's too far behind (or its needed
	// WAL is gone) and initiate a snapshot bootstrap.
	t.Log("Starting follower, expecting it to bootstrap from snapshot...")
	require.NoError(t, follower.Start())

	// 5. Wait for the follower to finish bootstrapping and catch up.
	require.Eventually(t, func() bool {
		follower.mu.RLock()
		defer follower.mu.RUnlock()
		if follower.engine == nil {
			return false
		}
		return follower.engine.GetSequenceNumber() >= leaderSeqNumBeforeFollowerStart
	}, 10*time.Second, 200*time.Millisecond, "Follower did not bootstrap from snapshot and catch up in time")

	t.Log("Follower has successfully bootstrapped from snapshot.")

	// 6. Verify data integrity on the follower.
	_, err = follower.engine.Get(ctx, "initial.data.for.purge", map[string]string{"id": "purge-test-59"}, 59+1)
	require.NoError(t, err, "Follower should have data from the snapshot")

	// 7. Put live data on the leader to ensure WAL streaming works *after* bootstrap.
	t.Log("Writing live data to leader post-bootstrap...")
	liveDp := mustNewDataPoint(t, "live.data.after.snapshot", map[string]string{"status": "streaming"}, 1000, map[string]interface{}{"value": 999.0})
	require.NoError(t, leaderEngine.Put(ctx, liveDp))

	// 8. Wait for the live data to be replicated via the now-active WAL stream.
	require.Eventually(t, func() bool {
		follower.mu.RLock()
		defer follower.mu.RUnlock()
		if follower.engine == nil {
			return false
		}
		_, err := follower.engine.Get(ctx, liveDp.Metric, liveDp.Tags, liveDp.Timestamp)
		return err == nil
	}, 5*time.Second, 100*time.Millisecond, "Follower did not replicate live data via WAL stream after snapshot bootstrap")

	t.Log("Follower successfully replicated live data. E2E snapshot bootstrap test passed.")
}

func TestReplication_E2E_LeaderFailureAndReconnect(t *testing.T) {
	// This test simulates a leader failure and recovery.
	// 1. Leader and follower are running and replicating.
	// 2. Leader server is stopped.
	// 3. Follower enters a retry loop.
	// 4. Leader server is restarted with the same data.
	// 5. Follower reconnects and resumes replication.

	// --- 1. Initial Setup ---
	// We can't use the standard setupE2ETest because we need to control the leader's lifecycle manually.
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
		Engine:  config.EngineConfig{DataDir: leaderDir},
		Logging: config.LoggingConfig{Level: "error", Output: "none"},
	}

	leaderLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", "leader")
	leaderEngineOpts := engine.DefaultStorageEngineOptions()
	leaderEngineOpts.DataDir = leaderDir
	leaderEngineOpts.Logger = leaderLogger

	leaderEngine, err := engine.NewStorageEngine(leaderEngineOpts)
	require.NoError(t, err)
	require.NoError(t, leaderEngine.Start())

	// --- Follower Setup ---
	followerDir := t.TempDir()
	followerLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", "follower")
	followerEngineOpts := engine.DefaultStorageEngineOptions()
	followerEngineOpts.DataDir = followerDir
	followerEngineOpts.Logger = followerLogger

	leaderReplicationAddr := fmt.Sprintf("127.0.0.1:%d", leaderReplicationPort)
	follower, err := NewFollower(leaderReplicationAddr, 1_000_000, followerEngineOpts, followerLogger)
	require.NoError(t, err)

	// --- Cleanup ---
	defer func() {
		follower.Stop()
		leaderEngine.Close()
	}()

	// --- 2. Start initial leader and follower ---
	leaderServer, err := server.NewAppServer(leaderEngine, leaderCfg, leaderLogger)
	require.NoError(t, err)

	leaderErrChan := make(chan error, 1)
	go func() {
		if err := leaderServer.Start(); err != nil {
			leaderErrChan <- err
		}
	}()

	require.NoError(t, follower.Start())

	// --- 3. Write initial data and wait for sync ---
	ctx := context.Background()
	dpInitial := mustNewDataPoint(t, "initial.metric", map[string]string{"state": "before_failure"}, 1, map[string]interface{}{"value": 1.0})
	require.NoError(t, leaderEngine.Put(ctx, dpInitial))

	require.Eventually(t, func() bool {
		follower.mu.RLock()
		defer follower.mu.RUnlock()
		if follower.engine == nil {
			return false
		}
		_, err := follower.engine.Get(ctx, dpInitial.Metric, dpInitial.Tags, dpInitial.Timestamp)
		return err == nil
	}, 5*time.Second, 100*time.Millisecond, "Follower did not sync initial data")
	t.Log("Initial data synced successfully.")

	// --- 4. Simulate Leader Failure ---
	t.Log("Stopping leader server to simulate failure...")
	leaderServer.Stop()
	// Wait for the server goroutine to exit
	select {
	case err := <-leaderErrChan:
		require.NoError(t, err, "Leader server should stop gracefully")
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for leader server to stop")
	}
	t.Log("Leader server stopped.")

	// Follower should now be in its retry loop.

	// --- 5. Restart Leader ---
	t.Log("Restarting leader server...")
	// Create a new AppServer instance using the *same* engine and config
	restartedLeaderServer, err := server.NewAppServer(leaderEngine, leaderCfg, leaderLogger)
	require.NoError(t, err)

	restartedLeaderErrChan := make(chan error, 1)
	go func() {
		if err := restartedLeaderServer.Start(); err != nil {
			restartedLeaderErrChan <- err
		}
	}()
	defer func() {
		restartedLeaderServer.Stop()
		select {
		case err := <-restartedLeaderErrChan:
			require.NoError(t, err, "Restarted leader server should stop gracefully")
		case <-time.After(2 * time.Second):
			t.Log("Timed out waiting for restarted leader server to stop")
		}
	}()

	// --- 6. Write new data to restarted leader ---
	t.Log("Writing new data to restarted leader...")
	dpAfterRestart := mustNewDataPoint(t, "live.metric", map[string]string{"state": "after_failure"}, 2, map[string]interface{}{"value": 2.0})
	require.NoError(t, leaderEngine.Put(ctx, dpAfterRestart))

	// --- 7. Verify follower reconnects and syncs new data ---
	t.Log("Waiting for follower to reconnect and sync new data...")
	require.Eventually(t, func() bool {
		follower.mu.RLock()
		defer follower.mu.RUnlock()
		if follower.engine == nil {
			return false
		}
		_, err := follower.engine.Get(ctx, dpAfterRestart.Metric, dpAfterRestart.Tags, dpAfterRestart.Timestamp)
		return err == nil
	}, 10*time.Second, 200*time.Millisecond, "Follower did not reconnect and sync data after leader restart")

	t.Log("Follower reconnected and synced new data successfully.")

	// --- 8. Final state check ---
	leaderSeq := leaderEngine.GetSequenceNumber()
	followerSeq := follower.engine.GetSequenceNumber()
	assert.Equal(t, leaderSeq, followerSeq, "Final sequence numbers should match")
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
