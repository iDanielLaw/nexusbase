package nbql

import (
	"context"
	"errors"
	"testing"

	"github.com/INLOpen/nexusbase/engine"
	corenbql "github.com/INLOpen/nexuscore/nbql"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/require"
)

func TestExecutor_SnapshotRestore(t *testing.T) {
	ctx := context.Background()

	t.Run("executeSnapshot success", func(t *testing.T) {
		mockEngine := new(engine.MockStorageEngine)
		executor := NewExecutor(mockEngine, clock.SystemClockDefault)
		cmd := &corenbql.SnapshotStatement{}
		expectedPath := "/var/data/nexus/snapshots/snap-123.nbb"

		mockEngine.On("CreateSnapshot", ctx).Return(expectedPath, nil).Once()

		result, err := executor.Execute(ctx, cmd)
		require.NoError(t, err)
		resMap, ok := result.(map[string]interface{})
		require.True(t, ok, "Result should be a map")
		require.Equal(t, "OK", resMap["status"])
		require.Equal(t, expectedPath, resMap["path"])
		mockEngine.AssertExpectations(t)
	})

	t.Run("executeSnapshot engine error", func(t *testing.T) {
		mockEngine := new(engine.MockStorageEngine)
		executor := NewExecutor(mockEngine, clock.SystemClockDefault)
		cmd := &corenbql.SnapshotStatement{}
		expectedError := errors.New("disk is full")

		mockEngine.On("CreateSnapshot", ctx).Return("", expectedError).Once()

		_, err := executor.Execute(ctx, cmd)
		require.Error(t, err)
		require.ErrorContains(t, err, expectedError.Error())
		mockEngine.AssertExpectations(t)
	})

	t.Run("executeRestore success", func(t *testing.T) {
		mockEngine := new(engine.MockStorageEngine)
		executor := NewExecutor(mockEngine, clock.SystemClockDefault)
		cmd := &corenbql.RestoreStatement{Path: "/path/to/restore.nbb", Overwrite: true}

		mockEngine.On("RestoreFromSnapshot", ctx, cmd.Path, cmd.Overwrite).Return(nil).Once()

		_, err := executor.Execute(ctx, cmd)
		require.NoError(t, err)
		mockEngine.AssertExpectations(t)
	})
}
