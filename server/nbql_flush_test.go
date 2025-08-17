package server

import (
	"context"
	"errors"
	"testing"

	"github.com/INLOpen/nexusbase/api/nbql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	corenbql "github.com/INLOpen/nexuscore/nbql"
)

func TestExecutor_ExecuteFlush_All(t *testing.T) {
	t.Run("Successful FLUSH ALL", func(t *testing.T) {
		mockEngine := new(MockStorageEngine)
		executor := nbql.NewExecutor(mockEngine, nil)
		flushCmd, err := corenbql.Parse("FLUSH ALL")
		require.NoError(t, err)
		// Expect a call to ForceFlush with wait=true
		mockEngine.On("ForceFlush", mock.Anything, true).Return(nil).Once()

		resp, err := executor.Execute(context.Background(), flushCmd)
		assert.NoError(t, err)

		_, ok := resp.(nbql.ManipulateResponse)
		assert.True(t, ok)
		// assert.Equal(t, "Memtable flushed to disk successfully.", manipulateResp.Message)

		mockEngine.AssertExpectations(t)
	})

	t.Run("Successful FLUSH (default to ALL)", func(t *testing.T) {
		mockEngine := new(MockStorageEngine)
		executor := nbql.NewExecutor(mockEngine, nil)
		flushCmd, err := corenbql.Parse("FLUSH")
		require.NoError(t, err)

		// Expect a call to ForceFlush with wait=true
		mockEngine.On("ForceFlush", mock.Anything, true).Return(nil).Once()

		resp, err := executor.Execute(context.Background(), flushCmd)
		assert.NoError(t, err)

		_, ok := resp.(nbql.ManipulateResponse)
		assert.True(t, ok)
		// assert.Equal(t, "Memtable flushed to disk successfully.", manipulateResp.Message)

		mockEngine.AssertExpectations(t)
	})
}

func TestExecutor_ExecuteFlush_Memtable(t *testing.T) {
	t.Run("Successful FLUSH MEMTABLE", func(t *testing.T) {
		mockEngine := new(MockStorageEngine)
		executor := nbql.NewExecutor(mockEngine, nil)
		flushCmd, err := corenbql.Parse("FLUSH MEMTABLE")
		require.NoError(t, err)

		// Expect a call to ForceFlush with wait=false
		mockEngine.On("ForceFlush", mock.Anything, false).Return(nil).Once()
		resp, err := executor.Execute(context.Background(), flushCmd)
		assert.NoError(t, err)
		_, ok := resp.(nbql.ManipulateResponse)
		assert.True(t, ok)
		// assert.Equal(t, "Memtable flush to immutable list triggered.", manipulateResp.Message)
		mockEngine.AssertExpectations(t)
	})
	t.Run("Failed FLUSH MEMTABLE", func(t *testing.T) {
		mockEngine := new(MockStorageEngine)
		executor := nbql.NewExecutor(mockEngine, nil)
		flushCmd, err := corenbql.Parse("FLUSH MEMTABLE")
		require.NoError(t, err)

		expectedErr := errors.New("flush already in progress")
		mockEngine.On("ForceFlush", mock.Anything, false).Return(expectedErr).Once()

		_, err = executor.Execute(context.Background(), flushCmd)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "flush already in progress")

		mockEngine.AssertExpectations(t)
	})
}

func TestExecutor_ExecuteFlush_Disk(t *testing.T) {
	mockEngine := new(MockStorageEngine)
	executor := nbql.NewExecutor(mockEngine, nil)
	flushCmd, err := corenbql.Parse("FLUSH DISK")
	require.NoError(t, err)

	mockEngine.On("TriggerCompaction").Return().Once()

	resp, err := executor.Execute(context.Background(), flushCmd)
	assert.NoError(t, err)
	_, ok := resp.(nbql.ManipulateResponse)
	assert.True(t, ok)
	// assert.Equal(t, "Compaction check triggered.", manipulateResp.Message)

	mockEngine.AssertExpectations(t)
}
