package checkpoint

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckpoint_WriteAndRead_Successful(t *testing.T) {
	tempDir := t.TempDir()
	cp := Checkpoint{LastSafeSegmentIndex: 123}

	// Write the checkpoint
	err := Write(tempDir, cp)
	require.NoError(t, err, "Write should succeed")

	// Verify the CHECKPOINT file exists and the temp file is gone
	checkpointPath := filepath.Join(tempDir, FileName)
	tempPath := filepath.Join(tempDir, TempFileName)
	_, err = os.Stat(checkpointPath)
	require.NoError(t, err, "CHECKPOINT file should exist after write")
	_, err = os.Stat(tempPath)
	require.True(t, os.IsNotExist(err), "CHECKPOINT.tmp file should not exist after successful write")

	// Read it back
	readCp, found, err := Read(tempDir)
	require.NoError(t, err, "Read should succeed")
	require.True(t, found, "Checkpoint should be found")

	// Verify the content
	assert.Equal(t, cp.LastSafeSegmentIndex, readCp.LastSafeSegmentIndex, "Read LastSafeSegmentIndex should match written value")
}

func TestCheckpoint_Read_NonExistent(t *testing.T) {
	tempDir := t.TempDir()

	// Attempt to read from an empty directory
	cp, found, err := Read(tempDir)
	require.NoError(t, err, "Read from an empty directory should not return an error")
	assert.False(t, found, "found should be false for a non-existent checkpoint")
	assert.Equal(t, uint64(0), cp.LastSafeSegmentIndex, "LastSafeSegmentIndex should be zero for a non-existent checkpoint")
}

func TestCheckpoint_Write_Overwrite(t *testing.T) {
	tempDir := t.TempDir()

	// Write initial checkpoint
	cp1 := Checkpoint{LastSafeSegmentIndex: 10}
	err := Write(tempDir, cp1)
	require.NoError(t, err)

	// Write a new checkpoint to overwrite
	cp2 := Checkpoint{LastSafeSegmentIndex: 20}
	err = Write(tempDir, cp2)
	require.NoError(t, err)

	// Read back and verify it's the new one
	readCp, found, err := Read(tempDir)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, cp2.LastSafeSegmentIndex, readCp.LastSafeSegmentIndex, "Read value should be from the second write")
}

func TestCheckpoint_Read_Corrupted(t *testing.T) {
	tempDir := t.TempDir()
	checkpointPath := filepath.Join(tempDir, FileName)

	t.Run("BadMagicNumber", func(t *testing.T) {
		// Write a file with a wrong magic number
		badData := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		err := os.WriteFile(checkpointPath, badData, 0644)
		require.NoError(t, err)

		_, found, err := Read(tempDir)
		require.Error(t, err, "Read should fail with a bad magic number")
		assert.True(t, found, "found should be true as the file exists")
		assert.Contains(t, err.Error(), "invalid checkpoint magic number")
	})

	t.Run("TruncatedFile", func(t *testing.T) {
		// Write a file that is too short
		truncatedData := []byte{0x43, 0x4B, 0x50, 0x54, 0x01, 0x00} // Magic number + 2 bytes
		err := os.WriteFile(checkpointPath, truncatedData, 0644)
		require.NoError(t, err)

		_, found, err := Read(tempDir)
		require.Error(t, err, "Read should fail with a truncated file")
		assert.True(t, found, "found should be true as the file exists")
	})
}

func TestCheckpoint_Write_AtomicitySimulation(t *testing.T) {
	tempDir := t.TempDir()
	tempPath := filepath.Join(tempDir, TempFileName)

	// 1. Create an old checkpoint file
	oldCp := Checkpoint{LastSafeSegmentIndex: 99}
	err := Write(tempDir, oldCp)
	require.NoError(t, err)

	// 2. Simulate a crash during a new write, after the .tmp file is created but before rename
	// We can't actually crash, so we'll manually create the .tmp file.
	newCp := Checkpoint{LastSafeSegmentIndex: 199}
	file, err := os.Create(tempPath)
	require.NoError(t, err)
	require.NoError(t, binary.Write(file, binary.LittleEndian, MagicNumber))
	require.NoError(t, binary.Write(file, binary.LittleEndian, newCp.LastSafeSegmentIndex))
	require.NoError(t, file.Sync())
	file.Close()

	// 3. Now, try to Read. It should read the old, valid CHECKPOINT file, not the .tmp one.
	readCp, found, err := Read(tempDir)
	require.NoError(t, err, "Read should succeed even with a dangling .tmp file")
	require.True(t, found, "Should find the old, valid checkpoint")
	assert.Equal(t, oldCp.LastSafeSegmentIndex, readCp.LastSafeSegmentIndex, "Should read the value from the old checkpoint")
}
