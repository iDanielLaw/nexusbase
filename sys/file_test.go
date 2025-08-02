package sys

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFileOperations covers basic file operations for the platform-specific File implementation.
func TestFileOperations(t *testing.T) {
	// Get the platform-specific file opener
	fileOpener := NewFile()
	if fileOpener == nil {
		t.Fatal("NewFile() returned nil")
	}

	tempDir := t.TempDir()
	testFilePath := filepath.Join(tempDir, "testfile.txt")

	t.Run("CreateAndWrite", func(t *testing.T) {
		// Test creating and writing to a new file
		file, err := fileOpener.OpenFile(testFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			t.Fatalf("OpenFile for writing failed: %v", err)
		}

		writeData := []byte("hello world")
		_, err = file.Write(writeData)
		if err != nil {
			file.Close()
			t.Fatalf("Write failed: %v", err)
		}

		err = file.Close()
		if err != nil {
			t.Fatalf("Close after write failed: %v", err)
		}

		// Verify content
		verifyFile, err := fileOpener.Open(testFilePath)
		if err != nil {
			t.Fatalf("Failed to open file for verification: %v", err)
		}
		defer verifyFile.Close()

		readData, err := io.ReadAll(verifyFile)
		if err != nil {
			t.Fatalf("ReadFile after write failed: %v", err)
		}
		if !bytes.Equal(readData, writeData) {
			t.Errorf("Read data mismatch: got %q, want %q", string(readData), string(writeData))
		}
	})

	t.Run("ReadExisting", func(t *testing.T) {
		// Test opening an existing file for reading
		file, err := fileOpener.Open(testFilePath) // Uses the simplified Open method
		if err != nil {
			t.Fatalf("Open for reading failed: %v", err)
		}
		defer file.Close()

		readData := make([]byte, 11) // "hello world" is 11 bytes
		_, err = file.Read(readData)
		if err != nil && err != io.EOF {
			t.Fatalf("Read failed: %v", err)
		}

		expectedData := []byte("hello world")
		if !bytes.Equal(readData, expectedData) {
			t.Errorf("Read data mismatch: got %q, want %q", string(readData), string(expectedData))
		}
	})

	t.Run("Append", func(t *testing.T) {
		// Test appending to an existing file
		file, err := fileOpener.OpenFile(testFilePath, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			t.Fatalf("OpenFile for appending failed: %v", err)
		}

		appendData := []byte(" again")
		_, err = file.Write(appendData)
		if err != nil {
			file.Close()
			t.Fatalf("Append write failed: %v", err)
		}

		err = file.Close()
		if err != nil {
			t.Fatalf("Close after append failed: %v", err)
		}

		// Verify content
		verifyFile, err := fileOpener.Open(testFilePath)
		if err != nil {
			t.Fatalf("Failed to open file for verification after append: %v", err)
		}
		defer verifyFile.Close()

		readData, err := io.ReadAll(verifyFile)
		if err != nil {
			t.Fatalf("ReadAll after append failed: %v", err)
		}

		expectedData := []byte("hello world again")
		if !bytes.Equal(readData, expectedData) {
			t.Errorf("Read data after append mismatch: got %q, want %q", string(readData), string(expectedData))
		}
	})

	t.Run("OpenWithRetry_Success", func(t *testing.T) {
		// This test just checks the success path of OpenWithRetry.
		// Testing the actual retry logic would require more complex mocking.
		retryFilePath := filepath.Join(tempDir, "retry_test.txt")
		err := os.WriteFile(retryFilePath, []byte("retry content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create file for retry test: %v", err)
		}

		file, err := fileOpener.OpenWithRetry(retryFilePath, os.O_RDONLY, 0, 3, 10*time.Millisecond)
		if err != nil {
			t.Fatalf("OpenWithRetry failed unexpectedly: %v", err)
		}
		defer file.Close()
	})

	t.Run("GC_NoPanic", func(t *testing.T) {
		// Just call GC to ensure it doesn't panic.
		// There's no reliable way to assert its effect in a unit test.
		err := fileOpener.GC()
		if err != nil {
			t.Errorf("GC() returned an unexpected error: %v", err)
		}
	})

	t.Run("SafeRemove", func(t *testing.T) {
		// Test removing an existing file
		removeFilePath := filepath.Join(tempDir, "remove_me.txt")
		err := os.WriteFile(removeFilePath, []byte("to be deleted"), 0644)
		if err != nil {
			t.Fatalf("Failed to create file for SafeRemove test: %v", err)
		}

		err = fileOpener.SafeRemove(removeFilePath)
		if err != nil {
			t.Fatalf("SafeRemove failed unexpectedly: %v", err)
		}

		// Verify file is gone
		if _, err := os.Stat(removeFilePath); !os.IsNotExist(err) {
			t.Errorf("Expected file %s to be removed, but it still exists (or another error occurred: %v)", removeFilePath, err)
		}

		// Test removing a non-existent file (should not error)
		err = fileOpener.SafeRemove(filepath.Join(tempDir, "does_not_exist.txt"))
		if err != nil {
			t.Errorf("SafeRemove on a non-existent file should not return an error, but got: %v", err)
		}
	})
}
