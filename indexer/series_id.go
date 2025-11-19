package indexer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/sys"
)

// SeriesIDStore manages the mapping between series keys and integer IDs.
type SeriesIDStore struct {
	logFile sys.FileHandle
	nextID  atomic.Uint64
	logger  *slog.Logger
	mu      sync.RWMutex // protects maps and file writes

	// In-memory indexes
	keyToID map[string]uint64
	idToKey map[uint64]string

	hookManager hooks.HookManager

	// Function fields for mocking persistence and closing behavior
	addEntryFunc func(seriesKey string, id uint64) error
	closeFunc    func() error
}

// NewSeriesIDStore creates a new SeriesIDStore.
func NewSeriesIDStore(logger *slog.Logger, hookManager hooks.HookManager) *SeriesIDStore {
	s := &SeriesIDStore{
		logger:      logger,
		keyToID:     make(map[string]uint64),
		idToKey:     make(map[uint64]string),
		hookManager: hookManager,
	}
	// Initialize function fields with their default implementations
	s.addEntryFunc = s.DefaultAddEntry
	s.closeFunc = s.DefaultClose
	s.nextID.Store(1)
	return s
}

// LoadFromFile loads the series mappings from the log file into memory.
// It now includes validation of the file header and per-record checksums
// to ensure integrity and compatibility.
func (s *SeriesIDStore) LoadFromFile(dataDir string) (err error) {
	logPath := filepath.Join(dataDir, core.SeriesMappingLogName)
	var maxId uint64 = 0

	file, openErr := sys.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0644)
	if openErr != nil {
		return fmt.Errorf("failed to open or create series mapping file: %w", err)
	}
	s.logFile = file // Store the file handle for later writes

	// Read and validate header
	var header core.FileHeader
	if err := binary.Read(file, binary.BigEndian, &header); err != nil {
		if err == io.EOF {
			// File is new or empty, write a new header.
			s.logger.Info("Series mapping file is new or empty, writing header.", "path", logPath)
			newHeader := core.NewFileHeader(core.SeriesStoreMagicNumber, core.CompressionNone)
			if _, err := file.Seek(0, 0); err != nil {
				return fmt.Errorf("failed to seek to start of new series mapping file: %w", err)
			}
			if err := binary.Write(file, binary.BigEndian, &newHeader); err != nil {
				return fmt.Errorf("failed to write header to new series mapping file: %w", err)
			}
			return nil // Nothing more to load
		}
		return fmt.Errorf("failed to read series mapping header: %w", err)
	}

	if header.Magic != core.SeriesStoreMagicNumber {
		return fmt.Errorf("invalid series mapping file magic number: got %x, want %x", header.Magic, core.SeriesStoreMagicNumber)
	}
	if header.Version > core.FormatVersion {
		return fmt.Errorf("unsupported series mapping file version: got %d, want <= %d", header.Version, core.FormatVersion)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.keyToID = make(map[string]uint64)
	s.idToKey = make(map[uint64]string)
	s.nextID.Store(1)

	reader := bufio.NewReader(file)

	for {
		// Read record length
		var recordLen uint32
		if err := binary.Read(reader, binary.BigEndian, &recordLen); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read record length: %w", err)
		}

		recordData := make([]byte, recordLen)
		if _, err := io.ReadFull(reader, recordData); err != nil {
			return fmt.Errorf("failed to read record data (expected %d bytes): %w", recordLen, err)
		}
		var storedChecksum uint32
		if err := binary.Read(reader, binary.BigEndian, &storedChecksum); err != nil {
			return fmt.Errorf("failed to read checksum: %w", err)
		}

		calculatedChecksum := crc32.ChecksumIEEE(recordData)
		if storedChecksum != calculatedChecksum {
			return fmt.Errorf("checksum mismatch for a record in %s", logPath)
		}

		// Decode the record data itself
		recordReader := bytes.NewReader(recordData)
		var id uint64
		binary.Read(recordReader, binary.BigEndian, &id)

		var keyLen uint16
		binary.Read(recordReader, binary.BigEndian, &keyLen)
		keyBytes := make([]byte, keyLen)
		io.ReadFull(recordReader, keyBytes)

		key := string(keyBytes)

		// Populate in-memory maps
		s.keyToID[key] = id
		s.idToKey[id] = key

		if id > maxId {
			maxId = id
		}
	}
	s.nextID.Store(maxId + 1) // nextID will be incremented before use

	// After loading, seek to the end of the file for subsequent appends.
	if _, err := s.logFile.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to end of series mapping file after loading: %w", err)
	}
	return nil // Success
}

// DefaultAddEntry is the default implementation for persisting a new series mapping.
func (s *SeriesIDStore) DefaultAddEntry(seriesKey string, id uint64) error {
	if s.logFile == nil {
		return errors.New("series id store log file is not open")
	}

	var dataBuf bytes.Buffer
	// Write SeriesID
	if err := binary.Write(&dataBuf, binary.BigEndian, id); err != nil {
		return err
	}
	// Write SeriesKey with length prefix
	keyBytes := []byte(seriesKey)
	keyLen := uint16(len(keyBytes))
	if err := binary.Write(&dataBuf, binary.BigEndian, keyLen); err != nil {
		return err
	}
	if _, err := dataBuf.Write(keyBytes); err != nil {
		return err
	}

	dataBytes := dataBuf.Bytes()
	recordLen := uint32(len(dataBytes))
	checksum := crc32.ChecksumIEEE(dataBytes)

	var recordBuf bytes.Buffer
	// Write record length
	if err := binary.Write(&recordBuf, binary.BigEndian, recordLen); err != nil {
		return err
	}
	// Write data
	if _, err := recordBuf.Write(dataBytes); err != nil {
		return err
	}
	// Write checksum
	if err := binary.Write(&recordBuf, binary.BigEndian, checksum); err != nil {
		return err
	}

	// Append to file
	if _, err := s.logFile.Write(recordBuf.Bytes()); err != nil {
		return err
	}

	// Sync to disk to ensure durability
	return s.logFile.Sync()
}

// defaultClose is the default implementation for closing the log file.
func (s *SeriesIDStore) DefaultClose() error {
	if s.logFile != nil {
		err := s.logFile.Sync() // Final sync before closing
		if err != nil {
			s.logger.Error("Failed to sync series mapping log on close", "error", err)
			// Continue to close anyway
		}
		errClose := s.logFile.Close()
		s.logFile = nil
		if errClose != nil {
			return fmt.Errorf("failed to close series mapping log: %w", errClose)
		}
	}
	return nil
}

// GetOrCreateID retrieves the ID for a series key, creating one if it doesn't exist.
func (s *SeriesIDStore) GetOrCreateID(seriesKey string) (uint64, error) {
	// First, check with a read lock for performance
	s.mu.RLock()
	id, ok := s.keyToID[seriesKey]
	s.mu.RUnlock()
	if ok {
		return id, nil
	}

	// If not found, acquire a write lock to create it
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check in case another goroutine created it while we were waiting for the lock
	if id, ok := s.keyToID[seriesKey]; ok {
		return id, nil
	}

	newID := s.nextID.Load()
	s.nextID.Add(1)
	// Update in-memory maps after successful persistence
	s.keyToID[seriesKey] = newID
	s.idToKey[newID] = seriesKey

	if err := s.addEntryFunc(seriesKey, newID); err != nil { // Call through the function field
		// If persistence fails, we should not add it to the in-memory map
		// and potentially roll back the atomic counter, though that's complex.
		// For now, we return the error. The next attempt will get a new ID.
		delete(s.keyToID, seriesKey)
		delete(s.idToKey, newID)
		return 0, fmt.Errorf("failed to persist new series mapping: %w", err)
	}

	// --- NEW HOOK TRIGGER ---
	if s.hookManager != nil {
		payload := hooks.SeriesCreatePayload{SeriesKey: seriesKey, SeriesID: newID}
		// Use background context as this is an internal, non-request-driven event.
		s.hookManager.Trigger(context.Background(), hooks.NewOnSeriesCreateEvent(payload))
	}

	return newID, nil
}

// GetID retrieves the ID for a series key.
func (s *SeriesIDStore) GetID(seriesKey string) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.keyToID[seriesKey]
	if !ok {
		return 0, false
	}
	return id, true
}

// GetKey retrieves the series key for an ID.
func (s *SeriesIDStore) GetKey(id uint64) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seriesKey, ok := s.idToKey[id]
	if !ok {
		return "", false
	}
	return seriesKey, true
}

// Sync flushes the underlying mapping files to disk.
func (s *SeriesIDStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.logFile != nil {
		return s.logFile.Sync()
	}
	return nil
}

// Close closes the underlying log file.
func (s *SeriesIDStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeFunc()
}

// Private
// GetLogFilePath returns the path of the underlying log file.
// This is part of the internal.PrivateSeriesIDStore interface.
func (s *SeriesIDStore) GetLogFilePath() string {
	if s.logFile == nil {
		return ""
	}
	return s.logFile.Name()
}
