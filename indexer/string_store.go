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
	"sort"
	"sync"
	"sync/atomic"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/sys"
)

// StringStore manages the mapping between strings (metrics, tag keys, tag values) and integer IDs.
type StringStore struct {
	logFile sys.FileHandle
	nextID  atomic.Uint64 // Use uint32 for IDs to save space
	logger  *slog.Logger
	mu      sync.RWMutex

	stringToID map[string]uint64
	idToString map[uint64]string

	hookManager hooks.HookManager
}

// NewStringStore creates a new StringStore.
func NewStringStore(logger *slog.Logger, hookManager hooks.HookManager) *StringStore {
	s := &StringStore{
		logger:      logger,
		stringToID:  make(map[string]uint64),
		idToString:  make(map[uint64]string),
		hookManager: hookManager,
	}

	s.nextID.Store(1)
	return s
}

// Private interface
func (s *StringStore) GetLogFilePath() string {
	return s.logFile.Name()
}

// LoadFromFile loads the string mappings from the log file into memory.
// It now includes validation of the file header and per-record checksums
// to ensure integrity and compatibility.
func (s *StringStore) LoadFromFile(dataDir string) (err error) {
	logPath := filepath.Join(dataDir, core.StringMappingLogName)
	var maxId uint64 = 0

	file, err := sys.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open or create string mapping file: %w", err)
	}
	s.logFile = file

	// Read and validate header
	var header core.FileHeader
	if err := binary.Read(file, binary.BigEndian, &header); err != nil {
		if err == io.EOF {
			// File is new or empty, write a new header.
			s.logger.Info("String mapping file is new or empty, writing header.", "path", logPath)
			newHeader := core.NewFileHeader(core.StringStoreMagicNumber, core.CompressionNone)
			if _, err := file.Seek(0, 0); err != nil {
				return fmt.Errorf("failed to seek to start of new string mapping file: %w", err)
			}
			if err := binary.Write(file, binary.BigEndian, &newHeader); err != nil {
				return fmt.Errorf("failed to write header to new string mapping file: %w", err)
			}
			return nil // Nothing more to load
		}
		return fmt.Errorf("failed to read string mapping header: %w", err)
	}

	if header.Magic != core.StringStoreMagicNumber {
		return fmt.Errorf("invalid string mapping file magic number: got %x, want %x", header.Magic, core.StringStoreMagicNumber)
	}
	if header.Version > core.FormatVersion {
		return fmt.Errorf("unsupported string mapping file version: got %d, want <= %d", header.Version, core.FormatVersion)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.stringToID = make(map[string]uint64)
	s.idToString = make(map[uint64]string)
	s.nextID.Store(1)

	// Start reading records after the header
	reader := bufio.NewReader(file)

	for {
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

		var strLen uint16
		binary.Read(recordReader, binary.BigEndian, &strLen)
		strBytes := make([]byte, strLen)
		io.ReadFull(recordReader, strBytes)

		str := string(strBytes)

		s.stringToID[str] = id
		s.idToString[id] = str

		if id > maxId {
			maxId = id
		}
	}
	s.nextID.Store(maxId + 1)

	// After loading, seek to the end of the file for subsequent appends.
	if _, err := s.logFile.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to end of string mapping file after loading: %w", err)
	}
	return nil
}

// GetOrCreateID retrieves the ID for a string, creating one if it doesn't exist.
func (s *StringStore) GetOrCreateID(str string) (uint64, error) {
	s.mu.RLock()
	id, ok := s.stringToID[str]
	s.mu.RUnlock()
	if ok {
		return id, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if id, ok := s.stringToID[str]; ok {
		return id, nil
	}

	newID := s.nextID.Load()
	s.nextID.Add(1)

	s.stringToID[str] = newID
	s.idToString[newID] = str

	if err := s.addEntry(str, newID); err != nil {
		delete(s.stringToID, str)
		delete(s.idToString, newID)
		return 0, fmt.Errorf("failed to persist new string mapping: %w", err)
	}

	// --- NEW HOOK TRIGGER ---
	if s.hookManager != nil {
		payload := hooks.StringCreatePayload{Str: str, ID: newID}
		// Use background context as this is an internal, non-request-driven event.
		s.hookManager.Trigger(context.Background(), hooks.NewOnStringCreateEvent(payload))
	}

	return newID, nil
}

// GetString retrieves the string for an ID.
func (s *StringStore) GetString(id uint64) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	str, ok := s.idToString[id]
	return str, ok
}

// GetID retrieves the ID for a string.
func (s *StringStore) GetID(str string) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.stringToID[str]
	return id, ok
}

// Symbols returns a copy of all stored symbols in insertion order.
func (s *StringStore) Symbols() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.idToString))
	// idToString is a map; to return in id order, collect ids and sort them
	ids := make([]uint64, 0, len(s.idToString))
	for id := range s.idToString {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		out = append(out, s.idToString[id])
	}
	return out
}

// ExportSortedSymbols returns a lexicographically sorted copy of all stored symbols.
func (s *StringStore) ExportSortedSymbols() []string {
	syms := s.Symbols()
	sort.Strings(syms)
	return syms
}

// addEntry persists a new string mapping to the log file.
func (s *StringStore) addEntry(str string, id uint64) error {
	if s.logFile == nil {
		return errors.New("string store log file is not open for writing")
	}

	var dataBuf bytes.Buffer
	binary.Write(&dataBuf, binary.BigEndian, id)
	strLen := uint16(len(str))
	binary.Write(&dataBuf, binary.BigEndian, strLen)
	dataBuf.WriteString(str)

	dataBytes := dataBuf.Bytes()
	recordLen := uint32(len(dataBytes))
	checksum := crc32.ChecksumIEEE(dataBytes)

	var recordBuf bytes.Buffer
	binary.Write(&recordBuf, binary.BigEndian, recordLen)
	recordBuf.Write(dataBytes)
	binary.Write(&recordBuf, binary.BigEndian, checksum)

	// Write and sync using a centralized helper so batch writes can reuse the same logic.
	return s.writeAndSync(recordBuf.Bytes())
}

// writeAndSync writes the given bytes to the log file and performs an fsync.
// It assumes caller holds any required locks.
func (s *StringStore) writeAndSync(data []byte) error {
	if s.logFile == nil {
		return errors.New("string store log file is not open for writing")
	}
	if _, err := s.logFile.Write(data); err != nil {
		return err
	}
	return s.logFile.Sync()
}

// AddStringsBatch atomically appends multiple new string entries to the store
// in a single write+sync operation. It assigns IDs for strings that do not
// already exist and returns the assigned IDs in the same order as the input
// slice. If writing fails, in-memory assignments are rolled back.
func (s *StringStore) AddStringsBatch(strs []string) ([]uint64, error) {
	if len(strs) == 0 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	newIDs := make([]uint64, len(strs))
	// track which strings we newly inserted to allow rollback on failure
	inserted := make([]string, 0, len(strs))

	var batchBuf bytes.Buffer
	for i, str := range strs {
		// If already present, return existing id
		if id, ok := s.stringToID[str]; ok {
			newIDs[i] = id
			continue
		}

		id := s.nextID.Load()
		s.nextID.Add(1)

		// prepare record data bytes
		var dataBuf bytes.Buffer
		binary.Write(&dataBuf, binary.BigEndian, id)
		strLen := uint16(len(str))
		binary.Write(&dataBuf, binary.BigEndian, strLen)
		dataBuf.WriteString(str)
		dataBytes := dataBuf.Bytes()

		// record header + data + checksum
		recordLen := uint32(len(dataBytes))
		checksum := crc32.ChecksumIEEE(dataBytes)

		binary.Write(&batchBuf, binary.BigEndian, recordLen)
		batchBuf.Write(dataBytes)
		binary.Write(&batchBuf, binary.BigEndian, checksum)

		// tentatively add to in-memory maps
		s.stringToID[str] = id
		s.idToString[id] = str
		inserted = append(inserted, str)
		newIDs[i] = id
	}

	// nothing to persist (all existed)
	if batchBuf.Len() == 0 {
		return newIDs, nil
	}

	// attempt a single write+sync for the whole batch
	if err := s.writeAndSync(batchBuf.Bytes()); err != nil {
		// rollback in-memory insertions
		for _, str := range inserted {
			if id, ok := s.stringToID[str]; ok {
				delete(s.idToString, id)
				delete(s.stringToID, str)
			}
		}
		// restore nextID (best-effort) by finding max id in maps
		maxID := uint64(0)
		for id := range s.idToString {
			if id > maxID {
				maxID = id
			}
		}
		s.nextID.Store(maxID + 1)
		return nil, fmt.Errorf("failed to persist string store batch: %w", err)
	}

	return newIDs, nil
}

// Sync flushes the string mapping log to disk.
func (s *StringStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sync()
}

// sync flushes non-thread safe, use local function only.
func (s *StringStore) sync() error {
	if s.logFile != nil {
		return s.logFile.Sync()
	}
	return nil
}

// Close closes the underlying log file.
func (s *StringStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.logFile != nil {
		if err := s.sync(); err != nil {
			return err
		}
		err := s.logFile.Close()
		s.logFile = nil
		return err
	}
	return nil
}
