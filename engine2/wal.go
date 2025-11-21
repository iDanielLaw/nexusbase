package engine2

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/INLOpen/nexusbase/core"
)

// WAL is a very small append-only write-ahead log for engine2.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// NewWAL opens or creates the wal file at path.
func NewWAL(path string) (*WAL, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}
	return &WAL{f: f, path: path}, nil
}

// Append writes a DataPoint as length-prefixed gob blob.
func (w *WAL) Append(dp *core.DataPoint) error {
	// Prepare a serializable entry using exported fields.
	entry := struct {
		Metric    string
		Tags      map[string]string
		Timestamp int64
		Fields    map[string]interface{}
	}{
		Metric:    dp.Metric,
		Tags:      dp.Tags,
		Timestamp: dp.Timestamp,
		Fields:    nil,
	}
	if dp.Fields != nil {
		entry.Fields = dp.Fields.ToMap()
	}
	var buf []byte
	// encode to buffer
	var bbuf = &bytes.Buffer{}
	enc := gob.NewEncoder(bbuf)
	if err := enc.Encode(entry); err != nil {
		return fmt.Errorf("failed to encode dp: %w", err)
	}
	buf = bbuf.Bytes()

	w.mu.Lock()
	defer w.mu.Unlock()
	// write length
	if err := binary.Write(w.f, binary.LittleEndian, uint32(len(buf))); err != nil {
		return fmt.Errorf("failed to write wal length: %w", err)
	}
	if _, err := w.f.Write(buf); err != nil {
		return fmt.Errorf("failed to write wal blob: %w", err)
	}
	// flush to disk
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("failed to sync wal: %w", err)
	}
	return nil
}

// Replay reads all entries from the WAL and calls fn for each DataPoint.
func (w *WAL) Replay(fn func(*core.DataPoint) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// open file for read
	rf, err := os.Open(w.path)
	if err != nil {
		return fmt.Errorf("failed to open wal for replay: %w", err)
	}
	defer rf.Close()

	for {
		var l uint32
		if err := binary.Read(rf, binary.LittleEndian, &l); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("failed to read wal length: %w", err)
		}
		data := make([]byte, l)
		if _, err := io.ReadFull(rf, data); err != nil {
			return fmt.Errorf("failed to read wal data: %w", err)
		}
		bbuf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(bbuf)
		var entry struct {
			Metric    string
			Tags      map[string]string
			Timestamp int64
			Fields    map[string]interface{}
		}
		if err := dec.Decode(&entry); err != nil {
			return fmt.Errorf("failed to decode wal dp: %w", err)
		}
		var fv core.FieldValues
		if entry.Fields != nil {
			fv, _ = core.NewFieldValuesFromMap(entry.Fields)
		}
		if entry.Timestamp == -1 {
			// series tombstone: call fn with nil fields and timestamp -1
			dp := &core.DataPoint{Metric: entry.Metric, Tags: entry.Tags, Timestamp: -1, Fields: nil}
			if err := fn(dp); err != nil {
				return err
			}
			continue
		}
		dp := &core.DataPoint{
			Metric:    entry.Metric,
			Tags:      entry.Tags,
			Timestamp: entry.Timestamp,
			Fields:    fv,
		}
		if err := fn(dp); err != nil {
			return err
		}
	}
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
