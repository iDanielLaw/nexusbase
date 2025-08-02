package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sys"
)

const (
	segmentFileSuffix = ".wal"
	// MaxSegmentSize is the default maximum size for a WAL segment file.
	MaxSegmentSize = 128 * 1024 * 1024 // 128 MB
)

// Segment represents a single WAL segment file.
type Segment struct {
	file  sys.FileInterface
	path  string
	index uint64
}

// SegmentWriter handles writing records to a segment.
type SegmentWriter struct {
	*Segment
	writer *bufio.Writer
}

// SegmentReader handles reading records from a segment.
type SegmentReader struct {
	*Segment
	reader *bufio.Reader
}

// formatSegmentFileName creates a segment file name from its index.
func formatSegmentFileName(index uint64) string {
	return fmt.Sprintf("%08d%s", index, segmentFileSuffix)
}

// parseSegmentFileName extracts the index from a segment file name.
func parseSegmentFileName(name string) (uint64, error) {
	if !strings.HasSuffix(name, segmentFileSuffix) {
		return 0, fmt.Errorf("file %s is not a WAL segment file", name)
	}
	name = strings.TrimSuffix(name, segmentFileSuffix)
	return strconv.ParseUint(name, 10, 64)
}

// CreateSegment creates a new segment file in the given directory.
func CreateSegment(dir string, index uint64) (*SegmentWriter, error) {
	path := filepath.Join(dir, formatSegmentFileName(index))
	file, err := sys.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment file %s: %w", path, err)
	}

	// Write header
	header := core.NewFileHeader(core.WALMagic, core.CompressionNone)
	if err := binary.Write(file, binary.LittleEndian, &header); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write segment header to %s: %w", path, err)
	}

	seg := &Segment{
		file:  file,
		path:  path,
		index: index,
	}
	return &SegmentWriter{
		Segment: seg,
		writer:  bufio.NewWriter(file),
	}, nil
}

// OpenSegmentForRead opens an existing segment file for reading.
func OpenSegmentForRead(path string) (*SegmentReader, error) {
	file, err := sys.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file for reading %s: %w", path, err)
	}

	// Read and verify header
	var header core.FileHeader
	if err := binary.Read(file, binary.LittleEndian, &header); err != nil {
		file.Close()
		if err == io.EOF {
			return nil, fmt.Errorf("segment file %s is empty or truncated at header", path)
		}
		return nil, fmt.Errorf("failed to read segment header from %s: %w", path, err)
	}
	if header.Magic != core.WALMagic {
		file.Close()
		return nil, fmt.Errorf("invalid magic number in segment %s: got %x, want %x", path, header.Magic, core.WALMagic)
	}

	index, err := parseSegmentFileName(filepath.Base(path))
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("could not parse segment index from path %s: %w", path, err)
	}

	seg := &Segment{
		file:  file,
		path:  path,
		index: index,
	}
	return &SegmentReader{
		Segment: seg,
		reader:  bufio.NewReader(file),
	}, nil
}

// WriteRecord writes a single record to the segment.
// Format: length (4 bytes) | data (variable) | checksum (4 bytes)
func (sw *SegmentWriter) WriteRecord(data []byte) error {
	if sw.file == nil {
		return os.ErrClosed
	}

	// Write length
	if err := binary.Write(sw.writer, binary.LittleEndian, uint32(len(data))); err != nil {
		return fmt.Errorf("failed to write record length: %w", err)
	}

	// Write data
	if _, err := sw.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write record data: %w", err)
	}

	// Write checksum
	checksum := crc32.ChecksumIEEE(data)
	if err := binary.Write(sw.writer, binary.LittleEndian, checksum); err != nil {
		return fmt.Errorf("failed to write record checksum: %w", err)
	}

	return nil
}

// ReadRecord reads a single record from the segment.
func (sr *SegmentReader) ReadRecord() ([]byte, error) {
	return readRecord(sr.reader)
}

// Sync flushes the buffered writer and syncs the file to disk.
func (sw *SegmentWriter) Sync() error {
	if err := sw.writer.Flush(); err != nil {
		return err
	}
	return sw.file.Sync()
}

// Close flushes and closes the segment file.
func (sw *SegmentWriter) Close() error {
	if sw.file == nil {
		return nil
	}
	err := sw.Sync()
	closeErr := sw.file.Close()
	sw.file = nil
	if err != nil {
		return err
	}
	return closeErr
}

// Close closes the segment file.
func (sr *SegmentReader) Close() error {
	if sr.file == nil {
		return nil
	}
	err := sr.file.Close()
	sr.file = nil
	return err
}

// Size returns the current size of the segment file.
func (s *Segment) Size() (int64, error) {
	if s.file == nil {
		return 0, os.ErrClosed
	}
	stat, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
