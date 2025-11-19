package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sys"
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
	size   int64 // Tracks the current size including buffered writes
}

// SegmentReader handles reading records from a segment.
type SegmentReader struct {
	*Segment
	reader *bufio.Reader
}

// CreateSegment creates a new segment file in the given directory.
// `writerBufSize` configures the size of the buffered writer for this segment.
func CreateSegment(dir string, index uint64, writerBufSize int, preallocSize int64) (*SegmentWriter, error) {
	path := filepath.Join(dir, core.FormatSegmentFileName(index))
	file, err := sys.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment file %s: %w", path, err)
	}

	// Write header
	header := core.NewFileHeader(core.WALMagicNumber, core.CompressionNone)
	if err := binary.Write(file, binary.LittleEndian, &header); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write segment header to %s: %w", path, err)
	}

	// Optionally preallocate the segment file to reduce fragmentation and growth cost.
	// Treat preallocation as best-effort: if it fails, log a warning and continue
	// rather than failing segment creation. This avoids hard failures on filesystems
	// that don't support fallocate or when the operation is not permitted.
	if preallocSize > 0 {
		if err := sys.Preallocate(file, preallocSize); err != nil {
			fmt.Printf("warning: failed to preallocate segment file %s to %d: %v\n", path, preallocSize, err)
		}
	}

	seg := &Segment{
		file:  file,
		path:  path,
		index: index,
	}
	return &SegmentWriter{
		Segment: seg,
		writer:  bufio.NewWriterSize(file, writerBufSize),
		size:    int64(header.Size()), // Initialize with header size
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
	if header.Magic != core.WALMagicNumber {
		file.Close()
		return nil, fmt.Errorf("invalid magic number in segment %s: got %x, want %x", path, header.Magic, core.WALMagicNumber)
	}

	index, err := core.ParseSegmentFileName(filepath.Base(path))
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

	// Update internal size tracker
	sw.size += int64(4 + len(data) + 4) // length + data + checksum

	return nil
}

// ReadRecord reads a single record from the segment.
func (sr *SegmentReader) ReadRecord() ([]byte, error) {
	var length uint32
	if err := binary.Read(sr.reader, binary.LittleEndian, &length); err != nil {
		return nil, err // Could be io.EOF for clean end
	}

	// A sanity check to prevent allocating huge amounts of memory for a corrupt record.
	// This limit can be adjusted based on expected maximum record size.
	if length > core.WALMaxSegmentSize {
		return nil, fmt.Errorf("wal record length %d exceeds sanity limit of %d bytes", length, core.WALMaxSegmentSize)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(sr.reader, data); err != nil {
		return nil, fmt.Errorf("failed to read record data (expected %d bytes): %w", length, err)
	}

	var storedChecksum uint32
	if err := binary.Read(sr.reader, binary.LittleEndian, &storedChecksum); err != nil {
		return nil, fmt.Errorf("failed to read record checksum: %w", err)
	}

	if calculatedChecksum := crc32.ChecksumIEEE(data); calculatedChecksum != storedChecksum {
		return nil, fmt.Errorf("checksum mismatch: stored=%x, calculated=%x", storedChecksum, calculatedChecksum)
	}

	return data, nil
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

// Size returns the current size of the segment file, including buffered data.
// This overrides the embedded Segment.Size method.
func (sw *SegmentWriter) Size() (int64, error) {
	if sw.file == nil {
		return 0, os.ErrClosed
	}
	return sw.size, nil
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
