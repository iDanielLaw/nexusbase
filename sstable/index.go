package sstable

// index.go: Defines Index structure, (de)serialization, lookup for SSTables.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log/slog" // Import slog
	"sort"     // Import the sort package

	"github.com/INLOpen/nexusbase/core"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Placeholder for SSTable index logic.
// Details to be filled based on FR4.2.

// BlockIndexEntry represents an entry in the SSTable's sparse index.
// It points to a data block.
type BlockIndexEntry struct {
	FirstKey    []byte // The first key in the block
	BlockOffset int64  // Offset of the data block in the SSTable file
	BlockLength uint32 // Length of the data block
}

// IndexBuilder helps in constructing the index as data is written to SSTable.
type IndexBuilder struct {
	entries []BlockIndexEntry
}

// Add records the metadata for a newly written data block.
// firstKey must be a copy, as the original might be reused.
func (ib *IndexBuilder) Add(firstKey []byte, blockOffset int64, blockLength uint32) {
	ib.entries = append(ib.entries, BlockIndexEntry{
		FirstKey:    firstKey,
		BlockOffset: blockOffset,
		BlockLength: blockLength,
	})
}

// Build serializes the collected index entries into a byte slice.
// Format per entry: KeyLen (uint32), Key, BlockOffset (int64), BlockLength (uint32).
// The returned byte slice is the raw index data. A checksum of this data is also returned.
// Corresponds to FR4.2.
func (ib *IndexBuilder) Build() ([]byte, uint32, error) {
	// Use a pooled buffer to avoid allocations during index building.
	buf := core.BufferPool.Get()
	defer core.BufferPool.Put(buf)
	for _, entry := range ib.entries {
		keyLen := uint32(len(entry.FirstKey))
		if err := binary.Write(buf, binary.LittleEndian, keyLen); err != nil {
			return nil, 0, err
		}
		if _, err := buf.Write(entry.FirstKey); err != nil {
			return nil, 0, err
		}
		if err := binary.Write(buf, binary.LittleEndian, entry.BlockOffset); err != nil {
			return nil, 0, err
		}
		if err := binary.Write(buf, binary.LittleEndian, entry.BlockLength); err != nil {
			return nil, 0, err
		}
	}
	indexData := buf.Bytes()
	checksum := crc32.ChecksumIEEE(indexData)
	// Return a copy of the data, as the buffer will be reset and reused.
	dataCopy := make([]byte, len(indexData))
	copy(dataCopy, indexData)
	return dataCopy, checksum, nil
}

// Index struct will be defined later for loading/reading the index.
// For now, IndexBuilder is sufficient for the writer.
// Index represents the in-memory representation of the SSTable's index.
// It allows for efficient lookups of key offsets.
// Corresponds to FR4.2.
type Index struct {
	entries []BlockIndexEntry
	tracer  trace.Tracer // For OpenTelemetry tracing
	logger  *slog.Logger // Add logger field
}

// DeserializeIndex reconstructs an Index from its serialized byte representation.
// It expects the raw index data (without checksum) and the expected checksum.
// Corresponds to FR4.2, FR4.6.
func DeserializeIndex(data []byte, expectedChecksum uint32, tracer trace.Tracer, logger *slog.Logger) (*Index, error) {
	if logger == nil {
		logger = slog.Default().With("component", "Index_default")
	}

	calculatedChecksum := crc32.ChecksumIEEE(data)
	if calculatedChecksum != expectedChecksum {
		logger.Error("Index checksum mismatch.", "expected", expectedChecksum, "calculated", calculatedChecksum)
		return nil, fmt.Errorf("index checksum mismatch: %w", ErrCorrupted)
	}

	idx := &Index{tracer: tracer, logger: logger}
	offset := 0

	for offset < len(data) {

		var keyLen uint32
		keyLen = binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		keyStart := offset
		keyEnd := offset + int(keyLen)
		if keyEnd > len(data) {
			return nil, fmt.Errorf("index key length exceeds data bounds")
		}
		key := data[keyStart:keyEnd]
		offset = keyEnd

		var blockOffset int64
		blockOffset = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8

		var blockLength uint32
		blockLength = binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		idx.entries = append(idx.entries, BlockIndexEntry{
			FirstKey:    key,
			BlockOffset: blockOffset,
			BlockLength: blockLength,
		})
	}
	return idx, nil

}

// Find searches for a key in the sparse index and returns the BlockIndexEntry
// for the block that *might* contain the key.
// It returns the entry for which entry.FirstKey <= key < nextEntry.FirstKey.
// If the key is smaller than all first keys, it returns the first block.
// If the key is larger than all first keys, it returns the last block.
// Corresponds to FR4.2.
func (idx *Index) Find(key []byte) (entry BlockIndexEntry, found bool) {
	var span trace.Span
	if idx.tracer != nil {
		_, span = idx.tracer.Start(context.Background(), "Index.Find")
		span.SetAttributes(attribute.String("sstable.index.search_key", string(key)))
		defer span.End()
	}

	if len(idx.entries) == 0 {
		return BlockIndexEntry{}, false
	}

	i := sort.Search(len(idx.entries), func(i int) bool {
		return bytes.Compare(idx.entries[i].FirstKey, key) >= 0
	})

	if i < len(idx.entries) {
		if bytes.Equal(idx.entries[i].FirstKey, key) {
			// Key is exactly the FirstKey of block i. This is the block.
			if span != nil {
				span.SetAttributes(attribute.Bool("sstable.index.found_exact", true), attribute.Int("sstable.index.found_block_idx", i))
			}
			return idx.entries[i], true
		}
		// If key < entries[i].FirstKey:
		if i > 0 {
			// Key is greater than FirstKey of block i-1 and less than FirstKey of block i.
			// So, key belongs to block i-1.
			if span != nil {
				span.SetAttributes(attribute.Bool("sstable.index.found_in_prev_block", true), attribute.Int("sstable.index.found_block_idx", i-1))
			}
			return idx.entries[i-1], true
		}
		// If i == 0, key is less than FirstKey of block 0.
		// The SSTable.Get() minKey check should ideally catch this.
		// However, if we reach here, block 0 is the only candidate.
		// This means key >= entries[0].FirstKey (from sort.Search condition if i=0 and FirstKey != key)
		// or key < entries[0].FirstKey.
		// If key < entries[0].FirstKey, the Get() method's minKey check should prevent this path for valid gets.
		// If key >= entries[0].FirstKey, then block 0 is the correct candidate.
		if span != nil {
			span.SetAttributes(attribute.Bool("sstable.index.candidate_is_first_block", true), attribute.Int("sstable.index.found_block_idx", 0))
		}
		return idx.entries[0], true // Candidate is the first block
	}

	// If i == len(idx.entries), key is greater than or equal to the FirstKey of the last block.
	// So, the last block is the candidate.
	if span != nil {
		span.SetAttributes(attribute.Bool("sstable.index.candidate_is_last_block", true), attribute.Int("sstable.index.found_block_idx", len(idx.entries)-1))
	}
	return idx.entries[len(idx.entries)-1], true
}

// findFirstGreaterOrEqual finds the index of the first entry whose key is greater than or equal to the given key.
// This is useful for iterators to find the starting position.
// Returns len(idx.entries) if all keys are less than the given key.
func (idx *Index) findFirstGreaterOrEqual(key []byte) int {
	// Use standard library's binary search which is optimized for this.
	// It returns the index i such that all elements in entries[:i] are < key,
	// and all elements in entries[i:] are >= key.
	return sort.Search(len(idx.entries), func(i int) bool {
		return bytes.Compare(idx.entries[i].FirstKey, key) >= 0
	})
}

// GetEntires returns the internal slice of BlockIndexEntry.
// This is useful for testing or introspection.
func (idx *Index) GetEntries() []BlockIndexEntry {
	return idx.entries
}
