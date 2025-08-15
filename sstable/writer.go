package sstable

import (
	"bytes"
	"context" // Import context for tracing
	"encoding/binary"
	"fmt"
	"hash/crc32" // Import hash/crc32
	"log/slog"
	"os"
	"path/filepath" // Import runtime
	"sync"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sys"

	"go.opentelemetry.io/otel/attribute" // For span attributes
	"go.opentelemetry.io/otel/codes"     // For span status codes
	"go.opentelemetry.io/otel/trace"     // For tracing
)

// writer.go: Defines SSTableWriter/Builder, Add, Finish
// Placeholder for SSTable writing logic.
// Details to be filled based on FR4.1, FR4.2, FR4.3, FR4.4.

// SSTableWriter is responsible for building a new SSTable file.
type SSTableWriter struct {
	filePath string
	file     sys.FileInterface
	offset   int64 // Current write offset in the file

	// Data structures to build during writing
	indexBuilder *IndexBuilder // To build the index (FR4.2) - IndexBuilder to be defined in index.go
	bloomFilter  *BloomFilter  // To build the bloom filter (FR4.3) from sstable package

	minKey []byte // Tracks the minimum key written (FR4.4)
	maxKey []byte // Tracks the maximum key written (FR4.4)

	// Configuration
	bloomFilterFalsePositiveRate float64         // Config for Bloom Filter (FR4.3)
	estimatedKeys                uint64          // Estimated number of keys for Bloom Filter (FR4.3)
	restartPointInterval         int             // Interval for restart points (FR4.8)
	blockSize                    int             // Target size for data blocks (FR4.8)
	compressor                   core.Compressor // Compressor interface instance

	// Mutex to protect concurrent access if needed (though typically writers are not concurrent)
	mu sync.Mutex

	// Block writing state
	currentBlockBuffer   bytes.Buffer // Buffer for the current data block
	currentBlockFirstKey []byte       // First key in the current block
	currentBlockLastKey  []byte       // Last key written to the current block (for prefix compression)
	numEntriesInBlock    int          // Number of entries in the current block
	restartPoints        []uint32     // Offsets of restart points in the current block
	currentBlockSize     int          // Estimated size of current block data (uncompressed)
	tracer               trace.Tracer // For OpenTelemetry tracing
	logger               *slog.Logger // Logger for debug/info messages
	// finished bool // To prevent Add after Finish
}

// NewSSTableWriter creates a new writer for building an SSTable.
// It creates a temporary file to write to.
// The final file will be renamed upon successful completion (FR7.1).
// 'id' is used to generate a unique temporary file name.
func NewSSTableWriter(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
	// 2. Open/Create the temporary file. Handle errors (FR7.1).
	// 3. Initialize indexBuilder (IndexBuilder to be defined in index.go).
	// 4. Initialize bloomFilter using estimatedKeys and falsePositiveRate (FR4.3).
	// 5. Initialize minKey and maxKey to nil or empty.
	// 6. Return the SSTableWriter struct.

	if opts.Logger == nil {
		opts.Logger = slog.Default() // Use default logger if none provided
	}

	tempFilePath := filepath.Join(opts.DataDir, fmt.Sprintf("%d.tmp", opts.ID))
	file, err := sys.Create(tempFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary sstable file %s: %w", tempFilePath, err) // Keep fmt.Errorf for error return
	}

	// Write the header at the beginning of the file.
	header := core.NewFileHeader(core.SSTableMagic, opts.Compressor.Type())
	if err := binary.Write(file, binary.LittleEndian, &header); err != nil {
		file.Close()
		os.Remove(tempFilePath)
		return nil, fmt.Errorf("failed to write sstable header: %w", err)
	}
	initialOffset := int64(header.Size())

	// Placeholder initialization
	bf, err := NewBloomFilter(opts.EstimatedKeys, opts.BloomFilterFalsePositiveRate) // Use sstable.NewBloomFilter
	if err != nil {
		file.Close()                                                     // Clean up the file handle
		os.Remove(tempFilePath)                                          // Attempt to remove the created .tmp file
		return nil, fmt.Errorf("failed to create bloom filter: %w", err) // Keep fmt.Errorf for error return
	}

	return &SSTableWriter{
		filePath:                     tempFilePath,
		file:                         file,
		offset:                       initialOffset, // Start writing data after the header
		indexBuilder:                 &IndexBuilder{},
		bloomFilter:                  bf,
		minKey:                       nil,
		maxKey:                       nil,
		bloomFilterFalsePositiveRate: opts.BloomFilterFalsePositiveRate,
		estimatedKeys:                opts.EstimatedKeys,
		restartPointInterval:         DefaultRestartPointInterval, // Use a default for now
		blockSize:                    opts.BlockSize,
		tracer:                       opts.Tracer,
		compressor:                   opts.Compressor,
		logger:                       opts.Logger,
	}, nil
	// Note: Logging writer.blockSize here would be after the return.
	// It's set to DefaultBlockSize initially. Tests can modify it on the returned instance.
}

// flushCurrentBlock writes the current buffered block to the file
// and updates the index.
// This method assumes that the caller (Add or Finish) already holds w.mu.
func (w *SSTableWriter) flushCurrentBlock() error {
	// Tracing for flushCurrentBlock
	// NOTE: Removed w.mu.Lock() and w.mu.Unlock() from here
	// as the callers (Add, Finish) already hold the lock.
	// This prevents nested locking / deadlocks.

	var span trace.Span
	if w.tracer != nil {
		_, span = w.tracer.Start(context.Background(), "SSTableWriter.flushCurrentBlock")
		defer span.End()
	}

	// REMOVED: w.mu.Lock()
	// REMOVED: defer w.mu.Unlock()

	if w.currentBlockBuffer.Len() == 0 || w.numEntriesInBlock == 0 {
		return nil // Nothing to flush
	}

	// --- Trailer Writing (Restart Points) ---
	// This happens *before* compression, as the trailer is part of the block data.

	for _, offset := range w.restartPoints {
		if err := binary.Write(&w.currentBlockBuffer, binary.LittleEndian, offset); err != nil {
			return fmt.Errorf("failed to write restart point offset: %w", err)
		}
	}
	binary.Write(&w.currentBlockBuffer, binary.LittleEndian, uint32(len(w.restartPoints)))
	// --- End Trailer Writing ---

	w.logger.Debug("Flushing block",
		"buffer_size", w.currentBlockBuffer.Len(),
		"num_entries", w.numEntriesInBlock,
		"first_key", string(w.currentBlockFirstKey))

	uncompressedBlockData := w.currentBlockBuffer.Bytes()

	w.logger.Debug("Before compression", "uncompressed_len", len(uncompressedBlockData))
	if w.compressor == nil {
		w.logger.Error("Compressor is nil, cannot flush block.")
		return fmt.Errorf("SSTableWriter.flushCurrentBlock: compressor is nil")
	}

	// Use a pooled buffer for the compressed data to reduce allocations.
	compressedBuf := core.BufferPool.Get()
	defer core.BufferPool.Put(compressedBuf)

	err := w.compressor.CompressTo(compressedBuf, uncompressedBlockData)
	if err != nil {
		w.logger.Error("Block compression failed", "error", err)
		return fmt.Errorf("failed to compress block: %w", err)
	}
	currentCompressionTypeOnDisk := w.compressor.Type()
	dataToWrite := compressedBuf.Bytes()
	w.logger.Debug("After compression", "data_to_write_len", len(dataToWrite), "compression_type", currentCompressionTypeOnDisk)

	checksum := crc32.ChecksumIEEE(dataToWrite)           // Checksum the data that will be written (potentially compressed)
	blockOffset := w.offset                               // This is the offset of the block's header (compression flag + checksum)
	blockLengthOnDisk := uint32(1 + 4 + len(dataToWrite)) // 1 (compression flag) + 4 (checksum) + len(dataToWrite)

	if span != nil {
		span.SetAttributes(
			attribute.Int64("sstable.block.offset", blockOffset),
			attribute.Int("sstable.block.uncompressed_len_bytes", len(uncompressedBlockData)),
			attribute.Int("sstable.block.compressed_len_bytes", len(dataToWrite)),
			attribute.Int("sstable.block.disk_len_bytes", int(blockLengthOnDisk)),
			attribute.Int("sstable.block.num_entries", w.numEntriesInBlock),
			attribute.String("sstable.block.compression", fmt.Sprintf("%v", currentCompressionTypeOnDisk)),
		)
	}
	// Write Compression Type Flag (1 byte)
	if err := binary.Write(w.file, binary.LittleEndian, byte(currentCompressionTypeOnDisk)); err != nil {
		w.logger.Error("Failed to write compression type flag", "error", err)
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return fmt.Errorf("failed to write compression type flag: %w", err)
	}
	w.offset += 1

	// Write checksum
	if err := binary.Write(w.file, binary.LittleEndian, checksum); err != nil { // Corrected: Use core.ChecksumSize
		w.logger.Error("Failed to write block checksum", "offset", w.offset, "error", err)
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return fmt.Errorf("failed to write block checksum (offset %d): %w", w.offset, err)
	}
	w.offset += int64(core.ChecksumSize)

	// Write actual block data (potentially compressed)
	if _, err := w.file.Write(dataToWrite); err != nil {
		w.logger.Error("Failed to write data block", "error", err)
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return fmt.Errorf("failed to write data block: %w", err)
	}
	w.offset += int64(len(dataToWrite))

	w.logger.Debug("Successfully wrote block data", "new_offset", w.offset)
	// Add to index (FR4.2 - sparse index for blocks)
	// FirstKey was already copied.
	w.logger.Debug("Before adding to index builder", "builder_entries", len(w.indexBuilder.entries), "first_key", string(w.currentBlockFirstKey))
	w.indexBuilder.Add(w.currentBlockFirstKey, blockOffset, blockLengthOnDisk)
	w.logger.Debug("After adding to index builder", "builder_entries", len(w.indexBuilder.entries))

	w.currentBlockBuffer.Reset()
	w.currentBlockFirstKey = nil
	w.currentBlockLastKey = nil
	w.numEntriesInBlock = 0
	w.restartPoints = w.restartPoints[:0] // Reset restart points for the new block
	w.currentBlockSize = 0                // Reset estimated size of current block
	return nil
}

// Add writes a key-value entry to the SSTable file.
// Entries must be added in increasing key order (FR4.1).
// Updates index, bloom filter, and min/max keys.
// Corresponds to FR4.1, FR4.2, FR4.3, FR4.4.
func (w *SSTableWriter) Add(key, value []byte, entryType core.EntryType, pointID uint64) error {
	var span trace.Span
	if w.tracer != nil {
		// Using background context as Add itself doesn't take one.
		// If context propagation is desired, Add would need to accept it.
		_, span = w.tracer.Start(context.Background(), "SSTableWriter.Add")
		defer span.End()
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	// --- Restart Point Logic ---
	// The first entry in a block is always a restart point.
	isRestartPoint := (w.numEntriesInBlock % w.restartPointInterval) == 0
	if isRestartPoint {
		// Record the offset of this entry *before* writing it.
		w.restartPoints = append(w.restartPoints, uint32(w.currentBlockBuffer.Len()))
	}

	// --- Prefix Compression ---
	var sharedPrefixLen int
	if w.currentBlockLastKey != nil && !isRestartPoint { // Force no sharing for restart points
		limit := len(key)
		if len(w.currentBlockLastKey) < limit {
			limit = len(w.currentBlockLastKey)
		}
		for sharedPrefixLen < limit && key[sharedPrefixLen] == w.currentBlockLastKey[sharedPrefixLen] {
			sharedPrefixLen++
		}
	}
	unsharedKey := key[sharedPrefixLen:]
	// --- End Prefix Compression ---

	entrySize := estimateEntrySizeWithPrefix(len(unsharedKey), len(value))

	// If the current block buffer is full, flush it before adding the new entry.
	// This ensures that individual blocks do not exceed blockSize.
	if w.currentBlockBuffer.Len() > 0 && (w.currentBlockSize+entrySize) > w.blockSize {
		if err := w.flushCurrentBlock(); err != nil {
			if span != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			return err
		}
		// After flushing, the new entry is the first in its block, so no prefix is shared.
		// and it must be a restart point.
		sharedPrefixLen = 0
		unsharedKey = key
		w.restartPoints = append(w.restartPoints, 0) // Offset is 0 in the new block
	}

	// Set first key for the block if it's the first entry in this block
	if w.currentBlockFirstKey == nil {
		w.currentBlockFirstKey = append([]byte(nil), key...)
	}

	if span != nil {
		span.SetAttributes(
			attribute.String("sstable.entry.key", string(key)), // Caution with large keys or PII
			attribute.Int("sstable.entry.type", int(entryType)),
			attribute.Int64("sstable.entry.point_id", int64(pointID)),
		)
	}

	// FR4.4: Update minKey and maxKey
	if w.minKey == nil || bytes.Compare(key, w.minKey) < 0 {
		w.minKey = append([]byte(nil), key...)
	}
	w.maxKey = append([]byte(nil), key...)

	// FR4.3: Add key to bloom filter
	w.bloomFilter.Add(key)

	// FR4.1 (Step 3): Format the entry using prefix compression and restart points.
	// Format: shared_key_len(varint), unshared_key_len(varint), value_len(varint), entry_type(byte), point_id(varint), unshared_key, value
	// Write directly to currentBlockBuffer
	varintBuf := make([]byte, binary.MaxVarintLen64)

	// Write shared_key_len (varint)
	n := binary.PutUvarint(varintBuf, uint64(sharedPrefixLen))
	w.currentBlockBuffer.Write(varintBuf[:n])

	// Write unshared_key_len (varint)
	n = binary.PutUvarint(varintBuf, uint64(len(unsharedKey)))
	w.currentBlockBuffer.Write(varintBuf[:n])

	// Write value_len (varint)
	n = binary.PutUvarint(varintBuf, uint64(len(value)))
	w.currentBlockBuffer.Write(varintBuf[:n])

	// Write entry_type (1 byte)
	if err := w.currentBlockBuffer.WriteByte(byte(entryType)); err != nil {
		return fmt.Errorf("failed to write entry type: %w", err)
	}

	// Write point_id (varint)
	n = binary.PutUvarint(varintBuf, pointID)
	if _, err := w.currentBlockBuffer.Write(varintBuf[:n]); err != nil {
		return fmt.Errorf("failed to write point id: %w", err)
	}

	// Write unshared_key and value
	w.currentBlockBuffer.Write(unsharedKey)
	w.currentBlockBuffer.Write(value)

	// Update state for next Add call
	w.currentBlockLastKey = append(w.currentBlockLastKey[:0], key...) // Update last key for next prefix compression
	w.numEntriesInBlock++
	w.currentBlockSize += entrySize // Accumulate estimated size of current block

	return nil
}

// Finish completes the SSTable writing process.
// It writes the index, bloom filter, footer, and renames the temporary file.
// Corresponds to FR4.1, FR4.2, FR4.3, FR7.1.
func (w *SSTableWriter) Finish() error {
	var span trace.Span
	if w.tracer != nil {
		_, span = w.tracer.Start(context.Background(), "SSTableWriter.Finish")
		// Defer the span.End() call. The recover logic is still needed due to the observed panics.
		defer func() {
			if span != nil {
				if r := recover(); r != nil {
					w.logger.Error("Recovered from panic during span.End()", "panic", r)
				}
				span.End()
			}
		}()
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	// if w.finished { return nil } // Idempotent Finish
	// w.finished = true

	// Flush any remaining entries in the current block
	w.logger.Debug("Finishing writer, flushing final block", "buffer_len", w.currentBlockBuffer.Len(), "num_entries_in_block", w.numEntriesInBlock)
	if err := w.flushCurrentBlock(); err != nil {
		w.abort() // Use non-locking abort
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return fmt.Errorf("failed to flush final block: %w", err)
	}

	w.logger.Debug("SSTableWriter final min/max keys before footer write",
		"writer_id", w.FilePath(), // Assuming w has an ID from NewSSTableWriter options
		"min_key", string(w.minKey),
		"max_key", string(w.maxKey))
	// Decode timestamps from MinKey/MaxKey and log
	if len(w.minKey) >= 8 && len(w.maxKey) >= 8 {
		minTs, _ := core.DecodeTimestamp(w.minKey[len(w.minKey)-8:])
		maxTs, _ := core.DecodeTimestamp(w.maxKey[len(w.maxKey)-8:])
		w.logger.Debug("SSTableWriter final min/max timestamps",
			"writer_id", w.FilePath(),
			"min_ts", time.Unix(0, minTs).Format(time.RFC3339Nano),
			"max_ts", time.Unix(0, maxTs).Format(time.RFC3339Nano))
	}

	w.logger.Debug("Building index", "builder_entries", len(w.indexBuilder.entries))
	// FR4.2: Get serialized index data and its checksum from indexBuilder
	indexData, indexChecksum, err := w.indexBuilder.Build()
	if err != nil {
		w.abort() // Attempt to clean up
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return fmt.Errorf("failed to build index: %w", err)
	}

	w.logger.Debug("Index built", "index_data_len", len(indexData), "index_entries", len(w.indexBuilder.entries))

	// Write index checksum
	if err := binary.Write(w.file, binary.LittleEndian, indexChecksum); err != nil {
		w.abort()
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return fmt.Errorf("failed to write index checksum: %w", err)
	}
	w.offset += int64(core.ChecksumSize) // Add checksum size to offset

	// FR4.3: Get serialized bloom filter data from w.bloomFilter
	bloomFilterData := w.bloomFilter.Bytes()

	// Write index data to file. Record its offset and length.
	indexOffset := w.offset
	n, err := w.file.Write(indexData)
	if err != nil {
		w.abort()
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return fmt.Errorf("failed to write index data: %w", err)
	}
	w.offset += int64(n)
	indexLen := uint32(n)

	// Write bloom filter data to file. Record its offset and length.
	bloomFilterOffset := w.offset
	n, err = w.file.Write(bloomFilterData)
	if err != nil {
		w.abort()
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return fmt.Errorf("failed to write bloom filter data: %w", err)
	}
	w.offset += int64(n)
	bloomFilterLen := uint32(n)

	// Write MinKey and MaxKey data to file. Record their offsets and lengths.
	minKeyOffset := w.offset
	n, err = w.file.Write(w.minKey)
	if err != nil {
		w.abort()
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return fmt.Errorf("failed to write min key data: %w", err)
	}
	w.offset += int64(n)
	minKeyLen := uint32(n)

	maxKeyOffset := w.offset
	n, err = w.file.Write(w.maxKey)
	if err != nil {
		w.abort()
		return fmt.Errorf("failed to write max key data: %w", err)
	}
	w.offset += int64(n)
	maxKeyLen := uint32(n)

	// FR4.1: Construct and write the footer structure
	footerBuf := core.BufferPool.Get()
	defer core.BufferPool.Put(footerBuf)
	// Order: IndexOffset, IndexLen, BloomFilterOffset, BloomFilterLen, MinKeyOffset, MinKeyLen, MaxKeyOffset, MaxKeyLen, MagicString
	binary.Write(footerBuf, binary.LittleEndian, uint64(indexOffset))       // Index offset
	binary.Write(footerBuf, binary.LittleEndian, indexLen)                  // Index length
	binary.Write(footerBuf, binary.LittleEndian, uint64(bloomFilterOffset)) // Bloom filter offset
	binary.Write(footerBuf, binary.LittleEndian, bloomFilterLen)            // Bloom filter length
	binary.Write(footerBuf, binary.LittleEndian, uint64(minKeyOffset))      // MinKey offset
	binary.Write(footerBuf, binary.LittleEndian, minKeyLen)                 // MinKey length
	binary.Write(footerBuf, binary.LittleEndian, uint64(maxKeyOffset))      // MaxKey offset
	binary.Write(footerBuf, binary.LittleEndian, maxKeyLen)                 // MaxKey length

	footerBuf.WriteString(MagicString) // Magic String

	if _, err := w.file.Write(footerBuf.Bytes()); err != nil {
		w.abort() // Use non-locking abort.
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return fmt.Errorf("failed to write footer: %w", err)
	}

	// FR7.1: Sync the file to disk
	if err := w.file.Sync(); err != nil {
		w.abort() // Use non-locking abort.
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return fmt.Errorf("failed to sync sstable file: %w", err)
	}

	// Close the file
	if err := w.file.Close(); err != nil {
		// Don't call Abort() here as the file might be partially valid or already synced.
		// The rename might fail, but the temp file might still exist.
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		w.file = nil // Set to nil after closing to prevent double-close in abort()
	}

	if gcErr := sys.GC(); gcErr != nil {
		w.logger.Warn("Failed to run GC to aid file handle release (Windows workaround).", "error", gcErr)
	}

	// FR7.1: Rename the temporary file to its final name
	finalPath := w.filePath[:len(w.filePath)-len(filepath.Ext(w.filePath))] + ".sst" // Change .tmp to .sst
	// Add retry logic for os.Rename, especially for Windows
	const maxRetries = 5
	const retryDelay = 100 * time.Millisecond
	var renameErr error
	for i := 0; i < maxRetries; i++ {
		renameErr = os.Rename(w.filePath, finalPath)
		if renameErr == nil {
			break // Success
		}
		w.logger.Warn("Failed to rename temporary sstable file, retrying...", "from", w.filePath, "to", finalPath, "attempt", i+1, "error", renameErr)
		time.Sleep(retryDelay)
	}

	if renameErr != nil {
		// If rename fails, the .tmp file still exists. abort might try to clean it.
		w.abort() // Attempt to clean up .tmp file if rename fails
		if span != nil {
			span.RecordError(renameErr)
			span.SetStatus(codes.Error, renameErr.Error())
		}
		return fmt.Errorf("failed to rename temporary sstable file %s to %s after %d retries: %w", w.filePath, finalPath, maxRetries, renameErr)
	}
	w.filePath = finalPath // Update to final path

	if span != nil {
		span.SetAttributes(attribute.String("sstable.final_path", finalPath))
	}

	return nil
}

// abort is the internal, non-locking version of Abort.
// It assumes the caller holds the mutex.
func (w *SSTableWriter) abort() error {
	// Close the file first
	if w.file != nil {
		w.file.Close() // Ignore error on close during abort
		w.file = nil
		sys.GC()
	}

	// Remove the temporary file with retry logic
	const maxRetries = 5
	const retryDelay = 100 * time.Millisecond
	var removeErr error
	if w.filePath != "" {
		for i := 0; i < maxRetries; i++ {
			removeErr = sys.Remove(w.filePath)
			if removeErr == nil || os.IsNotExist(removeErr) { // Success or file already gone
				break
			}
			w.logger.Warn("Failed to remove temporary sstable file during abort, retrying...", "path", w.filePath, "attempt", i+1, "error", removeErr)
			time.Sleep(retryDelay)
		}
	}

	if removeErr != nil && !os.IsNotExist(removeErr) {
		return fmt.Errorf("failed to remove temporary sstable file %s during abort after %d retries: %w", w.filePath, maxRetries, removeErr)
	}
	w.filePath = "" // Clear path after successful removal
	return nil
}

// Abort closes the writer and removes the temporary file.
// Should be called if an error occurs during writing.
// Corresponds to FR7.1.
func (w *SSTableWriter) Abort() error {
	var span trace.Span
	if w.tracer != nil {
		_, span = w.tracer.Start(context.Background(), "SSTableWriter.Abort")
		// Defer the span.End() call. The recover logic is still needed due to the observed panics.
		defer func() {
			if span != nil {
				if r := recover(); r != nil {
					w.logger.Error("Recovered from panic during span.End()", "panic", r)
				}
				span.End()
			}
		}()
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	err := w.abort()
	if err != nil && span != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// estimateEntrySizeWithPrefix provides a rough estimate of an entry's size on disk with prefix compression.
func estimateEntrySizeWithPrefix(unsharedKeyLen, valueLen int) int {
	// shared_len(varint) + unshared_len(varint) + value_len(varint) + type(1) + point_id(varint) + unshared_key + value
	return binary.MaxVarintLen32 + binary.MaxVarintLen32 + binary.MaxVarintLen32 + EntryTypeSize + binary.MaxVarintLen64 + unsharedKeyLen + valueLen
}

// BlockHeaderSize is the size of the compression flag and checksum at the start of each data block.
const BlockHeaderSize = 1 + 4 // 1 byte for compression flag + 4 bytes for CRC32 checksum

// FilePath returns the path to the SSTable file.
func (w *SSTableWriter) FilePath() string {
	return w.filePath
}

// CurrentSize returns the current size of the data written to the SSTable file so far (blocks only).
// This does not include the size of the index or bloom filter, which are written at Finish().
func (w *SSTableWriter) CurrentSize() int64 {
	return w.offset
}
