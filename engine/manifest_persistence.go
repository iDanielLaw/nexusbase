package engine

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/INLOpen/nexusbase/core"
)

// writeStringWithLength writes a length-prefixed string to the writer.
func writeStringWithLength(w io.Writer, s string) error {
	sBytes := []byte(s)
	if err := binary.Write(w, binary.LittleEndian, uint16(len(sBytes))); err != nil {
		return err
	}
	if len(sBytes) > 0 {
		if _, err := w.Write(sBytes); err != nil {
			return err
		}
	}
	return nil
}

// readStringWithLength reads a length-prefixed string from the reader.
func readStringWithLength(r io.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	sBytes := make([]byte, length)
	if _, err := io.ReadFull(r, sBytes); err != nil {
		return "", err
	}
	return string(sBytes), nil
}

// writeBytesWithLength writes a length-prefixed byte slice to the writer.
func writeBytesWithLength(w io.Writer, b []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint16(len(b))); err != nil {
		return err
	}
	if len(b) > 0 {
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

// readBytesWithLength reads a length-prefixed byte slice from the reader.
func readBytesWithLength(r io.Reader) ([]byte, error) {
	var length uint16
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	bBytes := make([]byte, length)
	if _, err := io.ReadFull(r, bBytes); err != nil {
		return nil, err
	}
	return bBytes, nil
}

// writeManifestBinary serializes the SnapshotManifest to a binary format.
func writeManifestBinary(w io.Writer, manifest *core.SnapshotManifest) error {
	// 1. Header
	header := core.NewFileHeader(core.ManifestMagic, core.CompressionNone)
	if err := binary.Write(w, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("failed to write manifest header: %w", err)
	}

	// 2. Sequence Number
	if err := binary.Write(w, binary.LittleEndian, manifest.SequenceNumber); err != nil {
		return fmt.Errorf("failed to write sequence number: %w", err)
	}

	// 3. Number of Levels
	if err := binary.Write(w, binary.LittleEndian, uint32(len(manifest.Levels))); err != nil {
		return fmt.Errorf("failed to write number of levels: %w", err)
	}

	// 4. Levels and Tables
	for _, level := range manifest.Levels {
		if err := binary.Write(w, binary.LittleEndian, uint32(level.LevelNumber)); err != nil {
			return fmt.Errorf("failed to write level number %d: %w", level.LevelNumber, err)
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(level.Tables))); err != nil {
			return fmt.Errorf("failed to write table count for level %d: %w", level.LevelNumber, err)
		}
		for _, table := range level.Tables {
			if err := binary.Write(w, binary.LittleEndian, table.ID); err != nil {
				return err
			}
			if err := writeBytesWithLength(w, []byte(table.FileName)); err != nil {
				return err
			}
			if err := writeBytesWithLength(w, table.MinKey); err != nil {
				return err
			}
			if err := writeBytesWithLength(w, table.MaxKey); err != nil {
				return err
			}
		}
	}

	// 5. Auxiliary Files
	if err := writeStringWithLength(w, manifest.WALFile); err != nil {
		return err
	}
	if err := writeStringWithLength(w, manifest.DeletedSeriesFile); err != nil {
		return err
	}
	if err := writeStringWithLength(w, manifest.RangeTombstonesFile); err != nil {
		return err
	}
	if err := writeStringWithLength(w, manifest.StringMappingFile); err != nil {
		return err
	}
	if err := writeStringWithLength(w, manifest.SeriesMappingFile); err != nil {
		return err
	}
	if err := writeStringWithLength(w, manifest.SSTableCompression); err != nil {
		return err
	}

	return nil
}

// readManifestBinary deserializes the SnapshotManifest from a binary format.
func readManifestBinary(r io.Reader) (*core.SnapshotManifest, error) {
	manifest := &core.SnapshotManifest{}

	// 1. Header
	var header core.FileHeader
	if err := binary.Read(r, binary.LittleEndian, &header); err != nil {
		if err == io.EOF {
			return nil, io.EOF // Handle empty file gracefully
		}
		return nil, fmt.Errorf("failed to read manifest header: %w", err)
	}
	if header.Magic != core.ManifestMagic {
		// This could be a JSON manifest. We can add a check here for backward compatibility.
		// For now, we'll just error out.
		return nil, fmt.Errorf("invalid binary manifest magic number. Got: %x", header.Magic)
	}

	// 2. Sequence Number
	if err := binary.Read(r, binary.LittleEndian, &manifest.SequenceNumber); err != nil {
		return nil, fmt.Errorf("failed to read sequence number: %w", err)
	}

	// 3. Number of Levels
	var numLevels uint32
	if err := binary.Read(r, binary.LittleEndian, &numLevels); err != nil {
		return nil, fmt.Errorf("failed to read number of levels: %w", err)
	}
	manifest.Levels = make([]core.SnapshotLevelManifest, numLevels)

	// 4. Levels and Tables
	for i := uint32(0); i < numLevels; i++ {
		var levelNum, numTables uint32
		if err := binary.Read(r, binary.LittleEndian, &levelNum); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.LittleEndian, &numTables); err != nil {
			return nil, err
		}

		manifest.Levels[i].LevelNumber = int(levelNum)
		manifest.Levels[i].Tables = make([]core.SSTableMetadata, numTables)

		for j := uint32(0); j < numTables; j++ {
			var table core.SSTableMetadata
			if err := binary.Read(r, binary.LittleEndian, &table.ID); err != nil {
				return nil, err
			}

			fileNameBytes, err := readBytesWithLength(r)
			if err != nil {
				return nil, err
			}
			table.FileName = string(fileNameBytes)

			table.MinKey, err = readBytesWithLength(r)
			if err != nil {
				return nil, err
			}

			table.MaxKey, err = readBytesWithLength(r)
			if err != nil {
				return nil, err
			}

			manifest.Levels[i].Tables[j] = table
		}
	}

	// 5. Auxiliary Files
	var err error
	manifest.WALFile, err = readStringWithLength(r)
	if err != nil {
		return nil, err
	}
	manifest.DeletedSeriesFile, err = readStringWithLength(r)
	if err != nil {
		return nil, err
	}
	manifest.RangeTombstonesFile, err = readStringWithLength(r)
	if err != nil {
		return nil, err
	}
	manifest.StringMappingFile, err = readStringWithLength(r)
	if err != nil {
		return nil, err
	}
	manifest.SeriesMappingFile, err = readStringWithLength(r)
	if err != nil {
		return nil, err
	}
	manifest.SSTableCompression, err = readStringWithLength(r)
	if err != nil {
		return nil, err
	}

	return manifest, nil
}
