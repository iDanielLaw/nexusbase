package engine2

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/INLOpen/nexusbase/sys"
)

// Binary manifest format
// header: 4 bytes magic 'NBMF' + uint32 version (1)
// count: uint64 number of entries
// each entry:
//   id uint64
//   keyCount uint64
//   createdAt int64 (unix nano)
//   pathLen uint32
//   path bytes

type SSTableManifestEntry struct {
	ID        uint64
	FilePath  string
	KeyCount  uint64
	CreatedAt time.Time
}

type SSTableManifest struct {
	Entries []SSTableManifestEntry
}

var manifestMagic = [4]byte{'N', 'B', 'M', 'F'}

const manifestVersion uint32 = 1

// LoadManifest loads manifest from path. If missing, returns empty manifest.
func LoadManifest(path string) (SSTableManifest, error) {
	// Try primary path first, but if it fails attempt recovery from .tmp or .bak.
	m, err := tryLoadManifest(path)
	if err == nil {
		return m, nil
	}

	// Try tmp file as a recovery source.
	tmp := path + ".tmp"
	if _, statErr := sys.Stat(tmp); statErr == nil {
		if mm, e2 := tryLoadManifest(tmp); e2 == nil {
			// attempt to promote tmp to main path
			_ = sys.Rename(tmp, path)
			return mm, nil
		}
	}

	// Try bak file as a recovery source.
	bak := path + ".bak"
	if _, statErr := sys.Stat(bak); statErr == nil {
		if mm, e3 := tryLoadManifest(bak); e3 == nil {
			// restore bak to main path
			_ = sys.Rename(bak, path)
			return mm, nil
		}
	}

	return SSTableManifest{}, err
}

func tryLoadManifest(path string) (SSTableManifest, error) {
	var m SSTableManifest
	f, err := sys.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return SSTableManifest{}, nil
		}
		return m, err
	}
	defer f.Close()

	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return m, fmt.Errorf("failed to read manifest header: %w", err)
	}
	if magic != manifestMagic {
		return m, fmt.Errorf("invalid manifest magic")
	}
	var ver uint32
	if err := binary.Read(f, binary.BigEndian, &ver); err != nil {
		return m, fmt.Errorf("failed to read manifest version: %w", err)
	}
	if ver != manifestVersion {
		return m, fmt.Errorf("unsupported manifest version: %d", ver)
	}

	var count uint64
	if err := binary.Read(f, binary.BigEndian, &count); err != nil {
		return m, fmt.Errorf("failed to read manifest count: %w", err)
	}
	entries := make([]SSTableManifestEntry, 0, count)
	for i := uint64(0); i < count; i++ {
		var id uint64
		if err := binary.Read(f, binary.BigEndian, &id); err != nil {
			return m, fmt.Errorf("failed to read entry id: %w", err)
		}
		var keyCount uint64
		if err := binary.Read(f, binary.BigEndian, &keyCount); err != nil {
			return m, fmt.Errorf("failed to read entry keyCount: %w", err)
		}
		var created int64
		if err := binary.Read(f, binary.BigEndian, &created); err != nil {
			return m, fmt.Errorf("failed to read entry createdAt: %w", err)
		}
		var pathLen uint32
		if err := binary.Read(f, binary.BigEndian, &pathLen); err != nil {
			return m, fmt.Errorf("failed to read entry pathLen: %w", err)
		}
		pathBuf := make([]byte, pathLen)
		if _, err := io.ReadFull(f, pathBuf); err != nil {
			return m, fmt.Errorf("failed to read entry path: %w", err)
		}
		entries = append(entries, SSTableManifestEntry{
			ID:        id,
			FilePath:  string(pathBuf),
			KeyCount:  keyCount,
			CreatedAt: time.Unix(0, created),
		})
	}
	m.Entries = entries
	return m, nil
}

// SaveManifest writes the manifest atomically to the given path in binary format.
func SaveManifest(path string, m SSTableManifest) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	// acquire a cross-process lock for the manifest to avoid concurrent writers
	// staleTTL protects against dead locks from crashed writers.
	release, lerr := sys.AcquireFileLock(path, 30, 50*time.Millisecond, sys.DefaultLockStaleTTL)
	if lerr != nil {
		return lerr
	}
	defer func() {
		_ = release()
	}()

	f, err := sys.Create(tmp)
	if err != nil {
		return err
	}
	// write header
	if _, err := f.Write(manifestMagic[:]); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := binary.Write(f, binary.BigEndian, manifestVersion); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	count := uint64(len(m.Entries))
	if err := binary.Write(f, binary.BigEndian, count); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	for _, e := range m.Entries {
		if err := binary.Write(f, binary.BigEndian, e.ID); err != nil {
			f.Close()
			_ = os.Remove(tmp)
			return err
		}
		if err := binary.Write(f, binary.BigEndian, e.KeyCount); err != nil {
			f.Close()
			_ = os.Remove(tmp)
			return err
		}
		if err := binary.Write(f, binary.BigEndian, e.CreatedAt.UnixNano()); err != nil {
			f.Close()
			_ = os.Remove(tmp)
			return err
		}
		pathBytes := []byte(e.FilePath)
		pathLen := uint32(len(pathBytes))
		if err := binary.Write(f, binary.BigEndian, pathLen); err != nil {
			f.Close()
			_ = os.Remove(tmp)
			return err
		}
		if _, err := f.Write(pathBytes); err != nil {
			f.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	// If existing manifest exists, move it to a .bak before replacing. This
	// provides a recovery point if the rename fails or new file is corrupted.
	bak := path + ".bak"
	if _, statErr := sys.Stat(path); statErr == nil {
		// remove any stale bak first
		_ = sys.Remove(bak)
		if err := sys.Rename(path, bak); err != nil {
			// attempt to cleanup tmp and return error
			_ = sys.Remove(tmp)
			return err
		}
	}

	// move tmp into final path
	if err := sys.Rename(tmp, path); err != nil {
		// try to restore bak if present
		if _, sErr := sys.Stat(bak); sErr == nil {
			_ = sys.Rename(bak, path)
		}
		_ = sys.Remove(tmp)
		return err
	}

	// Success: remove backup if present
	_ = sys.Remove(bak)
	return nil
}

// AppendManifestEntry loads the manifest, appends the entry and saves it.
func AppendManifestEntry(path string, e SSTableManifestEntry) error {
	m, err := LoadManifest(path)
	if err != nil {
		return err
	}
	m.Entries = append(m.Entries, e)
	return SaveManifest(path, m)
}
