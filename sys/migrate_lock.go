package sys

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// MigrateLockFileToBinary converts a legacy text lock file (pid\nunixnano\n)
// to the new binary format (uint32 pid + uint64 unixnano, LittleEndian).
// If the file is already in binary format, this is a no-op. The migration is
// performed atomically by writing to a temporary file and renaming it over
// the original.
func MigrateLockFileToBinary(lockPath string) error {
	b, err := os.ReadFile(lockPath)
	if err != nil {
		return err
	}

	asStr := string(b)
	// If it looks like binary (no newline and >=12 bytes), assume already migrated.
	if !strings.Contains(asStr, "\n") && len(b) >= 12 {
		return nil
	}

	// Otherwise try parse legacy text format. Be defensive: only treat as
	// legacy text if the first two lines appear to be ASCII digits (pid
	// and timestamp). If they contain other binary bytes (eg. accidental
	// newline in a binary file), treat the file as already-migrated/binary
	// and no-op rather than failing with a parse error.
	parts := strings.Split(strings.TrimSpace(asStr), "\n")
	if len(parts) < 2 {
		return errors.New("unrecognized lock file format")
	}
	pidStr := strings.TrimSpace(parts[0])
	tsStr := strings.TrimSpace(parts[1])

	// quick sanity: pidStr and tsStr should be all digits; otherwise assume
	// this is not a legacy text file (likely binary data that happened to
	// contain a newline) and skip migration.
	isDigits := func(s string) bool {
		if s == "" {
			return false
		}
		for i := 0; i < len(s); i++ {
			c := s[i]
			if c < '0' || c > '9' {
				return false
			}
		}
		return true
	}
	if !isDigits(pidStr) || !isDigits(tsStr) {
		// treat as already binary / non-legacy -> no-op
		return nil
	}

	pid, perr := strconv.ParseUint(pidStr, 10, 32)
	if perr != nil {
		return perr
	}
	ts, terr := strconv.ParseUint(tsStr, 10, 64)
	if terr != nil {
		return terr
	}

	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(pid))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(ts))

	// write to temp file (system temp dir)
	tmpFile, err := os.CreateTemp("", "lock_migrate_")
	if err != nil {
		// fallback: attempt to write directly
		return os.WriteFile(lockPath, buf, 0644)
	}
	tmpPath := tmpFile.Name()
	if _, werr := tmpFile.Write(buf); werr != nil {
		tmpFile.Close()
		_ = os.Remove(tmpPath)
		return werr
	}
	tmpFile.Close()

	// Rename over original
	if rerr := os.Rename(tmpPath, lockPath); rerr != nil {
		_ = os.Remove(tmpPath)
		return rerr
	}
	return nil
}

// MigrateLocksInDir walks `dir` recursively and attempts to migrate every file
// whose name ends with `.lock` from the legacy text format to the binary
// format. It returns the number of files migrated and an error if one or more
// migrations failed (the error will aggregate messages).
func MigrateLocksInDir(dir string) (int, error) {
	var migrated int
	var errs []string

	walkFn := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// collect and continue
			errs = append(errs, fmt.Sprintf("walk error %s: %v", path, err))
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".lock") {
			return nil
		}
		// Quick check: only attempt migrate if file looks like legacy text (contains newline)
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			errs = append(errs, fmt.Sprintf("read %s: %v", path, rerr))
			return nil
		}
		if !strings.Contains(string(b), "\n") && len(b) >= 12 {
			// already binary
			return nil
		}
		if merr := MigrateLockFileToBinary(path); merr != nil {
			errs = append(errs, fmt.Sprintf("migrate %s: %v", path, merr))
			return nil
		}
		migrated++
		return nil
	}

	if err := filepath.Walk(dir, walkFn); err != nil {
		errs = append(errs, fmt.Sprintf("walk failed: %v", err))
	}

	if len(errs) > 0 {
		return migrated, errors.New(strings.Join(errs, "; "))
	}
	return migrated, nil
}
