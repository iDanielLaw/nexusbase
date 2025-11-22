package sys

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// AcquireFileLock tries to create a lock file at path + ".lock" using an
// atomic create (O_EXCL). It retries up to maxRetries with retryInterval.
// On success it returns a release function that removes the lock file.
// AcquireFileLock tries to create a lock file at path + ".lock" using an
// atomic create (O_EXCL). It retries up to maxRetries with retryInterval.
// If staleTTL > 0, an existing lock file older than staleTTL (based on the
// recorded timestamp inside the lockfile or file modtime) will be removed and
// acquisition retried. On success it returns a release function that removes
// the lock file only if it still belongs to this process (pid/timestamp match).
func AcquireFileLock(path string, maxRetries int, retryInterval time.Duration, staleTTL time.Duration) (func() error, error) {
	lockPath := path + ".lock"
	var lastErr error
	// Values we'll record when we successfully create the lock file.
	var ourPid int
	var ourTimestamp int64
	for i := 0; i <= maxRetries; i++ {
		// If the lock file already exists, check its timestamp and decide
		// whether it's considered stale. If it's fresh (age <= staleTTL) then
		// respect it and wait; if stale and staleTTL>0, try removing it.
		if info, serr := os.Stat(lockPath); serr == nil {
			if staleTTL > 0 {
				// try read timestamp from file contents
				b, rerr := os.ReadFile(lockPath)
				var fileTs int64 = 0
				if rerr == nil {
					// Prefer binary format only if the file doesn't look like the legacy
					// text format (which contains newlines). This ensures backward
					// compatibility with existing text lock files that are longer
					// than 12 bytes.
					asStr := string(b)
					if !strings.Contains(asStr, "\n") && len(b) >= 12 {
						// timestamp placed after pid
						fileTs = int64(binary.LittleEndian.Uint64(b[4:12]))
					} else {
						// Fallback to legacy text format (pid\nunixnano\n)
						parts := strings.Split(strings.TrimSpace(asStr), "\n")
						if len(parts) >= 2 {
							if tsParsed, perr := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); perr == nil {
								fileTs = tsParsed
							}
						}
					}
				}
				var age time.Duration
				now := time.Now().UTC()
				if fileTs > 0 {
					age = now.Sub(time.Unix(0, fileTs))
				} else {
					age = now.Sub(info.ModTime())
				}
				if age <= staleTTL {
					// fresh lock: wait and retry
					time.Sleep(retryInterval)
					continue
				}
				// stale: try to remove and continue to acquisition attempts
				_ = os.Remove(lockPath)
				// small backoff
				time.Sleep(10 * time.Millisecond)
			} else {
				// No staleTTL provided, just wait and retry
				time.Sleep(retryInterval)
				continue
			}
		}
		// First, try platform-native lock if available.
		if rel, err := AcquireOSFileLock(lockPath, 0); err == nil {
			// Successfully acquired OS-level lock (non-blocking). Write our pid/timestamp
			// into the file for diagnostics and return a release function that unlocks.
			// Note: AcquireOSFileLock opened the file and will remove it on release,
			// so we can just write into it now.
			// Attempt to write pid/timestamp into the file; ignore errors.
			// write binary: pid (uint32) followed by unixnano timestamp (uint64)
			buf := make([]byte, 12)
			binary.LittleEndian.PutUint32(buf[0:4], uint32(os.Getpid()))
			binary.LittleEndian.PutUint64(buf[4:12], uint64(time.Now().UTC().UnixNano()))
			_ = os.WriteFile(lockPath, buf, 0644)
			return rel, nil
		}

		// Fallback: Attempt to create lock file atomically
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err == nil {
			// write pid and unixnano timestamp for easy parsing
			ourPid = os.Getpid()
			ourTimestamp = time.Now().UTC().UnixNano()
			// write binary: pid (uint32) followed by unixnano timestamp (uint64)
			buf := make([]byte, 12)
			binary.LittleEndian.PutUint32(buf[0:4], uint32(ourPid))
			binary.LittleEndian.PutUint64(buf[4:12], uint64(ourTimestamp))
			_, _ = f.Write(buf)
			f.Close()
			// success
			release := func() error {
				// Only remove if the lock file still contains our pid and timestamp.
				b, err := os.ReadFile(lockPath)
				if err != nil {
					// if file is already gone, treat as success
					if os.IsNotExist(err) {
						return nil
					}
					return err
				}
				// Prefer binary format only if file doesn't look like legacy text (contains newlines)
				asStr := string(b)
				if !strings.Contains(asStr, "\n") && len(b) >= 12 {
					pidFromFile := int(binary.LittleEndian.Uint32(b[0:4]))
					tsFromFile := int64(binary.LittleEndian.Uint64(b[4:12]))
					if pidFromFile == ourPid && tsFromFile == ourTimestamp {
						return os.Remove(lockPath)
					}
					return nil
				}
				// Fallback to legacy text parsing
				parts := strings.Split(strings.TrimSpace(asStr), "\n")
				if len(parts) >= 2 {
					pidStr := strings.TrimSpace(parts[0])
					tsStr := strings.TrimSpace(parts[1])
					if pidStr == strconv.Itoa(ourPid) && tsStr == strconv.FormatInt(ourTimestamp, 10) {
						return os.Remove(lockPath)
					}
					return nil
				}
				// Unexpected content — avoid removing
				return nil
			}
			return release, nil
		}
		lastErr = err

		// If file exists, consider stale-break logic
		if os.IsExist(err) && staleTTL > 0 {
			// First, try reading the file contents to get the recorded timestamp.
			b, rerr := os.ReadFile(lockPath)
			var fileTs int64 = 0
			if rerr == nil {
				parts := strings.Split(strings.TrimSpace(string(b)), "\n")
				if len(parts) >= 2 {
					// parts[1] expected to be unixnano timestamp
					tsStr := strings.TrimSpace(parts[1])
					if tsParsed, perr := strconv.ParseInt(tsStr, 10, 64); perr == nil {
						fileTs = tsParsed
					}
				}
			}

			now := time.Now().UTC()
			// If we have a parsed timestamp, use it; otherwise fall back to file modtime.
			var age time.Duration
			if fileTs > 0 {
				age = now.Sub(time.Unix(0, fileTs))
			} else if info, serr := os.Stat(lockPath); serr == nil {
				age = now.Sub(info.ModTime())
			}
			if age > staleTTL {
				// Attempt to remove stale lock file. This may race with other processes;
				// if removal fails we will just sleep and retry.
				_ = os.Remove(lockPath)
				// small backoff to let the filesystem settle before retrying create
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		// Otherwise wait and retry
		time.Sleep(retryInterval)
	}
	if lastErr == nil {
		lastErr = errors.New("failed to acquire lock")
	}
	return nil, fmt.Errorf("AcquireFileLock: %w", lastErr)
}

// DefaultLockStaleTTL is the default TTL used when breaking stale lock files
// if callers choose to use the package default rather than specifying one.
var DefaultLockStaleTTL = 30 * time.Second

// SetDefaultLockStaleTTL updates the package default TTL used by callers that
// rely on `DefaultLockStaleTTL`.
func SetDefaultLockStaleTTL(d time.Duration) {
	DefaultLockStaleTTL = d
}
