//go:build linux

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"log/slog"

	"golang.org/x/sys/unix"

	"github.com/INLOpen/nexusbase/sys"
)

func main() {
	dir := flag.String("dir", "/tmp", "directory to create test file in")
	size := flag.Int64("size", 16*1024*1024, "bytes to attempt to preallocate")
	flag.Parse()

	now := time.Now().UnixNano()
	tmp := filepath.Join(*dir, fmt.Sprintf("nexusbase_prealloc_check_%d.tmp", now))
	slog.Info("Creating test file", "path", tmp)

	f, err := os.Create(tmp)
	if err != nil {
		slog.Error("failed to create file", "err", err)
		os.Exit(2)
	}
	defer func() {
		f.Close()
		sys.Remove(tmp)
	}()

	// Try unix.Fallocate with KEEP_SIZE first and report exact error values.
	fd := int(f.Fd())
	slog.Info("Attempting unix.Fallocate (KEEP_SIZE)", "fd", fd, "size", *size)
	if err := unix.Fallocate(fd, unix.FALLOC_FL_KEEP_SIZE, 0, *size); err != nil {
		// Print raw error and errno numeric when available.
		slog.Error("unix.Fallocate KEEP_SIZE failed", "err", err)
		if errno, ok := err.(unix.Errno); ok {
			slog.Error("errno numeric", "errno", int(errno))
		}
	} else {
		slog.Info("unix.Fallocate KEEP_SIZE succeeded", "fd", fd, "size", *size)
	}

	// Try plain fallocate (may change file size) as fallback.
	slog.Info("Attempting unix.Fallocate (plain)", "fd", fd, "size", *size)
	if err := unix.Fallocate(fd, 0, 0, *size); err != nil {
		slog.Error("unix.Fallocate plain failed", "err", err)
		if errno, ok := err.(unix.Errno); ok {
			slog.Error("errno numeric", "errno", int(errno))
		}
	} else {
		slog.Info("unix.Fallocate plain succeeded", "fd", fd, "size", *size)
	}

	// Now call the repository wrapper so we see the same result users get via sys.Preallocate.
	slog.Info("Calling repo sys.Preallocate", "size", *size)
	if err := sys.Preallocate(f, *size); err != nil {
		slog.Error("sys.Preallocate returned error", "err", err)
	} else {
		slog.Info("sys.Preallocate returned: nil (success)")
	}

	slog.Info("Done")
}
