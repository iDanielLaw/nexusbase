//go:build linux

package sys

import (
	"errors"
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

// (prealloc cache and counters are declared in prealloc_counters.go)

// Preallocate attempts to allocate space for the given file without changing
// the visible file size using fallocate where available. On filesystems that
// don't support KEEP_SIZE, it will attempt a best-effort allocation.
func Preallocate(f FileHandle, size int64) error {
	if size <= 0 {
		return nil
	}
	fg, ok := f.(interface{ Fd() uintptr })
	if !ok {
		return ErrPreallocNotSupported
	}
	fd := int(fg.Fd())

	// Use a cache keyed by device id to avoid repeated fstatfs calls for files
	// on the same mounted device. The package-level `preallocCache` stores a
	// boolean indicating whether preallocation is allowed on that device.

	// If the file path is on a Windows mount under /mnt, skip preallocation.
	// This is a common case with WSL where the underlying filesystem does not
	// support the fallocate syscall in a way we can use. Treat as non-fatal.
	if path := f.Name(); path != "" {
		if strings.HasPrefix(path, "/mnt/") {
			return ErrPreallocNotSupported
		}
	}

	// Get the device ID for the underlying file so we can cache per-device
	// decisions (avoids repeated fstatfs calls for many files on the same mount).
	var stat unix.Stat_t
	var dev uint64
	if err := unix.Fstat(fd, &stat); err == nil {
		dev = uint64(stat.Dev)
		if allow, ok := preallocCacheLoad(dev); ok {
			preallocCacheHit()
			if !allow {
				return ErrPreallocNotSupported
			}
			// allowed: proceed directly to fallocate
			if err := unix.Fallocate(fd, unix.FALLOC_FL_KEEP_SIZE, 0, size); err == nil {
				return nil
			}
			if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTTY) {
				return ErrPreallocNotSupported
			}
			if err := unix.Fallocate(fd, 0, 0, size); err == nil {
				return nil
			}
			if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTTY) {
				return ErrPreallocNotSupported
			}
			return fmt.Errorf("preallocation failed for fd=%d: %w", fd, err)
		}
		// not cached: record a miss and fall through to fstatfs and compute allowed status
		preallocCacheMiss()
	}

	// Try fallocate with KEEP_SIZE (allocate blocks without changing file size).

	// Quick fstatfs check: ensure the filesystem type is a typical local
	// filesystem. If it's unknown or network/virtual, skip preallocation.
	var st unix.Statfs_t
	if err := unix.Fstatfs(fd, &st); err != nil {
		return ErrPreallocNotSupported
	}

	switch st.Type {
	case 0xEF53, // EXT2/3/4
		0x58465342, // XFS
		0x9123683E, // BTRFS
		0x01021994, // TMPFS
		0x794C7630, // OVERLAYFS
		0xF2F52010, // F2FS
		0x2FC12FC1, // ZFS on Linux
		0x2011BAB0, // EXFAT
		0x9660,     // ISO9660 / CD-ROM (may be read-only)
		0x73717368, // SQUASHFS
		0x3434,     // NILFS2 (placeholder common value)
		0x42465331: // BFS (BeOS FS) - unlikely but harmless to include
		// allowed (local filesystems)
	default:
		return ErrPreallocNotSupported
	}

	if err := unix.Fallocate(fd, unix.FALLOC_FL_KEEP_SIZE, 0, size); err == nil {
		// cache positive result per device when available
		if dev != 0 {
			preallocCacheStore(dev, true)
		}
		return nil
	} else {
		// If fallocate returns a known "not supported"/invalid error, map to sentinel.
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTTY) {
			if dev != 0 {
				preallocCacheStore(dev, false)
			}
			return ErrPreallocNotSupported
		}
		// Otherwise try a plain fallocate which may change file size.
	}

	if err := unix.Fallocate(fd, 0, 0, size); err == nil {
		if dev != 0 {
			preallocCacheStore(dev, true)
		}
		return nil
	} else {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTTY) {
			if dev != 0 {
				preallocCacheStore(dev, false)
			}
			return ErrPreallocNotSupported
		}
		return fmt.Errorf("preallocation failed for fd=%d: %w", fd, err)
	}
}
