//go:build linux

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"

	"github.com/INLOpen/nexusbase/sys"
)

func main() {
	dir := flag.String("dir", "/tmp", "directory to create test file in")
	size := flag.Int64("size", 16*1024*1024, "bytes to attempt to preallocate")
	flag.Parse()

	now := time.Now().UnixNano()
	tmp := filepath.Join(*dir, fmt.Sprintf("nexusbase_prealloc_check_%d.tmp", now))
	fmt.Printf("Creating test file: %s\n", tmp)

	f, err := os.Create(tmp)
	if err != nil {
		fmt.Printf("failed to create file: %v\n", err)
		os.Exit(2)
	}
	defer func() {
		f.Close()
		os.Remove(tmp)
	}()

	// Try unix.Fallocate with KEEP_SIZE first and report exact error values.
	fd := int(f.Fd())
	fmt.Printf("Attempting unix.Fallocate(fd=%d, KEEP_SIZE, 0, %d)\n", fd, *size)
	if err := unix.Fallocate(fd, unix.FALLOC_FL_KEEP_SIZE, 0, *size); err != nil {
		// Print raw error and errno numeric when available.
		fmt.Printf("unix.Fallocate KEEP_SIZE failed: %v\n", err)
		if errno, ok := err.(unix.Errno); ok {
			fmt.Printf("errno numeric: %d\n", int(errno))
		}
	} else {
		fmt.Printf("unix.Fallocate KEEP_SIZE succeeded\n")
	}

	// Try plain fallocate (may change file size) as fallback.
	fmt.Printf("Attempting unix.Fallocate(fd=%d, 0, 0, %d)\n", fd, *size)
	if err := unix.Fallocate(fd, 0, 0, *size); err != nil {
		fmt.Printf("unix.Fallocate plain failed: %v\n", err)
		if errno, ok := err.(unix.Errno); ok {
			fmt.Printf("errno numeric: %d\n", int(errno))
		}
	} else {
		fmt.Printf("unix.Fallocate plain succeeded\n")
	}

	// Now call the repository wrapper so we see the same result users get via sys.Preallocate.
	fmt.Printf("Calling repo sys.Preallocate(file, %d)\n", *size)
	if err := sys.Preallocate(f, *size); err != nil {
		fmt.Printf("sys.Preallocate returned: %v\n", err)
	} else {
		fmt.Printf("sys.Preallocate returned: nil (success)\n")
	}

	fmt.Println("Done")
}
