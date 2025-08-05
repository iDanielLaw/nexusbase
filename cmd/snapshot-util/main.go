package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/INLOpen/nexusbase/core"
	// We need access to the unexported readManifestBinary function.
	// For this utility, we might need to make it public or copy its logic.
	// Let's assume we have a way to call it, perhaps via a new `snapshot` package.
	// For this example, I'll copy the necessary function logic.
)

// SnapshotInfo holds display-friendly information about a single snapshot.
type SnapshotInfo struct {
    Timestamp    time.Time
    Type         string // "FULL" or "INCR"
    Size         int64  // Approximate size on disk
    ManifestPath string
}

func main() {
    baseDir := flag.String("base-dir", "", "The base directory containing the snapshot chain (required)")
    flag.Parse()

    if *baseDir == "" {
        log.Fatal("Error: -base-dir flag is required.")
    }

    snapshots, err := listSnapshots(*baseDir)
    if err != nil {
        log.Fatalf("Error listing snapshots: %v", err)
    }

    if len(snapshots) == 0 {
        fmt.Println("No snapshots found.")
        return
    }

    // Print results in a table
    w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
    fmt.Fprintln(w, "TIMESTAMP\tTYPE\tSIZE (MB)\tMANIFEST PATH")
    fmt.Fprintln(w, "---------\t----\t---------\t-------------")

    for _, s := range snapshots {
        sizeMB := float64(s.Size) / (1024 * 1024)
        fmt.Fprintf(w, "%s\t%s\t%.2f\t%s\n",
            s.Timestamp.Format("2006-01-02 15:04:05 MST"),
            s.Type,
            sizeMB,
            s.ManifestPath,
        )
    }
    w.Flush()
}

// listSnapshots implements the chain traversal logic.
func listSnapshots(baseDir string) ([]SnapshotInfo, error) {
    var snapshots []SnapshotInfo

    currentFilePath := filepath.Join(baseDir, "CURRENT")
    manifestRelativePathBytes, err := os.ReadFile(currentFilePath)
    if err != nil {
        if os.IsNotExist(err) {
            return nil, nil // No snapshots exist yet
        }
        return nil, fmt.Errorf("failed to read CURRENT file: %w", err)
    }

    currentManifestRelativePath := strings.TrimSpace(string(manifestRelativePathBytes))

    for currentManifestRelativePath != "" {
        manifestPath := filepath.Join(baseDir, currentManifestRelativePath)
        snapshotDir := filepath.Dir(manifestPath)

        file, err := os.Open(manifestPath)
        if err != nil {
            return nil, fmt.Errorf("failed to open manifest %s: %w", manifestPath, err)
        }

        // NOTE: This assumes readManifestBinary is accessible.
        // You might need to move it to a shared package.
        manifest, err := readManifestBinary(file)
        file.Close()
        if err != nil {
            return nil, fmt.Errorf("failed to read manifest %s: %w", manifestPath, err)
        }

        // Calculate approximate size of this snapshot's directory
        var dirSize int64
        filepath.Walk(snapshotDir, func(_ string, info os.FileInfo, err error) error {
            if err == nil && !info.IsDir() {
                dirSize += info.Size()
            }
            return nil
        })

        // Determine snapshot type
        snapshotType := "INCR"
        if manifest.ParentManifest == "" {
            snapshotType = "FULL"
        }

        // Extract timestamp from the directory name (e.g., "20060102T150405Z")
        dirName := filepath.Base(snapshotDir)
        ts, err := time.Parse("20060102T150405Z", dirName)
        if err != nil {
            // Fallback if parsing fails
            ts = time.Now()
        }

        snapshots = append(snapshots, SnapshotInfo{
            Timestamp:    ts,
            Type:         snapshotType,
            Size:         dirSize,
            ManifestPath: currentManifestRelativePath,
        })

        // Move to the parent
        currentManifestRelativePath = manifest.ParentManifest
    }

    return snapshots, nil
}

// NOTE: The following function is copied from engine/manifest_persistence.go
// For a real implementation, you should move this to a shared package.
func readManifestBinary(r io.Reader) (*core.SnapshotManifest, error) {
    // ... (implementation of readManifestBinary from engine/manifest_persistence.go)
    // This function would need to be made accessible to the utility.
    // For brevity, the full implementation is omitted here.
    // You would copy the exact code from the original file.
    return nil, fmt.Errorf("readManifestBinary not implemented in this example")
}
