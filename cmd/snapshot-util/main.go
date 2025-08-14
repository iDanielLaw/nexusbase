package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"text/tabwriter"

	"github.com/INLOpen/nexusbase/snapshot"
)

// mockProvider is a minimal implementation of snapshot.EngineProvider
// needed to instantiate the snapshot manager. The ListSnapshots method
// does not actually use any provider methods, so this can be empty.
type mockProvider struct {
	snapshot.EngineProvider
	logger *slog.Logger
}

func (m *mockProvider) GetLogger() *slog.Logger { return m.logger }
func (m *mockProvider) CheckStarted() error     { return nil }

func main() {
	baseDir := flag.String("base-dir", "", "The base directory containing the snapshot directories (required)")
	flag.Parse()

	if *baseDir == "" {
		fmt.Fprintln(os.Stderr, "Error: -base-dir flag is required.")
		flag.Usage()
		os.Exit(1)
	}

	// The manager doesn't need a real provider for listing snapshots.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	manager := snapshot.NewManager(&mockProvider{logger: logger})

	snapshots, err := manager.ListSnapshots(*baseDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listing snapshots: %v\n", err)
		os.Exit(1)
	}

	if len(snapshots) == 0 {
		fmt.Println("No snapshots found.")
		return
	}

	// Print results in a table
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "ID\tTYPE\tCREATED AT\tSIZE (MB)\tPARENT ID")
	fmt.Fprintln(w, "--\t----\t----------\t---------\t---------")

	for _, s := range snapshots {
		sizeMB := float64(s.Size) / (1024 * 1024)
		fmt.Fprintf(w, "%s\t%s\t%s\t%.2f\t%s\n",
			s.ID,
			s.Type,
			s.CreatedAt.Format("2006-01-02 15:04:05 MST"),
			sizeMB,
			s.ParentID,
		)
	}
	w.Flush()
}
