package indexer

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestAddStringsBatch_Persistence(t *testing.T) {
	dir := t.TempDir()

	s := NewStringStore(slog.Default(), nil)
	if err := s.LoadFromFile(dir); err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	inputs := []string{"metric.a", "tag.k", "tag.v", "metric.a"}
	ids, err := s.AddStringsBatch(inputs)
	if err != nil {
		t.Fatalf("AddStringsBatch failed: %v", err)
	}
	if len(ids) != len(inputs) {
		t.Fatalf("expected %d ids, got %d", len(inputs), len(ids))
	}

	// Ensure GetID returns same ids
	for i, sstr := range inputs {
		id, ok := s.GetID(sstr)
		if !ok {
			t.Fatalf("expected id for %s to exist", sstr)
		}
		if id != ids[i] {
			t.Fatalf("id mismatch for %s: expected %d got %d", sstr, ids[i], id)
		}
	}

	// Close and reload from disk to verify persistence
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close string store: %v", err)
	}

	s2 := NewStringStore(slog.Default(), nil)
	if err := s2.LoadFromFile(dir); err != nil {
		t.Fatalf("LoadFromFile (second) failed: %v", err)
	}

	// Validate ids persisted
	for i, sstr := range inputs {
		id, ok := s2.GetID(sstr)
		if !ok {
			t.Fatalf("expected id for %s after reload", sstr)
		}
		if id != ids[i] {
			t.Fatalf("id mismatch after reload for %s: expected %d got %d", sstr, ids[i], id)
		}
	}

	// Ensure the log file exists (filename matches implementation)
	logPath := filepath.Join(dir, "string_mapping.log")
	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("expected log file at %s, stat err: %v", logPath, err)
	}
}
