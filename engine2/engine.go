package engine2

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sys"
)

var (
	// ErrAlreadyExists is returned when a database already exists.
	ErrAlreadyExists = errors.New("database already exists")
	// ErrInvalidName is returned when a database name fails validation.
	ErrInvalidName = errors.New("invalid database name")
)

// Engine2 is a minimal engine implementation that manages per-database filesystem layout.
type Engine2 struct {
	dataRoot string
	mu       sync.Mutex
	wal      *WAL
	mem      *Memtable
}

// NewEngine2 constructs a new Engine2 rooted at dataRoot.
func NewEngine2(dataRoot string) (*Engine2, error) {
	if dataRoot == "" {
		return nil, fmt.Errorf("dataRoot must be specified")
	}
	if err := sys.MkdirAll(dataRoot, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data root: %w", err)
	}
	// initialize wal and memtable
	walPath := filepath.Join(dataRoot, "wal", "engine.wal")
	w, err := NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}
	m := NewMemtable()

	// replay WAL into memtable
	if err := w.Replay(func(dp *core.DataPoint) error {
		m.Put(dp)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	return &Engine2{dataRoot: dataRoot, wal: w, mem: m}, nil
}

func (e *Engine2) GetDataRoot() string { return e.dataRoot }

// Validate DB name: starts with letter, followed by letters, digits, underscore or hyphen, max 64 chars.
var dbNameRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]{0,63}$`)

// CreateDatabase creates the directory layout and writes metadata.
func (e *Engine2) CreateDatabase(ctx context.Context, name string, opts CreateDBOptions) error {
	if !dbNameRe.MatchString(name) {
		return ErrInvalidName
	}
	// reserved names
	if name == "system" || name == "internal" {
		return ErrInvalidName
	}

	dbMetaPath := filepath.Join(e.dataRoot, name, "metadata")

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, err := os.Stat(dbMetaPath); err == nil {
		// metadata already exists
		if opts.IfNotExists {
			return nil
		}
		return ErrAlreadyExists
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat metadata file: %w", err)
	}

	// create directories
	if err := EnsureDBDirs(e.dataRoot, name); err != nil {
		return fmt.Errorf("failed to create db dirs: %w", err)
	}

	meta := &DatabaseMetadata{
		CreatedAt:    time.Now().Unix(),
		Version:      1,
		LastSequence: 0,
		Options:      opts.Options,
	}

	if err := SaveMetadataAtomic(dbMetaPath, meta); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}
