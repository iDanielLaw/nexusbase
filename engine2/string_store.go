package engine2

import "sync"

// StringStore is a small in-memory string <-> id mapping used by some
// engine2 tests and utilities. It is intentionally minimal — thread-safe
// and deterministic. Tests and utilities can use this lightweight store
// for engine-local string-id mappings.
type StringStore struct {
	mu      sync.RWMutex
	idByStr map[string]uint64
	strs    []string
}

// NewStringStore creates an initialized StringStore.
func NewStringStore() *StringStore {
	return &StringStore{
		idByStr: make(map[string]uint64),
		strs:    make([]string, 0),
	}
}

// Add inserts the given string and returns its assigned id. If the string
// already exists, the existing id is returned.
func (s *StringStore) Add(str string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.idByStr[str]; ok {
		return id
	}
	id := uint64(len(s.strs))
	s.strs = append(s.strs, str)
	s.idByStr[str] = id
	return id
}

// Lookup returns the id for the given string and whether it exists.
func (s *StringStore) Lookup(str string) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.idByStr[str]
	return id, ok
}

// Get returns the string for the given id and whether that id is valid.
func (s *StringStore) Get(id uint64) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if int(id) < 0 || int(id) >= len(s.strs) {
		return "", false
	}
	return s.strs[id], true
}

// Len returns number of stored strings.
func (s *StringStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.strs)
}

// Methods to satisfy indexer.StringStoreInterface used by tests.
func (s *StringStore) GetOrCreateID(str string) (uint64, error) {
	id := s.Add(str)
	return id, nil
}

func (s *StringStore) GetString(id uint64) (string, bool) { return s.Get(id) }

func (s *StringStore) GetID(str string) (uint64, bool) { return s.Lookup(str) }

func (s *StringStore) Sync() error { return nil }

func (s *StringStore) Close() error { return nil }

func (s *StringStore) LoadFromFile(dataDir string) error { return nil }
