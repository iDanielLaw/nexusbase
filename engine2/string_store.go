package engine2

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/INLOpen/nexusbase/indexer"
)

// simple in-memory string->id store for engine2. Not persisted.
type inMemoryStringStore struct {
	mu      sync.RWMutex
	strToID map[string]uint64
	idToStr map[uint64]string
	nextID  uint64
}

var _ indexer.StringStoreInterface = (*inMemoryStringStore)(nil)

func newInMemoryStringStore() *inMemoryStringStore {
	return &inMemoryStringStore{
		strToID: make(map[string]uint64),
		idToStr: make(map[uint64]string),
		nextID:  1,
	}
}

func (s *inMemoryStringStore) GetOrCreateID(str string) (uint64, error) {
	s.mu.RLock()
	if id, ok := s.strToID[str]; ok {
		s.mu.RUnlock()
		return id, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.strToID[str]; ok {
		return id, nil
	}
	id := atomic.AddUint64(&s.nextID, 1)
	s.strToID[str] = id
	s.idToStr[id] = str
	return id, nil
}

func (s *inMemoryStringStore) GetString(id uint64) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.idToStr[id]
	return v, ok
}

func (s *inMemoryStringStore) GetID(str string) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.strToID[str]
	return id, ok
}

func (s *inMemoryStringStore) Sync() error  { return nil }
func (s *inMemoryStringStore) Close() error { return nil }
func (s *inMemoryStringStore) LoadFromFile(dataDir string) error {
	return fmt.Errorf("not implemented")
}
