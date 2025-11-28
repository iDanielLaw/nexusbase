package memtable

import (
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexuscore/utils/clock"
)

// TestMemtable2PutRawDefensiveCopy ensures PutRaw makes an internal copy of
// the provided value bytes so subsequent mutations of the caller's slice do
// not affect the stored memtable value.
func TestMemtable2PutRawDefensiveCopy(t *testing.T) {
	m := NewMemtable2(1<<20, clock.SystemClockDefault)
	defer m.Close()

	key := []byte("test-series-key-00000001")
	// original value buffer we will mutate after PutRaw
	val := []byte{0x01, 0x02, 0x03, 0x04}

	if err := m.PutRaw(key, val, core.EntryTypePutEvent, 1); err != nil {
		t.Fatalf("PutRaw failed: %v", err)
	}

	// Mutate the caller-owned slice
	val[0] = 0x99

	stored, et, ok := m.Get(key)
	if !ok {
		t.Fatalf("expected key to be present in memtable")
	}
	if et != core.EntryTypePutEvent {
		t.Fatalf("unexpected entry type: %v", et)
	}
	if len(stored) == 0 {
		t.Fatalf("stored value empty")
	}
	if stored[0] == 0x99 {
		t.Fatalf("memtable stored value was aliased to caller buffer")
	}
}
