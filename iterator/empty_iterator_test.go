package iterator

import (
	"testing"
)

func TestEmptyIterator(t *testing.T) {
	iter := NewEmptyIterator()

	// Next() should always be false
	if iter.Next() {
		t.Error("Expected Next() to be false, but got true")
	}

	key, value, entryType, seqNo := iter.At()
	// All other methods should return nil or zero values
	if key != nil {
		t.Errorf("Expected Key() to be nil, got %v", key)
	}
	if value != nil {
		t.Errorf("Expected Value() to be nil, got %v", value)
	}
	if entryType != 0 {
		t.Errorf("Expected EntryType() to be 0, got %v", entryType)
	}
	if seqNo != 0 {
		t.Errorf("Expected SequenceNumber() to be 0, got %v", seqNo)
	}
	if iter.Error() != nil {
		t.Errorf("Expected Error() to be nil, got %v", iter.Error())
	}
	if err := iter.Close(); err != nil {
		t.Errorf("Expected Close() to return nil, got %v", err)
	}

	// Calling Next() again should still be false
	if iter.Next() {
		t.Error("Expected Next() to be false after first call, but got true")
	}
}
