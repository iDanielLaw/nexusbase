package indexer

import (
	"encoding/binary"
	"testing"
)

func TestEncodeTagIndexKey_LengthAndEndianness(t *testing.T) {
	k := EncodeTagIndexKey(0x0102030405060708, 0x0a0b0c0d0e0f1011)
	if len(k) != 16 {
		t.Fatalf("expected key length 16, got %d", len(k))
	}
	// check first uint64 is big-endian
	a := binary.BigEndian.Uint64(k[0:8])
	b := binary.BigEndian.Uint64(k[8:16])
	if a != 0x0102030405060708 {
		t.Fatalf("unexpected first uint64: 0x%x", a)
	}
	if b != 0x0a0b0c0d0e0f1011 {
		t.Fatalf("unexpected second uint64: 0x%x", b)
	}
}

func TestEncodeTagIndexKey_Alias(t *testing.T) {
	a := EncodeTagIndexKey(1, 2)
	b := encodeTagIndexKey(1, 2)
	if string(a) != string(b) {
		t.Fatalf("alias mismatch: %v vs %v", a, b)
	}
}
