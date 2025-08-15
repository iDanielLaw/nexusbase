package sstable

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func TestNewBloomFilter_ValidParameters(t *testing.T) {
	tests := []struct {
		name              string
		numElements       uint64
		falsePositiveRate float64
		expectError       bool
	}{
		{"typical", 1000, 0.01, false},
		{"large_elements", 100000, 0.001, false},
		{"small_elements", 10, 0.1, false},
		{"high_fpr", 100, 0.5, false},
		{"low_fpr", 100, 0.00001, false},
		{"zero_elements", 0, 0.01, false}, // Should create a minimal valid filter
		{"invalid_fpr_zero", 100, 0.0, true},
		{"invalid_fpr_one", 100, 1.0, true},
		{"invalid_fpr_negative", 100, -0.1, true},
		{"invalid_fpr_greater_than_one", 100, 1.1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf, err := NewBloomFilter(tt.numElements, tt.falsePositiveRate)
			if (err != nil) != tt.expectError {
				t.Errorf("NewBloomFilter() error = %v, expectError %v", err, tt.expectError)
				return
			}
			if tt.expectError {
				if bf != nil {
					t.Errorf("NewBloomFilter() returned non-nil filter for error case")
				}
			} else {
				if bf == nil {
					t.Errorf("NewBloomFilter() returned nil filter for valid parameters")
				}
				if tt.numElements == 0 {
					if len(bf.bits) == 0 || bf.numBits == 0 || bf.numHashes == 0 {
						t.Errorf("NewBloomFilter() for 0 elements created invalid filter: %+v", bf)
					}
				} else {
					if bf.numBits == 0 || bf.numHashes == 0 || len(bf.bits) == 0 {
						t.Errorf("NewBloomFilter() created filter with zero dimensions: %+v", bf)
					}
				}
			}
		})
	}
}

func TestBloomFilter_AddAndContains(t *testing.T) {
	bf, err := NewBloomFilter(100, 0.01) // Expect 100 elements, 1% FPR
	if err != nil {
		t.Fatalf("NewBloomFilter failed: %v", err)
	}

	keysToAdd := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("date"),
	}

	for _, key := range keysToAdd {
		bf.Add(key)
	}

	// Test contains for added keys (should all be true)
	for _, key := range keysToAdd {
		if !bf.Contains(key) {
			t.Errorf("Bloom filter should contain key %q, but it doesn't (False Negative)", key)
		}
	}

	// Test contains for non-existent keys (should mostly be false, but some might be true - False Positive)
	nonExistentKeys := [][]byte{
		[]byte("grape"),
		[]byte("kiwi"),
		[]byte("lemon"),
		[]byte("mango"),
	}

	for _, key := range nonExistentKeys {
		if bf.Contains(key) {
			t.Logf("Bloom filter unexpectedly contains key %q (False Positive)", key)
		}
	}
}

func TestBloomFilter_Serialization(t *testing.T) {
	originalBF, err := NewBloomFilter(50, 0.05)
	if err != nil {
		t.Fatalf("NewBloomFilter failed: %v", err)
	}

	keys := [][]byte{[]byte("test1"), []byte("test2"), []byte("test3")}
	for _, key := range keys {
		originalBF.Add(key)
	}

	serializedData := originalBF.Bytes()
	if len(serializedData) == 0 {
		t.Fatal("Serialized data is empty")
	}

	deserializedBF, err := DeserializeBloomFilter(serializedData)
	if err != nil {
		t.Fatalf("DeserializeBloomFilter failed: %v", err)
	}

	// Verify properties match
	if originalBF.numBits != deserializedBF.numBits {
		t.Errorf("numBits mismatch: got %d, want %d", deserializedBF.numBits, originalBF.numBits)
	}
	if originalBF.numHashes != deserializedBF.numHashes {
		t.Errorf("numHashes mismatch: got %d, want %d", deserializedBF.numHashes, originalBF.numHashes)
	}
	if !bytes.Equal(originalBF.bits, deserializedBF.bits) {
		t.Errorf("bits mismatch: got %v, want %v", deserializedBF.bits, originalBF.bits)
	}

	// Verify functionality after deserialization
	for _, key := range keys {
		if !deserializedBF.Contains(key) {
			t.Errorf("Deserialized Bloom filter should contain key %q, but it doesn't", key)
		}
	}

	// Test with corrupted/short data
	_, err = DeserializeBloomFilter([]byte{1, 2, 3})
	if err == nil {
		t.Error("DeserializeBloomFilter expected error for short data, got nil")
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	// This test is probabilistic and might fail occasionally.
	// As of Go 1.20, the recommended way is to create a local random source
	// for reproducible tests. We use a fixed seed here.
	r := rand.New(rand.NewSource(12345))

	numElements := uint64(1000)
	targetFPR := 0.01 // 1%
	testKeysCount := 10000

	bf, err := NewBloomFilter(numElements, targetFPR)
	if err != nil {
		t.Fatalf("NewBloomFilter failed: %v", err)
	}

	// Add known elements
	knownKeys := make([][]byte, numElements)
	for i := uint64(0); i < numElements; i++ {
		key := []byte(fmt.Sprintf("known_key_%d", i))
		bf.Add(key)
		knownKeys[i] = key
	}

	// Generate non-existent keys and count false positives
	falsePositives := 0
	for i := 0; i < testKeysCount; i++ {
		nonExistentKey := []byte(fmt.Sprintf("non_existent_key_%d_%d", i, r.Intn(1000000)))
		// Ensure the key is truly not among known keys (highly unlikely for random strings)
		isKnown := false
		for _, k := range knownKeys {
			if bytes.Equal(k, nonExistentKey) {
				isKnown = true
				break
			}
		}
		if isKnown {
			continue // Skip if by chance it's a known key
		}

		if bf.Contains(nonExistentKey) {
			falsePositives++
		}
	}

	actualFPR := float64(falsePositives) / float64(testKeysCount)
	t.Logf("Target FPR: %.4f, Actual FPR: %.4f (from %d non-existent keys)", targetFPR, actualFPR, testKeysCount)

	// Allow a reasonable margin of error for probabilistic tests
	// For 1% target, allow up to 1.5% or 2% for actual.
	// The actual FPR can vary, especially with a limited number of test keys.
	// A common heuristic is to allow actual FPR to be within 2x of target FPR, or a fixed small delta.
	if actualFPR > targetFPR*2 { // Adjust multiplier as needed based on test stability
		t.Errorf("Actual False Positive Rate (%.4f) is significantly higher than target (%.4f)", actualFPR, targetFPR)
	}
}
