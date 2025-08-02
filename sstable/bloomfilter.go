package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
)

// BloomFilter is a probabilistic data structure for membership testing.
type BloomFilter struct {
	bits      []byte
	numBits   uint64
	numHashes uint32
}

// NewBloomFilter creates a new Bloom Filter.
// `numElements` is the expected number of elements.
// `falsePositiveRate` is the desired false positive rate (e.g., 0.01 for 1%).
func NewBloomFilter(numElements uint64, falsePositiveRate float64) (*BloomFilter, error) {
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		return nil, errors.New("invalid arguments for NewBloomFilter: falsePositiveRate must be (0, 1)")
	}
	if numElements == 0 {
		// Handle 0 elements by creating a minimal valid bloom filter.
		// This is useful for empty SSTables that still need a valid (though empty) filter.
		return &BloomFilter{bits: make([]byte, 1), numBits: 8, numHashes: 1}, nil
	}

	m := uint64(math.Ceil(float64(numElements) * math.Abs(math.Log(falsePositiveRate)) / (math.Log(2) * math.Log(2))))
	k := uint32(math.Ceil((float64(m) / float64(numElements)) * math.Log(2)))

	if m%8 != 0 {
		m = (m/8 + 1) * 8
	}
	if m == 0 {
		m = 8 // Smallest possible multiple of 8
	}
	if k == 0 { // Ensure at least one hash function
		k = 1
	}

	return &BloomFilter{
		bits:      make([]byte, m/8),
		numBits:   m,
		numHashes: k,
	}, nil
}

// Add adds a key to the Bloom Filter.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := fnvHash(key)
	for i := uint32(0); i < bf.numHashes; i++ {
		idx := (uint64(h1) + uint64(i)*uint64(h2)) % bf.numBits
		byteIndex := idx / 8
		bitIndex := idx % 8
		bf.bits[byteIndex] |= (1 << bitIndex)
	}
}

// Contains checks if a key might be in the Bloom Filter.
func (bf *BloomFilter) Contains(key []byte) bool {
	if bf == nil || len(bf.bits) == 0 { // Handle nil or uninitialized filter
		return false // Or true, depending on desired behavior for empty/nil filter
	}
	h1, h2 := fnvHash(key)
	for i := uint32(0); i < bf.numHashes; i++ {
		idx := (uint64(h1) + uint64(i)*uint64(h2)) % bf.numBits
		byteIndex := idx / 8
		bitIndex := idx % 8
		if (bf.bits[byteIndex]>>(bitIndex))&1 == 0 {
			return false
		}
	}
	return true
}

func fnvHash(data []byte) (uint32, uint32) {
	// Use FNV-1a 64-bit hash and split it into two 32-bit hashes.
	// This is a common and generally effective way to generate two "independent" hash functions
	// for Bloom filters from a single hash computation.
	h := fnv.New64a()
	h.Write(data)
	hash64 := h.Sum64()
	h1 := uint32(hash64)
	h2 := uint32(hash64 >> 32)
	return h1, h2
}

// Bytes returns the filter data as a byte slice, implementing the filter.Filter interface.
func (bf *BloomFilter) Bytes() []byte {
	buf := make([]byte, 8+4+len(bf.bits))
	binary.LittleEndian.PutUint64(buf[0:8], bf.numBits)
	binary.LittleEndian.PutUint32(buf[8:12], bf.numHashes)
	copy(buf[12:], bf.bits)
	return buf
}

func DeserializeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 12 {
		return nil, errors.New("invalid bloom filter data: too short")
	}
	numBits := binary.LittleEndian.Uint64(data[0:8])
	numHashes := binary.LittleEndian.Uint32(data[8:12])
	bits := data[12:]
	if numBits == 0 || numHashes == 0 || uint64(len(bits)*8) != numBits {
		return nil, fmt.Errorf("invalid bloom filter data: inconsistent sizes. numBits: %d, numHashes: %d, actualBitsLen: %d", numBits, numHashes, len(bits)*8)
	}
	return &BloomFilter{
		bits:      bits,
		numBits:   numBits,
		numHashes: numHashes,
	}, nil
}
