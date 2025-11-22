// Package indexer
//
// The `indexer` package implements the secondary tag index for NexusBase.
// See the repository documentation for the index disk format and glossary:
//   - docs/index-disk-format.md
//
// This package exposes the TagIndexManager responsible for memtables,
// index SSTable flush/compaction, and query merging. The on-disk key
// encoding used for index SSTables is documented in
// `docs/index-disk-format.md` and implemented in `keys.go`.
package indexer
