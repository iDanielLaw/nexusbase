//go:build false
// +build false

package engine2

// Deprecated: previously provided an in-memory non-persistent string store.
// The implementation is retained here intentionally behind a build tag
// to avoid accidental compilation; prefer the persisted implementation in
// the `indexer` package (`indexer.StringStore`).
