# Changelog

All notable changes to this project are documented in this file.

## Unreleased

- Deprecated: `memtable.Memtable` replaced by `memtable.Memtable2`.
  A compatibility wrapper (`memtable.NewMemtable`) now delegates to
  `Memtable2` to preserve the legacy API surface during the transition.

  Removal timeline: The legacy `Memtable` implementation is deprecated and
  scheduled for removal in the next major release (target: `v1.0.0`).
  Please migrate callsites to use `memtable.NewMemtable2(...)` or the
  `Memtable2` type directly before that release.

  Quick migration:

  - Before (legacy):

    ```go
    m := memtable.NewMemtable(threshold, clk)
    m.Put(key, value, core.EntryTypePutEvent, pointID)
    ```

  - After (recommended):

    ```go
    m := memtable.NewMemtable2(threshold, clk)
    m.Put(dp)            // datapoint-centric API
    // or
    m.PutRaw(key, value, core.EntryTypePutEvent, pointID)
    ```

- Engine: Add configurable SSTable writer options
  - New config keys under `engine.sstable`:
    - `prealloc_multiplier_bytes_per_key` — bytes reserved per estimated key (default 128).
    - `restart_point_interval` — optional restart-point interval for block encoding.
    - `preallocate` — enable best-effort file preallocation for SSTable writes.
  - These options are wired through `engine2.StorageEngineOptions` and used by SSTable writers and compaction.
  - Fixed compressor selection in server startup to use the correct constructors for Snappy/LZ4/ZSTD/None.


## Past Releases

- Initial project history is maintained in VCS and other release notes.
