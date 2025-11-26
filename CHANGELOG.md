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


## Past Releases

- Initial project history is maintained in VCS and other release notes.
