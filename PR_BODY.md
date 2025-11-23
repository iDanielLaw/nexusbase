Title: docs + CI: document engine.max_chunk_bytes and add CI workflow

Summary:
- Add documentation to `configs/README.md` describing the new `engine.max_chunk_bytes` configuration option (default 16 KiB), recommended usage, and the atomic publish behavior used for `chunks.dat` (tmp -> fsync -> rename, fallback to copy+sync when rename fails).
- Add a GitHub Actions CI workflow `.github/workflows/ci.yml` that runs `go test ./...` on `ubuntu-latest` and `windows-latest` to exercise cross-platform behaviors (including rename/copy fallback tests).

Why:
- Documents runtime tuning and guarantees for the new engine2 index writer and chunk layout.
- CI ensures platform-specific behaviors (Windows rename/copy) are verified and prevents regressions.

Testing:
- Ran `gofmt -w .` and `go test ./...` locally on Windows; all packages passed.

Notes for reviewers:
- The CI workflow uses Go 1.20; if the project pins a different Go version in `go.mod`, consider updating the workflow to match.
- No functional code changes were made in this branch; only docs and CI files were added.
