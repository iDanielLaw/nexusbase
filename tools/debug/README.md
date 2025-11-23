# Debug Tools

This folder contains debugging and inspection tools for NexusBase.

How to build a tool:

```powershell
go build -o bin/<tool> ./tools/debug/<tool>
```

Examples:

```powershell
go build -o bin/inspect_wal ./tools/debug/inspect_wal
.
./bin/inspect_wal -dir C:\path\to\wal
```

Notes:
- These tools are intended for developer/debugging use only and are not built by default in CI.
- If you need to run them locally, build from this folder as shown above.
