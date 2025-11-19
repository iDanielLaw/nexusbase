<!-- Copilot / AI-agent instructions for the NexusBase repo -->

# NexusBase — Quick guide for AI coding assistants

This file captures the minimal, high-value knowledge an AI agent needs to be productive in this Go monorepo.

1. Big picture
   - NexusBase is a Go-based time-series DB implementing an LSM-style storage engine. Key runtime pieces live in `engine/`, `core/`, `memtable/`, `wal/`, `sstable/`, `levels/` and `index`-related code.
   - The server process is in `cmd/server` and composes: load config → create logger/tracer → create `engine.NewStorageEngine(opts)` → `dbEngine.Start()` → `server.NewAppServer(dbEngine, cfg, logger)` → `appServer.Start()`.
   - Hooks and extensibility: the engine exposes a HookManager (`GetHookManager`) — see `cmd/server/main.go` for hook registration examples (`hooks/listeners`).

2. Where to read to understand behavior
   - `cmd/server/main.go` — primary app lifecycle (logger, tracer, engine options, hook registration, graceful shutdown).
   - `config/config.go` and `cmd/server/config.yaml` — canonical config shape, defaults, and string-duration semantics (`config.ParseDuration`).
   - `engine/` — storage engine surface area and `StorageEngineOptions` used by callers.
   - `core/`, `memtable/`, `wal.go`, `sstable/`, and `levels/` — actual LSM components and compaction logic.
   - `compressors/` — compression implementations (lz4,zstd,snappy,none) used via config `engine.sstable.compression`.

3. Common developer workflows & commands
   - Build server: `go build -o tsdb-server ./cmd/server`
   - Run server with sample config: `./tsdb-server -config cmd/server/config.yaml` (on Windows: `tsdb-server.exe -config cmd\server\config.yaml`).
   - Load configuration programmatically: code calls `config.LoadConfig(path)` which returns defaults if file is missing.
   - Run tests: `go test ./...` (or test a package: `go test ./core -run TestName`).
   - Coverage HTML: `go test -coverprofile=coverage.out ./... ; go tool cover -html=coverage.out -o coverage.html`.
   - Formatting: `gofmt -w .` or `go fmt ./...`.

4. Project-specific conventions & patterns
   - Config values use YAML with many durations as strings (e.g., `"60s"`); use `config.ParseDuration` to interpret them.
   - Initialization order matters: create and start `engine` before constructing `AppServer` so engine-managed resources (hooks, metrics, stopper) are available.
   - Use the engine options struct (`engine.StorageEngineOptions`) for changes affecting disk/mem/compaction behavior — callers pass options from the parsed config.
   - Logging uses Go `log/slog` JSON handlers (see `cmd/server/createLogger`). Preserve structured logging fields when adding logs.
   - Tracing uses OpenTelemetry; initialization is done in `cmd/server` (`initTracerProvider`). When instrumenting, use the repo tracer provider patterns.
   - Compression is pluggable via `compressors/*` and selected by config — prefer using the existing compressor structs.

5. Integration points & external dependencies
   - gRPC server implementation lives in `server/` — clients under `clients/` (Go, Python, TypeScript) speak the same API.
   - Replication: `ReplicationConfig` in `config` controls leader/follower behavior. Look at `replication/` for protocol details.
   - Metrics: `server.NewMetricsServer` and the `/metrics` endpoint are toggled via `debug` config.
   - External systems: OpenTelemetry collectors (OTLP), optional pprof, and compression libs (lz4/zstd/snappy) are used.

6. What to search for when making a change
   - “GetHookManager” — if you need to register cross-cutting hooks/events.
   - `StorageEngineOptions` — when adding new engine configurations.
   - `ParseDuration` and `config.LoadConfig` — to respect config semantics and defaults.
   - Tests: search for `*_test.go` in the package you modify and update/extend tests there.

7. Examples to copy from
   - Engine creation + opts: `cmd/server/main.go` (use same pattern for options fields and logging/tracing hooks).
   - Compressor selection: `cmd/server/main.go` switch on `cfg.Engine.SSTable.Compression`.
   - Hook registration: `hookManager.Register(hooks.EventPostCompaction, listener)`.

8. Constraints and non-obvious pitfalls
   - Config defaults are defined in `config.Load` — changes to config shape should preserve defaults and YAML tags.
   - Startup order and graceful shutdown sequence are delicate (engine must be closed after `AppServer.Stop()` to flush/close resources). Follow `cmd/server/main.go` ordering.
   - Many packages rely on concrete option values; prefer extending options structs rather than adding globals.

If anything above is unclear or you'd like additional examples (e.g., sample PRs, test harnesses, or a small runnable example), tell me which area to expand and I'll iterate.
