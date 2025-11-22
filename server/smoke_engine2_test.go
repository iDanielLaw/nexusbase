package server_test

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/engine2"
	"github.com/INLOpen/nexusbase/server"
)

// Smoke test: construct an Engine2 + Engine2Adapter and make sure NewAppServer accepts it.
func TestAppServerAcceptsEngine2Adapter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nexusbase-engine2-smoke-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	eng2, err := engine2.NewEngine2(tmpDir)
	if err != nil {
		t.Fatalf("NewEngine2 failed: %v", err)
	}
	ad := engine2.NewEngine2AdapterWithHooks(eng2, nil)
	if err := ad.Start(); err != nil {
		_ = ad.Close()
		t.Fatalf("failed to start engine2 adapter: %v", err)
	}
	defer ad.Close()

	// Build a minimal config with ports 0 so server doesn't listen on network.
	cfg := &config.Config{}
	cfg.Server.GRPCPort = 0
	cfg.Server.TCPPort = 0
	cfg.QueryServer.Enabled = false

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	appSrv, err := server.NewAppServer(ad, cfg, logger)
	if err != nil {
		t.Fatalf("NewAppServer failed with engine2 adapter: %v", err)
	}
	// No need to Start() — construction succeeded. Call Stop() to exercise cleanup path.
	appSrv.Stop()
}
