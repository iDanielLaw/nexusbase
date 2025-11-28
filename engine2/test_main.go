package engine2

import (
	"log/slog"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// Set global default logger to only WARN+ errors
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	os.Exit(m.Run())
}
