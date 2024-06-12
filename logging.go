package kefka

import (
	"io"
	"log/slog"
	"os"
)

// NopLogger returns a logger that discards all log messages.
//
// This is useful for testing, however, it is not recommended to use this in
// production.
func NopLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func DefaultLogger() *slog.Logger {
	leveler := new(slog.LevelVar)
	leveler.Set(slog.LevelError)
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     leveler,
	})
	return slog.New(handler)
}
