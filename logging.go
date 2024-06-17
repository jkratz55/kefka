package kefka

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// NopLogger returns a logger that discards all log messages.
//
// This is useful for testing, however, it is not recommended to use this in
// production.
func NopLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// DefaultLogger returns a default configured logger that logs to stderr using
// JSON format. If the KEFKA_LOG_LEVEL environment variable is set and a valid
// value, the logger's level will be set to that value. Otherwise, the log level
// will default to ERROR.
func DefaultLogger() *slog.Logger {
	level := os.Getenv("KEFKA_LOG_LEVEL")
	leveler := new(slog.LevelVar)
	leveler.Set(parseLogLevel(level))
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     leveler,
	})
	return slog.New(handler)
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToUpper(level) {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		// If the level is not present or not valid default to ERROR
		return slog.LevelError
	}
}
