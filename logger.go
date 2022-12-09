package kefka

import (
	"fmt"
	"log"
	"os"
)

// LogLevel represents the level/importance of a log message
type LogLevel uint

const (
	// DebugLevel is for logs that are useful for debugging or tracing the behavior
	// of the code.
	DebugLevel LogLevel = iota
	// InfoLevel is for information log messages that do not need to be actioned but
	// are still useful.
	InfoLevel
	// WarnLevel is for log messages that may need to be actioned/addressed but aren't
	// necessarily errors.
	WarnLevel
	// ErrorLevel is for logs regarding errors
	ErrorLevel
	// FatalLevel are for catastrophic errors for which the system cannot recover or
	// continue
	FatalLevel
)

// Logger is an interface type that defines the logging behavior of Kefka. This
// interface can be implemented to allow any third party logger to integrate into
// Kefka.
type Logger interface {
	Printf(lvl LogLevel, format string, args ...any)
}

// LoggerFunc is a convenient type to implement Logger interface without needing
// to create a new type.
type LoggerFunc func(lvl LogLevel, format string, args ...any)

func (l LoggerFunc) Printf(level LogLevel, msg string, args ...any) {
	l(level, msg, args...)
}

// logger is a default implementation of the Logger interface using the Logger
// type from the standard library.
type logger struct {
	logger *log.Logger
	level  LogLevel
}

// defaultLogger returns a default implementation of the Logger interface which
// logs messages to stderr using the Logger from the standard library.
func defaultLogger() *logger {
	return &logger{
		logger: log.New(os.Stderr, "", log.LstdFlags),
		level:  InfoLevel,
	}
}

func (l *logger) Printf(lvl LogLevel, format string, args ...any) {
	if lvl >= l.level {
		l.logger.Printf("%s %s", mapLevel(lvl), fmt.Sprintf(format, args...))
	}
}

// mapLevel maps LogLevel from Kefka to a text representation for logging
//
// If somehow the level isn't an expected value an empty string is returned.
func mapLevel(lvl LogLevel) string {
	switch lvl {
	case DebugLevel:
		return "[DEBUG]"
	case InfoLevel:
		return "[INFO]"
	case WarnLevel:
		return "[WARN]"
	case ErrorLevel:
		return "[ERROR]"
	case FatalLevel:
		return "[FATAL]"
	default:
		return ""
	}
}
