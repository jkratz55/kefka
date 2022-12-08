package kefka

import (
	"fmt"
	"log"
	"os"
)

type LogLevel uint

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

type Logger interface {
	Printf(LogLevel, string, ...any)
}

type LoggerFunc func(LogLevel, string, ...any)

func (l LoggerFunc) Printf(level LogLevel, msg string, args ...any) {
	l(level, msg, args...)
}

type logger struct {
	logger *log.Logger
	level  LogLevel
}

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
