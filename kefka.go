package kefka

type MarshallFunc func(v any) ([]byte, error)

type UnmarshallFunc func(data []byte, v any) error

type logLevel uint

const (
	DebugLevel logLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

type Logger interface {
	Printf(logLevel, string, ...any)
}

type LoggerFunc func(logLevel, string, ...any)

func (l LoggerFunc) Printf(level logLevel, msg string, args ...any) {
	l(level, msg, args...)
}
