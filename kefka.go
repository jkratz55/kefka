package kefka

import (
	"bytes"
	"encoding/gob"
	"encoding/json"

	"github.com/vmihailenco/msgpack/v5"
)

type MarshallFunc func(v any) ([]byte, error)

type UnmarshallFunc func(data []byte, v any) error

func JsonMarshaller() MarshallFunc {
	return func(v any) ([]byte, error) {
		return json.Marshal(v)
	}
}

func JsonUnmarshaller() UnmarshallFunc {
	return func(data []byte, v any) error {
		return json.Unmarshal(data, v)
	}
}

func MsgpackMarshaller() MarshallFunc {
	return func(v any) ([]byte, error) {
		return msgpack.Marshal(v)
	}
}

func MsgpackUnmarshaller() UnmarshallFunc {
	return func(data []byte, v any) error {
		return msgpack.Unmarshal(data, v)
	}
}

func GobMarshaller() MarshallFunc {
	return func(v any) ([]byte, error) {
		buffer := &bytes.Buffer{}
		err := gob.NewEncoder(buffer).Encode(v)
		return buffer.Bytes(), err
	}
}

func GobUnmarshaller() UnmarshallFunc {
	return func(data []byte, v any) error {
		reader := bytes.NewReader(data)
		return gob.NewDecoder(reader).Decode(v)
	}
}

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
