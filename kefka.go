package kefka

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	// LastOffset specifies to use the most recent offset available for a partition
	LastOffset kafka.Offset = -1
	// FirstOffset specifies to use the least recent offset available for a partition
	FirstOffset kafka.Offset = -2
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

// StringMarshaller returns a MarshallFunc that is only capable of marshalling
// a string or any type that implements the fmt.Stringer interface. If any other
// type is provided an error will be returned.
//
// StringMarshaller is useful for marshalling keys which are typically a string.
func StringMarshaller() MarshallFunc {
	return func(v any) ([]byte, error) {
		switch v.(type) {
		case string:
			s := v.(string)
			return []byte(s), nil
		case fmt.Stringer:
			s := v.(fmt.Stringer)
			return []byte(s.String()), nil
		default:
			return nil, fmt.Errorf("StringMarshaller only supports strings and types that implement fmt.Stringer")
		}
	}
}
