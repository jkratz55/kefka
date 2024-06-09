package kefka

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Ack string

const (
	// AckNone disables acknowledgements from the brokers. The producer will not
	// wait for any acknowledgment from the broker and the broker does not wait
	// for the message to be written before it responds.
	AckNone Ack = "none"

	// AckLeader ensures the leader broker must receive the record and successfully
	// write it to its local log before responding.
	AckLeader Ack = "leader"

	// AckAll ensures the leader and all in-sync replicas must receive the record
	// and successfully write it to their local log before responding.
	AckAll Ack = "all"
)

func (a *Ack) UnmarshalText(text []byte) error {
	ack, err := ParseAck(string(text))
	if err != nil {
		return err
	}
	*a = ack
	return nil
}

func (a *Ack) UnmarshalJSON(data []byte) error {
	var str string
	err := json.Unmarshal(data, &str)
	if err != nil {
		return fmt.Errorf("ack unmarsal json: %w", err)
	}

	ack, err := ParseAck(str)
	if err != nil {
		return err
	}
	*a = ack
	return nil
}

func (a *Ack) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	err := unmarshal(&str)
	if err != nil {
		return fmt.Errorf("ack unmarshal yaml: %w", err)
	}

	ack, err := ParseAck(str)
	if err != nil {
		return err
	}
	*a = ack
	return nil
}

func (a *Ack) value() int {
	switch *a {
	case AckNone:
		return 0
	case AckLeader:
		return 1
	case AckAll:
		return -1
	default:
		panic(fmt.Sprintf("kafka: unknown ack value: %s", *a))
	}
}

func ParseAck(s string) (Ack, error) {
	switch strings.ToLower(s) {
	case "none":
		return AckNone, nil
	case "leader":
		return AckLeader, nil
	case "all":
		return AckAll, nil
	default:
		return "", fmt.Errorf("kafka: invalid ack value: %s", s)
	}
}
