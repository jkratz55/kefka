package kefka

import (
	"encoding/json"
	"fmt"
	"strings"
)

type SaslMechanism string

const (
	Plain       SaslMechanism = "PLAIN"
	GSSAPI      SaslMechanism = "GSSAPI"
	ScramSha256 SaslMechanism = "SCRAM-SHA-256"
	ScramSha512 SaslMechanism = "SCRAM-SHA-512"
)

func (sm *SaslMechanism) UnmarshalJSON(b []byte) error {
	var str string
	err := json.Unmarshal(b, &str)
	if err != nil {
		return fmt.Errorf("sasl mechanism unmarshal json: %w", err)
	}

	s, err := ParseSaslMechanism(str)
	if err != nil {
		return err
	}
	*sm = s
	return nil
}

func (sm *SaslMechanism) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	err := unmarshal(&str)
	if err != nil {
		return fmt.Errorf("sasl mechanism unmarshal yaml: %w", err)
	}

	s, err := ParseSaslMechanism(str)
	if err != nil {
		return err
	}
	*sm = s
	return nil
}

func (sm *SaslMechanism) UnmarshalText(b []byte) error {
	s, err := ParseSaslMechanism(string(b))
	if err != nil {
		return err
	}
	*sm = s
	return nil
}

func (sm *SaslMechanism) String() string {
	return string(*sm)
}

func ParseSaslMechanism(s string) (SaslMechanism, error) {
	switch strings.ToUpper(s) {
	case "PLAIN":
		return Plain, nil
	case "GSSAPI":
		return GSSAPI, nil
	case "SCRAM-SHA-256":
		return ScramSha256, nil
	case "SCRAM-SHA-512":
		return ScramSha512, nil
	default:
		return "", fmt.Errorf("kafka:invalid sasl mechanism: %s", s)
	}
}
