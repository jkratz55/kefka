package kefka

import (
	"encoding/json"
	"fmt"
	"strings"
)

type AutoOffsetReset string

const (
	Earliest AutoOffsetReset = "earliest"
	Latest   AutoOffsetReset = "latest"
)

func (a *AutoOffsetReset) UnmarshalText(text []byte) error {
	val, err := ParseAutoOffsetReset(string(text))
	if err != nil {
		return err
	}
	*a = val
	return nil
}

func (a *AutoOffsetReset) UnmarshalJSON(bytes []byte) error {
	var str string
	err := json.Unmarshal(bytes, &str)
	if err != nil {
		return fmt.Errorf("auto offset reset unmarshal json: %w", err)
	}

	val, err := ParseAutoOffsetReset(str)
	if err != nil {
		return err
	}
	*a = val
	return nil
}

func (a *AutoOffsetReset) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	err := unmarshal(&str)
	if err != nil {
		return fmt.Errorf("auto offset reset unmarshal yaml: %w", err)
	}

	val, err := ParseAutoOffsetReset(str)
	if err != nil {
		return err
	}
	*a = val
	return nil
}

func (a *AutoOffsetReset) String() string {
	return string(*a)
}

func ParseAutoOffsetReset(s string) (AutoOffsetReset, error) {
	switch strings.ToLower(s) {
	case "earliest":
		return Earliest, nil
	case "latest":
		return Latest, nil
	default:
		return "", fmt.Errorf("kafka: invalid auto offset reset: %s", s)
	}
}
