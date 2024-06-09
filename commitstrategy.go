package kefka

import (
	"encoding/json"
	"fmt"
	"strings"
)

type CommitStrategy string

const (
	Immediate CommitStrategy = "immediate"
	Interval  CommitStrategy = "interval"
)

func ParseCommitStrategy(s string) (CommitStrategy, error) {
	switch strings.ToLower(s) {
	case "immediate":
		return Immediate, nil
	case "interval":
		return Interval, nil
	default:
		return "", fmt.Errorf("invalid commit strategy: %s", s)
	}
}

func (c *CommitStrategy) UnmarshalText(text []byte) error {
	strategy, err := ParseCommitStrategy(string(text))
	if err != nil {
		return err
	}

	*c = strategy
	return nil
}

func (c *CommitStrategy) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	strategy, err := ParseCommitStrategy(value)
	if err != nil {
		return err
	}

	*c = strategy
	return nil
}

func (c *CommitStrategy) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var value string
	if err := unmarshal(&value); err != nil {
		return err
	}

	strategy, err := ParseCommitStrategy(value)
	if err != nil {
		return err
	}

	*c = strategy
	return nil
}

func (c *CommitStrategy) String() string {
	return string(*c)
}
