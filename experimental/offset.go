package experimental

import (
	"fmt"
	"strings"
)

type AutoOffsetReset string

func (a *AutoOffsetReset) UnmarshalText(text []byte) error {
	val, err := ParseAutoOffsetReset(string(text))
	if err != nil {
		return err
	}
	*a = val
	return nil
}

func (a *AutoOffsetReset) UnmarshalJSON(bytes []byte) error {
	val, err := ParseAutoOffsetReset(string(bytes))
	if err != nil {
		return err
	}
	*a = val
	return nil
}

func (a *AutoOffsetReset) String() string {
	return string(*a)
}

const (
	Earliest AutoOffsetReset = "earliest"
	Latest   AutoOffsetReset = "latest"
)

func ParseAutoOffsetReset(s string) (AutoOffsetReset, error) {
	switch strings.ToLower(s) {
	case "earliest":
		return Earliest, nil
	case "latest":
		return Latest, nil
	default:
		return "", fmt.Errorf("invalid auto offset reset: %s", s)
	}
}
