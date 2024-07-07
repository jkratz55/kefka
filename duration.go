package kefka

import (
	"encoding/json"
	"fmt"
	"time"
)

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("duration unmarshal json: %w", err)
	}
	duration, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = duration
	return nil
}

func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	err := unmarshal(&str)
	if err != nil {
		return fmt.Errorf("duration unmarshal yaml: %w", err)
	}
	duration, err := time.ParseDuration(str)
	if err != nil {
		return err
	}
	d.Duration = duration
	return nil
}
