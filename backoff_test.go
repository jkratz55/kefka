package kefka

import (
	"fmt"
	"testing"
	"time"
)

func TestExponentialBackoff_Next(t *testing.T) {
	backoff := ExponentialBackoff(time.Second*1, time.Minute*10)
	for {
		fmt.Println(backoff.Next().String())
		time.Sleep(time.Second)
	}
}

func TestRandomBackoff_Next(t *testing.T) {
	backoff := RandomBackoff(time.Second*1, time.Second*10)
	for {
		fmt.Println(backoff.Next().String())
		time.Sleep(time.Second)
	}
}
