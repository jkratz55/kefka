package kefka

import (
	"math/rand"
	"time"
)

// Backoff is a type that provides a delay (backoff) duration between operations.
type Backoff interface {
	Delay() time.Duration
}

var (
	random = rand.New(rand.NewSource(time.Now().UnixNano()))

	defaultBackoff = ExponentialBackoff(200*time.Millisecond, 3*time.Second)
)

// ConstantBackoff returns a Backoff that delays a constant duration between operations.
func ConstantBackoff(dur time.Duration) Backoff {
	return constantBackoff{dur}
}

type constantBackoff struct {
	dur time.Duration
}

func (c constantBackoff) Delay() time.Duration {
	return c.dur
}

// ExponentialBackoff returns a Backoff that delays an exponentially increasing
// duration between operations up to the maxDelay.
func ExponentialBackoff(initialDelay, maxDelay time.Duration) Backoff {
	if maxDelay < initialDelay {
		panic("maxDelay must be greater than initialDelay")
	}
	return &exponentialBackoff{initialDelay, maxDelay}
}

type exponentialBackoff struct {
	dur time.Duration
	max time.Duration
}

func (e *exponentialBackoff) Delay() time.Duration {
	next := e.dur

	maxJitter := e.dur / 4
	randomFactor := time.Duration(random.Intn(int(maxJitter*2+1))) - maxJitter
	e.dur = e.dur*2 + randomFactor
	if e.dur > e.max {
		e.dur = e.max
	}
	return next
}
