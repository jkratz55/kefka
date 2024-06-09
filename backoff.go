package kefka

import (
	"math/rand"
	"time"
)

type Backoff interface {
	Delay() time.Duration
}

var (
	random = rand.New(rand.NewSource(time.Now().UnixNano()))

	defaultBackoff = ExponentialBackoff(200*time.Millisecond, 3*time.Second)
)

func ConstantBackoff(dur time.Duration) Backoff {
	return constantBackoff{dur}
}

type constantBackoff struct {
	dur time.Duration
}

func (c constantBackoff) Delay() time.Duration {
	return c.dur
}

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
