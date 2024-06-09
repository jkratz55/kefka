package experimental

import (
	"math/rand"
	"time"
)

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

type Backoff interface {
	Next() time.Duration
}

func ConstantBackoff(dur time.Duration) Backoff {
	return constantBackoff{dur}
}

type constantBackoff struct {
	dur time.Duration
}

func (c constantBackoff) Next() time.Duration {
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

func (e *exponentialBackoff) Next() time.Duration {
	next := e.dur

	maxJitter := e.dur / 4
	randomFactor := time.Duration(random.Intn(int(maxJitter*2+1))) - maxJitter
	e.dur = e.dur*2 + randomFactor
	if e.dur > e.max {
		e.dur = e.max
	}
	return next
}

func RandomBackoff(min, max time.Duration) Backoff {
	if max < min {
		panic("max must be greater than min")
	}
	return &randomBackoff{min, max}
}

type randomBackoff struct {
	min time.Duration
	max time.Duration
}

func (r randomBackoff) Next() time.Duration {
	durRange := r.max - r.min
	return r.min + time.Duration(random.Int63n(int64(durRange)))
}
