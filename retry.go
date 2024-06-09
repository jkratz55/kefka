package kefka

import (
	"math"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pkg/errors"
)

// Retry is a middleware for a Handler that retries processing a message when
// a Handler returns an error.
func Retry(opts RetryOpts) func(h Handler) Handler {
	if opts.Backoff == nil {
		opts.Backoff = defaultBackoff
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = math.MaxInt64 // This basically means infinite attempts although not technically infinite :)
	}
	return func(h Handler) Handler {
		return &RetryHandler{
			next: h,
			opts: opts,
		}
	}
}

// OnErrorFunc is a function type that is invoked when an error occurs within
// a Handler while processing a message from Kafka. The function returns a bool
// indicating if the message should be retried again.
type OnErrorFunc func(error) bool

// RetryOpts contains options for configuring the behavior of the RetryHandler.
type RetryOpts struct {

	// Maximum attempts to process a message by invoking the Handler.
	//
	// The default value is math.MaxInt64, which for all practical purposes is
	// infinite attempts.
	MaxAttempts int

	// Backoff controls the delay between retries to processing a message. This
	// can help to prevent overwhelming a system that may already be struggling
	// or experiencing issues.
	//
	// The default value is an exponential backoff with an initial delay of
	// 200ms and a maximum delay of 3 seconds.
	Backoff Backoff

	// OnError is a callback function that is invoked when a Handler returns an
	// and error. The function should return a bool indicating if the message
	// should be retried again. If the function returns false no further attempts
	// will be made to process the message and the last error is returned.
	OnError OnErrorFunc
}

// RetryHandler is a middleware wrapper for a Handler that retries processing a
// Kafka messages when a Handler returns an error.
type RetryHandler struct {
	next Handler
	opts RetryOpts
}

func (r *RetryHandler) Handle(msg *kafka.Message) error {
	var lastErr error
	for i := 0; i < r.opts.MaxAttempts; i++ {
		err := r.next.Handle(msg)
		if err == nil {
			return nil
		}
		lastErr = err

		// If the Handler returned an error and the OnError callback is set
		// invoke the callback. If the callback returns false there will be
		// no further attempts to process the message and the last error is
		// returned.
		if err != nil && r.opts.OnError != nil {
			if ok := r.opts.OnError(err); !ok {
				return err
			}
		}

		// If there are still attempts remaining delay the retry based on the
		// Backoff.
		if i < r.opts.MaxAttempts-1 {
			time.Sleep(r.opts.Backoff.Delay())
		}
	}
	return errors.Wrap(lastErr, "failed to process message: max attempts reached")
}