package kefka

func IsRetryable(err error) bool {
	if e, ok := err.(retryable); ok {
		return e.IsRetryable()
	}
	return false
}

func RetryableError(err error) error {
	if err == nil {
		return nil
	}
	return retryableError{err: err, retryable: true}
}

type retryable interface {
	IsRetryable() bool
}

type retryableError struct {
	err       error
	retryable bool
}

func (r retryableError) IsRetryable() bool {
	return r.retryable
}

func (r retryableError) Error() string {
	return r.err.Error()
}

func (r retryableError) Unwrap() error {
	return r.err
}
