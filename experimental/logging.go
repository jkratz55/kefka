package experimental

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"

	"github.com/pkg/errors"
)

func NopLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func defaultLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, nil)).
		With("name", "kefka")
}

func errAttr(err error) slog.Attr {
	return slog.Any("err", err)
}

func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	switch a.Value.Kind() {
	case slog.KindAny:
		switch v := a.Value.Any().(type) {
		case error:
			a.Value = fmtErr(v)
		}
	}

	return a
}

// fmtErr returns a slog.GroupValue with keys "msg" and "trace". If the error
// does not implement interface { StackTrace() errors.StackTrace }, the "trace"
// key is omitted.
func fmtErr(err error) slog.Value {
	var groupValues []slog.Attr

	groupValues = append(groupValues, slog.String("msg", err.Error()))

	type StackTracer interface {
		StackTrace() errors.StackTrace
	}

	// Find the trace to the location of the first errors.New,
	// errors.Wrap, or errors.WithStack call.
	var st StackTracer
	for err := err; err != nil; err = errors.Unwrap(err) {
		if x, ok := err.(StackTracer); ok {
			st = x
		}
	}

	if st != nil {
		groupValues = append(groupValues,
			slog.Any("trace", traceLines(st.StackTrace())),
		)
	}

	return slog.GroupValue(groupValues...)
}

func traceLines(frames errors.StackTrace) []string {
	traceLines := make([]string, len(frames))

	// Iterate in reverse to skip uninteresting, consecutive runtime frames at
	// the bottom of the trace.
	var skipped int
	skipping := true
	for i := len(frames) - 1; i >= 0; i-- {
		// Adapted from errors.Frame.MarshalText(), but avoiding repeated
		// calls to FuncForPC and FileLine.
		pc := uintptr(frames[i]) - 1
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			traceLines[i] = "unknown"
			skipping = false
			continue
		}

		name := fn.Name()

		if skipping && strings.HasPrefix(name, "runtime.") {
			skipped++
			continue
		} else {
			skipping = false
		}

		filename, lineNr := fn.FileLine(pc)

		traceLines[i] = fmt.Sprintf("%s %s:%d", name, filename, lineNr)
	}

	return traceLines[:len(traceLines)-skipped]
}

func mapLibrdKafkaLevel(lvl int) slog.Level {
	switch lvl {
	case 0, 1, 2, 3:
		return slog.LevelError
	case 4:
		return slog.LevelWarn
	case 5, 6:
		return slog.LevelInfo
	default:
		return slog.LevelDebug
	}
}
