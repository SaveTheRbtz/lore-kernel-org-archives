// Package retryafter carries server-requested cooldowns to the mirror scheduler.
package retryafter

import (
	"net/http"
	"strconv"
	"time"
)

type Error struct {
	error
	after time.Duration
}

func (e *Error) Unwrap() error             { return e.error }
func (e *Error) RetryAfter() time.Duration { return e.after }

func Wrap(err error, delay time.Duration) error {
	if err == nil {
		return nil
	}
	return &Error{error: err, after: max(0, min(delay, 24*time.Hour))}
}

// Delay combines Retry-After with any delay already supplied by the SDK.
// A 429 without either gets a one-minute cooldown.
func Delay(resp *http.Response, delay time.Duration) time.Duration {
	if resp == nil {
		return delay
	}
	value := resp.Header.Get("Retry-After")
	if seconds, err := strconv.ParseUint(value, 10, 32); err == nil {
		delay = max(delay, time.Duration(seconds)*time.Second)
	} else if until, err := http.ParseTime(value); err == nil {
		delay = max(delay, time.Until(until))
	}
	if resp.StatusCode == http.StatusTooManyRequests && delay <= 0 {
		return time.Minute
	}
	return delay
}
