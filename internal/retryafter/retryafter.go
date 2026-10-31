// Package retryafter carries server-requested cooldowns to the mirror scheduler.
package retryafter

import (
	"net/http"
	"time"
)

const maxDelay = 24 * time.Hour

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
	return &Error{error: err, after: max(0, min(delay, maxDelay))}
}

// Delay combines Retry-After with any delay already supplied by the SDK.
// A 429 without a positive delay gets a one-minute cooldown.
func Delay(resp *http.Response, delay time.Duration) time.Duration {
	if resp == nil {
		return delay
	}
	value := resp.Header.Get("Retry-After")
	if seconds, ok := delaySeconds(value); ok {
		delay = max(delay, seconds)
	} else if until, err := http.ParseTime(value); err == nil {
		delay = max(delay, time.Until(until))
	}
	if resp.StatusCode == http.StatusTooManyRequests && delay <= 0 {
		return time.Minute
	}
	return delay
}

// HTTP delay-seconds allows arbitrarily many ASCII digits.
func delaySeconds(value string) (time.Duration, bool) {
	if value == "" {
		return 0, false
	}
	var seconds time.Duration
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return 0, false
		}
		seconds = min(seconds*10+time.Duration(digit-'0'), maxDelay/time.Second)
	}
	return seconds * time.Second, true
}
