package retryafter

import (
	"net/http"
	"testing"
	"time"
)

func TestDelaySeconds(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"0000000000000000000000000000000000001", time.Second},
		{"4294967296", 24 * time.Hour},
		{"1844674407370955161600", 24 * time.Hour},
		{"1844674407370955161600x", 0},
	} {
		resp := &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{"Retry-After": {tc.value}}}
		if got := Delay(resp, 0); got != tc.want {
			t.Errorf("Retry-After %q: got %v, want %v", tc.value, got, tc.want)
		}
	}
}
