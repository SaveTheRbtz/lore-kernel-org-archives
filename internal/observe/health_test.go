package observe

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthEndpointTracksProgressAndIndependentOperations(t *testing.T) {
	now := time.Now()
	health := NewHealth(func() time.Time { return now })
	check := func(want int) {
		t.Helper()
		response := httptest.NewRecorder()
		health.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		if response.Code != want {
			t.Fatalf("health status=%d, want %d", response.Code, want)
		}
	}
	check(http.StatusOK)
	finishSync := health.Begin("sync")
	now = now.Add(4 * time.Minute)
	health.Progress("sync")
	now = now.Add(4 * time.Minute)
	check(http.StatusOK)
	now = now.Add(time.Minute)
	finishDiscovery := health.Begin("discovery")
	check(http.StatusServiceUnavailable) // Fresh discovery cannot hide a stuck sync.
	health.Progress("sync")
	check(http.StatusOK)
	now = now.Add(5 * time.Minute)
	finishSync()
	check(http.StatusServiceUnavailable) // Discovery is still stuck.
	finishDiscovery()
	now = now.Add(24 * time.Hour)
	check(http.StatusOK) // Idle time and retry backoff are healthy.

	response := httptest.NewRecorder()
	health.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/other", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown endpoint status=%d, want 404", response.Code)
	}
}
