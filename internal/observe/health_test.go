package observe

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"
)

func TestHealthEndpointTracksProgressAndIndependentOperations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var health Health
		check := func(want int) {
			t.Helper()
			response := httptest.NewRecorder()
			health.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
			if response.Code != want {
				t.Fatalf("health status=%d, want %d", response.Code, want)
			}
		}
		check(http.StatusOK)
		health.SelectionPending.Store(true)
		check(http.StatusServiceUnavailable)
		health.SelectionPending.Store(false)
		check(http.StatusOK)
		finishSync := health.Begin(Sync)
		time.Sleep(4 * time.Minute)
		health.SyncProgress()
		time.Sleep(4 * time.Minute)
		check(http.StatusOK)
		time.Sleep(time.Minute)
		finishDiscovery := health.Begin(Discovery)
		check(http.StatusServiceUnavailable) // Fresh discovery cannot hide a stuck sync.
		health.SyncProgress()
		check(http.StatusOK)
		time.Sleep(5 * time.Minute)
		finishSync()
		check(http.StatusServiceUnavailable) // Discovery is still stuck.
		finishDiscovery()
		time.Sleep(24 * time.Hour)
		check(http.StatusOK) // Idle time and retry backoff are healthy.

		response := httptest.NewRecorder()
		health.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/other", nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("unknown endpoint status=%d, want 404", response.Code)
		}
	})
}
