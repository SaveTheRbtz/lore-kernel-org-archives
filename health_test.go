package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func healthClock() (*healthState, func(time.Duration)) {
	var clock atomic.Int64
	clock.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	health := newHealthState()
	health.now = func() time.Time { return time.Unix(0, clock.Load()) }
	return health, func(elapsed time.Duration) { clock.Add(int64(elapsed)) }
}

func TestHealthTracksIndependentActiveOperations(t *testing.T) {
	health, advance := healthClock()
	health.progress("sync") // Progress without an active operation stays idle.
	health.begin("unbounded repository name")()
	advance(10 * time.Minute)
	if health.stuck() {
		t.Fatal("idle worker or retry backoff became unhealthy")
	}
	finishSync := health.begin("sync")
	advance(4 * time.Minute)
	health.progress("sync")
	advance(4 * time.Minute)
	if health.stuck() {
		t.Fatal("streaming progress did not refresh the deadline")
	}
	advance(time.Minute)
	finishDiscovery := health.begin("discovery")
	if !health.stuck() {
		t.Fatal("fresh discovery hid a stuck sync")
	}
	health.progress("sync")
	if health.stuck() {
		t.Fatal("resumed progress did not restore health")
	}
	advance(5 * time.Minute)
	finishSync()
	if !health.stuck() {
		t.Fatal("completing sync hid a stuck discovery")
	}
	finishDiscovery()
	if health.stuck() {
		t.Fatal("finished operations stayed stuck")
	}
}

func TestHealthEndpointAndMetricAgree(t *testing.T) {
	health, advance := healthClock()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	if _, err := newTelemetry(provider.Meter("health-test"), health); err != nil {
		t.Fatal(err)
	}
	check := func(wantStatus int, wantBody string, wantMetric int64) {
		t.Helper()
		response := httptest.NewRecorder()
		health.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		if response.Code != wantStatus || response.Body.String() != wantBody {
			t.Fatalf("health response: %d %q", response.Code, response.Body.String())
		}
		observed := collectInstrumentation(t, reader)
		points := observed["lore_mirror_stuck"].Data.(metricdata.Gauge[int64]).DataPoints
		if len(points) != 1 || points[0].Value != wantMetric || points[0].Attributes.Len() != 0 {
			t.Fatalf("stuck metric: %+v", points)
		}
	}
	check(http.StatusOK, "ok\n", 0)
	finish := health.begin("sync")
	advance(stuckAfter)
	check(http.StatusServiceUnavailable, "stuck\n", 1)
	finish()
	check(http.StatusOK, "ok\n", 0)
	response := httptest.NewRecorder()
	health.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/private", nil))
	if response.Code != http.StatusNotFound {
		t.Fatal("health server served another endpoint")
	}
}
