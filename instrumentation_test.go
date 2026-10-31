package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func instrumentationMeter(t *testing.T) (*telemetry, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	metrics, err := newTelemetry(provider.Meter("instrumentation-test"), nil)
	if err != nil {
		t.Fatal(err)
	}
	return metrics, reader
}

func collectInstrumentation(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var resource metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resource); err != nil {
		t.Fatal(err)
	}
	result := map[string]metricdata.Metrics{}
	for _, scope := range resource.ScopeMetrics {
		for _, metric := range scope.Metrics {
			result[metric.Name] = metric
		}
	}
	return result
}

func TestDaemonRecordsOperationsAndWorkerState(t *testing.T) {
	metrics, reader := instrumentationMeter(t)
	attempts := 0
	d := daemon{
		fetch: func(context.Context) (Manifest, error) {
			return Manifest{"/first/git/0.git": {Modified: 2}, "/second/git/0.git": {Modified: 1}}, nil
		},
		sync: func(context.Context, string, ManifestEntry) error {
			observed := collectInstrumentation(t, reader)
			for name, want := range map[string]int64{
				"lore_mirror_worker_busy": 1,
				"lore_mirror_pending":     int64(1 - attempts),
			} {
				points := observed[name].Data.(metricdata.Gauge[int64]).DataPoints
				if len(points) != 1 || points[0].Value != want || points[0].Attributes.Len() != 0 {
					t.Fatalf("%s during sync: %+v", name, points)
				}
			}
			active := observed["lore_mirror_worker_time"].Data.(metricdata.Sum[float64]).DataPoints
			if len(active) != 1 || active[0].Value <= 0 {
				t.Fatalf("in-flight worker time not observed: %+v", active)
			}
			attempts++
			if attempts == 1 {
				return errors.New("injected failure")
			}
			return nil
		},
		log: testLogger(), metrics: metrics, timeout: time.Second,
	}
	if err := d.runOnce(context.Background()); err == nil {
		t.Fatal("failed sync was not reported")
	}
	observed := collectInstrumentation(t, reader)
	counts := observed["lore_mirror_operations"].Data.(metricdata.Sum[int64]).DataPoints
	got := map[string]int64{}
	for _, point := range counts {
		operation, _ := point.Attributes.Value("operation")
		result, _ := point.Attributes.Value("result")
		if point.Attributes.Len() != 2 {
			t.Fatalf("unexpected metric labels: %v", point.Attributes)
		}
		got[operation.AsString()+"/"+result.AsString()] = point.Value
	}
	for _, series := range []string{"discovery/success", "sync/success", "sync/error"} {
		if got[series] != 1 {
			t.Fatalf("%s count: %v", series, got)
		}
	}
	if len(got) != 3 {
		t.Fatalf("unexpected operation series: %v", got)
	}
	latencies := observed["lore_mirror_operation_duration"].Data.(metricdata.Histogram[float64]).DataPoints
	if len(latencies) != 3 {
		t.Fatalf("latency series: %+v", latencies)
	}
	for _, point := range latencies {
		if point.Count != 1 || point.Sum <= 0 || point.Attributes.Len() != 2 {
			t.Fatalf("invalid latency observation: %+v", point)
		}
	}
	for _, name := range []string{"lore_mirror_pending", "lore_mirror_worker_busy"} {
		points := observed[name].Data.(metricdata.Gauge[int64]).DataPoints
		if len(points) != 1 || points[0].Value != 0 {
			t.Fatalf("%s after run: %+v", name, points)
		}
	}
}

func TestRelayCountsFailedAndSuccessfulStreamingBytes(t *testing.T) {
	metrics, reader := instrumentationMeter(t)
	s := newRelayTestServer(t)
	hash := relayTestCommit(t, s.source, "mail")
	relayTestSetRef(t, s.source, "refs/heads/master", hash)
	r := Relay{Client: s.server.Client(), AllowHTTP: true, MaxPushBytes: 180, metrics: metrics}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	failed, err := r.Sync(ctx, s.server.URL+"/source.git", s.server.URL+"/destination.git", "test-token")
	if !errors.Is(err, ErrPushTooLarge) || failed.Bytes != 180 {
		t.Fatalf("expected partial failed push, got %+v %v", failed, err)
	}
	assertBytes := func(wantSend int64) {
		t.Helper()
		observed := collectInstrumentation(t, reader)
		points := observed["lore_mirror_transfer"].Data.(metricdata.Sum[int64]).DataPoints
		got := map[string]int64{}
		for _, point := range points {
			direction, _ := point.Attributes.Value("direction")
			if point.Attributes.Len() != 1 {
				t.Fatalf("unexpected transfer labels: %v", point.Attributes)
			}
			got[direction.AsString()] = point.Value
		}
		if got["send"] != wantSend || got["receive"] <= 0 || len(got) != 2 {
			t.Fatalf("streamed bytes: %v, want %d sent", got, wantSend)
		}
	}
	assertBytes(failed.Bytes)
	r.MaxPushBytes = 0
	succeeded, err := r.Sync(ctx, s.server.URL+"/source.git", s.server.URL+"/destination.git", "test-token")
	if err != nil || !succeeded.Changed {
		t.Fatalf("retry: %+v %v", succeeded, err)
	}
	assertBytes(failed.Bytes + succeeded.Bytes)
}

func TestTelemetryDisabledAndLabelsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	disabled, shutdown, err := startTelemetry(ctx, "", nil)
	if err != nil || disabled != nil {
		t.Fatalf("disabled telemetry required cloud access: %v %v", disabled, err)
	}
	disabled.operation(ctx, "sync", time.Now(), nil)
	disabled.queue(1)
	disabled.busy(true)
	disabled.addBytes(ctx, "send", 1)
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	metrics, reader := instrumentationMeter(t)
	metrics.operation(ctx, "/private/repo.git", time.Now(), nil)
	metrics.operation(ctx, "sync", time.Now(), context.Canceled)
	metrics.operation(ctx, "sync", time.Now(), context.DeadlineExceeded)
	metrics.addBytes(ctx, "https://private.example/", 100)
	metrics.addBytes(ctx, "send", -100)
	observed := collectInstrumentation(t, reader)
	points := observed["lore_mirror_operations"].Data.(metricdata.Sum[int64]).DataPoints
	results := map[string]int64{}
	for _, point := range points {
		operation, _ := point.Attributes.Value("operation")
		result, _ := point.Attributes.Value("result")
		if operation.AsString() != "sync" || point.Attributes.Len() != 2 {
			t.Fatalf("unbounded operation labels: %v", point.Attributes)
		}
		results[result.AsString()] = point.Value
	}
	if len(results) != 2 || results["canceled"] != 1 || results["error"] != 1 {
		t.Fatalf("outcomes: %v", results)
	}
	for _, point := range observed["lore_mirror_transfer"].Data.(metricdata.Sum[int64]).DataPoints {
		if point.Value != 0 || point.Attributes.Len() != 1 {
			t.Fatalf("invalid direction or negative bytes recorded: %+v", point)
		}
	}
}

func TestDaemonHealthTracksProgressWithoutTelemetry(t *testing.T) {
	health := newHealthState()
	now := time.Now()
	health.now = func() time.Time { return now }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := daemon{health: health, timeout: time.Hour}
	d.fetch = func(context.Context) (Manifest, error) {
		now = now.Add(6 * time.Minute)
		if !health.stuck() {
			t.Fatal("discovery did not mark active work")
		}
		return nil, context.Canceled
	}
	if _, err := d.discover(ctx); !errors.Is(err, context.Canceled) || health.stuck() {
		t.Fatalf("discovery cancellation retained active work: %v", err)
	}
	d.sync = func(ctx context.Context, _ string, _ ManifestEntry) error {
		now = now.Add(6 * time.Minute)
		if !health.stuck() {
			t.Fatal("sync did not mark active work")
		}
		reader := relayCountingReader{Reader: strings.NewReader("progress"), health: health}
		if _, err := io.ReadAll(&reader); err != nil || health.stuck() {
			t.Fatalf("received bytes did not refresh progress: %v", err)
		}
		now = now.Add(6 * time.Minute)
		if _, err := reader.Read(make([]byte, 1)); err != io.EOF || !health.stuck() {
			t.Fatal("EOF without bytes incorrectly refreshed progress")
		}
		writer := relayLimitedWriter{w: io.Discard, limit: 100, health: health}
		if _, err := writer.Write([]byte("progress")); err != nil || health.stuck() {
			t.Fatalf("sent bytes did not refresh progress: %v", err)
		}
		cancel()
		return ctx.Err()
	}
	if err := d.syncWork(ctx, "/test/git/0.git", ManifestEntry{}); !errors.Is(err, context.Canceled) || health.stuck() {
		t.Fatalf("sync cancellation retained active work: %v", err)
	}
	now = now.Add(time.Hour)
	if health.stuck() {
		t.Fatal("idle worker was reported stuck")
	}
}
