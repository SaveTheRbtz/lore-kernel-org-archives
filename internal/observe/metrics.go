package observe

import (
	"context"
	"errors"
	"fmt"
	runtimemetrics "runtime/metrics"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/compute/metadata"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/oauth"
)

// Telemetry has no repository, inbox, URL, or error-message labels. Streaming
// bytes only touch atomics; the exporter collects them once a minute.
type Telemetry struct {
	operations metric.Int64Counter
	duration   metric.Float64Histogram
	pending    atomic.Int64
	received   atomic.Int64
	sent       atomic.Int64
	mu         sync.Mutex
	active     time.Time
	busyTime   time.Duration
}

// An empty project disables telemetry, including metadata and ADC requests.
func StartTelemetry(ctx context.Context, project string, health *Health) (*Telemetry, func(context.Context) error, error) {
	if project == "" {
		return nil, func(context.Context) error { return nil }, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	instance, err := metadata.InstanceIDWithContext(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry instance identity: %w", err)
	}
	zone, err := metadata.ZoneWithContext(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry instance zone: %w", err)
	}
	creds, err := oauth.NewApplicationDefault(ctx, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry credentials: %w", err)
	}
	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpointURL("https://telemetry.googleapis.com:443"),
		otlpmetricgrpc.WithHeaders(map[string]string{"x-goog-user-project": project}),
		otlpmetricgrpc.WithDialOption(grpc.WithPerRPCCredentials(creds)),
		otlpmetricgrpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(resource.NewSchemaless(
			attribute.String("gcp.project_id", project),
			attribute.String("cloud.platform", "gcp_compute_engine"),
			attribute.String("cloud.availability_zone", zone),
			attribute.String("service.name", "lore-mirror"),
			attribute.String("service.instance.id", instance),
		)),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter,
			sdkmetric.WithInterval(time.Minute), sdkmetric.WithTimeout(10*time.Second))),
	)
	t, err := newTelemetry(provider.Meter("lore-mirror"), health)
	if err != nil {
		_ = provider.Shutdown(ctx)
		return nil, nil, err
	}
	// Shutdown performs a final collection, including gauges before any sync.
	return t, provider.Shutdown, nil
}

func newTelemetry(meter metric.Meter, health *Health) (*Telemetry, error) {
	t := &Telemetry{}
	operations, e1 := meter.Int64Counter("lore_mirror_operations",
		metric.WithDescription("Completed operations by outcome"), metric.WithUnit("{operation}"))
	duration, e2 := meter.Float64Histogram("lore_mirror_operation_duration",
		metric.WithDescription("Operation duration"), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.1, 1, 5, 15, 60, 300, 900, 1800))
	transfer, e3 := meter.Int64ObservableCounter("lore_mirror_transfer",
		metric.WithDescription("Source pack bytes read and destination request bytes written, including failed attempts"), metric.WithUnit("By"))
	pending, e4 := meter.Int64ObservableGauge("lore_mirror_pending",
		metric.WithDescription("Due or failed repositories waiting for sync, excluding the active repository"), metric.WithUnit("{repository}"))
	busy, e5 := meter.Int64ObservableGauge("lore_mirror_worker_busy",
		metric.WithDescription("Whether the single sync worker is active"), metric.WithUnit("1"))
	workTime, e6 := meter.Float64ObservableCounter("lore_mirror_worker_time",
		metric.WithDescription("Cumulative active worker time; rate is worker utilization"), metric.WithUnit("s"))
	memory, e7 := meter.Int64ObservableGauge("lore_mirror_go_memory",
		metric.WithDescription("Go runtime memory excluding released heap pages; not process RSS"), metric.WithUnit("By"))
	goroutines, e8 := meter.Int64ObservableGauge("lore_mirror_go_goroutines",
		metric.WithDescription("Live Go goroutines"), metric.WithUnit("{goroutine}"))
	stuck, e9 := meter.Int64ObservableGauge("lore_mirror_stuck",
		metric.WithDescription("Whether an active discovery or sync has made no progress for five minutes"), metric.WithUnit("1"))
	if err := errors.Join(e1, e2, e3, e4, e5, e6, e7, e8, e9); err != nil {
		return nil, err
	}
	t.operations, t.duration = operations, duration
	_, err := meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		t.mu.Lock()
		elapsed, active := t.busyTime, int64(0)
		if !t.active.IsZero() {
			elapsed += time.Since(t.active)
			active = 1
		}
		t.mu.Unlock()
		observer.ObserveInt64(pending, t.pending.Load())
		observer.ObserveInt64(busy, active)
		unhealthy := int64(0)
		if health.Stuck() {
			unhealthy = 1
		}
		observer.ObserveInt64(stuck, unhealthy)
		observer.ObserveFloat64(workTime, elapsed.Seconds())
		observer.ObserveInt64(transfer, t.received.Load(), metric.WithAttributes(attribute.String("direction", "receive")))
		observer.ObserveInt64(transfer, t.sent.Load(), metric.WithAttributes(attribute.String("direction", "send")))
		samples := []runtimemetrics.Sample{
			{Name: "/memory/classes/total:bytes"},
			{Name: "/memory/classes/heap/released:bytes"},
			{Name: "/sched/goroutines:goroutines"},
		}
		runtimemetrics.Read(samples)
		observer.ObserveInt64(memory, int64(samples[0].Value.Uint64()-samples[1].Value.Uint64()))
		observer.ObserveInt64(goroutines, int64(samples[2].Value.Uint64()))
		return nil
	}, transfer, pending, busy, workTime, memory, goroutines, stuck)
	return t, err
}

var activityNames = [activityCount]string{Discovery: "discovery", Sync: "sync"}

func (t *Telemetry) Operation(ctx context.Context, activity Activity, started time.Time, err error) {
	if t == nil || activity >= activityCount {
		return
	}
	result := "success"
	if errors.Is(err, context.Canceled) {
		result = "canceled"
	} else if err != nil {
		result = "error"
	}
	attrs := metric.WithAttributes(attribute.String("operation", activityNames[activity]), attribute.String("result", result))
	t.operations.Add(ctx, 1, attrs)
	t.duration.Record(ctx, time.Since(started).Seconds(), attrs)
}

func (t *Telemetry) Queue(pending int64) {
	if t != nil {
		t.pending.Store(pending)
	}
}

func (t *Telemetry) Busy(active bool) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if active && t.active.IsZero() {
		t.active = time.Now()
	} else if !active && !t.active.IsZero() {
		t.busyTime += time.Since(t.active)
		t.active = time.Time{}
	}
}

func (t *Telemetry) AddBytes(direction string, n int64) {
	if t == nil || n <= 0 {
		return
	}
	switch direction {
	case "receive":
		t.received.Add(n)
	case "send":
		t.sent.Add(n)
	}
}
