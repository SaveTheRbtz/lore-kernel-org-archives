package mirror

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"lore-mirror/internal/lore"
	"lore-mirror/internal/observe"
)

type daemon struct {
	fetch                  func(context.Context) (lore.Manifest, error)
	sync                   func(context.Context, string, lore.ManifestEntry) error
	log                    *slog.Logger
	metrics                *observe.Telemetry
	health                 *observe.Health
	poll, recheck, timeout time.Duration
}

type work struct {
	entry    lore.ManifestEntry
	failures int
	next     time.Time
}

// Only successful manifests reach the worker. Failed discovery never empties
// the inventory, and a busy transfer cannot block discovery.
func (d *daemon) pollManifests(ctx context.Context, updates chan lore.Manifest) {
	for {
		manifest, err := d.discover(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			d.log.Error("discovery_failed", "error", err)
		} else {
			d.log.Info("discovered", "repositories", len(manifest))
			select {
			case <-updates:
			default:
			}
			select {
			case updates <- manifest:
			case <-ctx.Done():
				return
			}
		}
		timer := time.NewTimer(jitter(d.poll))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (d *daemon) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	updates := make(chan lore.Manifest, 1)
	pollerDone := make(chan struct{})
	go func() { defer close(pollerDone); d.pollManifests(ctx, updates) }()
	defer func() { cancel(); <-pollerDone }()
	queue := map[string]*work{}
	defer d.metrics.Queue(0)
	var cooldown time.Time
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case manifest := <-updates:
			d.reconcile(queue, manifest)
		default:
		}
		now := time.Now()
		path, wake, pending := nextWork(queue, now)
		if cooldown.After(now) {
			path = ""
			wake = cooldown
		}
		if path != "" {
			pending-- // The active transfer is represented by worker activity.
		}
		d.metrics.Queue(pending)
		if path != "" {
			item := queue[path]
			err := d.syncWork(ctx, path, item.entry)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == nil {
				item.failures = 0
				item.next = time.Now().Add(jitter(d.recheck))
			} else {
				item.failures++
				delay := retryDelay(err, item.failures)
				item.next = time.Now().Add(delay)
				if pause := serverRetryAfter(err); pause > 0 {
					cooldown = time.Now().Add(pause)
				}
				d.log.Error("sync_failed", "source", path, "error", err, "retry_after", delay.String())
			}
			continue
		}
		var timer *time.Timer
		var ready <-chan time.Time
		if !wake.IsZero() {
			timer = time.NewTimer(time.Until(wake))
			ready = timer.C
		}
		select {
		case <-ctx.Done():
		case manifest := <-updates:
			d.reconcile(queue, manifest)
		case <-ready:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

func (d *daemon) runOnce(ctx context.Context) error {
	manifest, err := d.discover(ctx)
	if err != nil {
		return err
	}
	defer d.metrics.Queue(0)
	failed := 0
	for index, path := range lore.OrderedPaths(manifest) {
		if err := ctx.Err(); err != nil {
			return err
		}
		d.metrics.Queue(int64(len(manifest) - index - 1))
		err := d.syncWork(ctx, path, manifest[path])
		if err != nil {
			failed++
			d.log.Error("sync_failed", "source", path, "error", err)
			if pause := serverRetryAfter(err); pause > 0 {
				return fmt.Errorf("rate limited: stop one-shot run and retry after %s: %w", pause, err)
			}
		}
	}
	if failed != 0 {
		return fmt.Errorf("%d of %d repositories failed", failed, len(manifest))
	}
	return nil
}

func (d *daemon) discover(ctx context.Context) (lore.Manifest, error) {
	finish := d.health.Begin("discovery")
	defer finish()
	started := time.Now()
	manifest, err := d.fetch(ctx)
	d.metrics.Operation(ctx, "discovery", started, err)
	return manifest, err
}

func (d *daemon) syncWork(ctx context.Context, path string, entry lore.ManifestEntry) error {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	finish := d.health.Begin("sync")
	defer finish()
	d.metrics.Busy(true)
	defer d.metrics.Busy(false)
	started := time.Now()
	err := d.sync(ctx, path, entry)
	d.metrics.Operation(ctx, "sync", started, err)
	return err
}

func (d *daemon) reconcile(queue map[string]*work, manifest lore.Manifest) {
	now := time.Now()
	for path := range queue {
		if _, exists := manifest[path]; !exists {
			delete(queue, path)
		}
	}
	for path, entry := range manifest {
		old, exists := queue[path]
		if !exists {
			queue[path] = &work{entry: entry, next: now.Add(rand.N(d.recheck))}
		} else {
			changed := old.entry.Fingerprint != entry.Fingerprint || old.entry.Modified != entry.Modified
			if changed && old.failures == 0 && old.next.After(now) {
				old.next = now
			}
			// Keep failure backoff and the position of work already due.
			old.entry = entry
		}
	}
}

func nextWork(queue map[string]*work, now time.Time) (chosen string, wake time.Time, pending int64) {
	for path, item := range queue {
		if item.failures > 0 || !item.next.After(now) {
			pending++
		}
		if item.next.After(now) {
			if wake.IsZero() || item.next.Before(wake) {
				wake = item.next
			}
			continue
		}
		if chosen == "" || item.next.Before(queue[chosen].next) ||
			(item.next.Equal(queue[chosen].next) && (item.entry.Modified > queue[chosen].entry.Modified ||
				(item.entry.Modified == queue[chosen].entry.Modified && path < chosen))) {
			chosen = path
		}
	}
	return chosen, wake, pending
}

func retryDelay(err error, failures int) time.Duration {
	if errors.Is(err, ErrPushTooLarge) {
		return 24 * time.Hour
	}
	delay := time.Minute
	for i := 1; i < failures && delay < time.Hour; i++ {
		delay *= 2
	}
	if delay > time.Hour {
		delay = time.Hour
	}
	delay = jitter(delay)
	if pause := serverRetryAfter(err); pause > delay {
		delay = pause
	}
	return delay
}

func serverRetryAfter(err error) time.Duration {
	var throttled interface{ RetryAfter() time.Duration }
	if errors.As(err, &throttled) {
		return throttled.RetryAfter()
	}
	return 0
}

func jitter(delay time.Duration) time.Duration {
	return delay + time.Duration(rand.Float64()*float64(delay)/10)
}
