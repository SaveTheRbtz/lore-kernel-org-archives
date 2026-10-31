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
	fetch                 func(context.Context) (lore.Manifest, error)
	sync                  func(context.Context, string, lore.ManifestEntry) error
	log                   *slog.Logger
	metrics               *observe.Telemetry
	health                *observe.Health
	poll, rescan, timeout time.Duration
}

type work struct {
	entry     lore.ManifestEntry
	done      bool
	failures  int
	next      time.Time
	attempted time.Time
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
	nextRescan := time.Now().Add(d.rescan)
	var cooldown time.Time
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case manifest := <-updates:
			reconcile(queue, manifest)
		default:
		}
		now := time.Now()
		if !now.Before(nextRescan) {
			for _, item := range queue {
				item.done = false
			}
			nextRescan = now.Add(d.rescan)
			d.log.Info("rescan", "repositories", len(queue))
		}
		path, wake := nextWork(queue, now, nextRescan)
		if cooldown.After(now) {
			path = ""
			wake = minTime(cooldown, nextRescan)
		}
		pending := int64(0)
		for _, item := range queue {
			if !item.done {
				pending++
			}
		}
		if path != "" {
			pending-- // The active transfer is represented by worker activity.
		}
		d.metrics.Queue(pending)
		if path != "" {
			item := queue[path]
			item.attempted = now
			started := time.Now()
			err := d.syncWork(ctx, path, item.entry)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == nil {
				item.done = true
				item.failures = 0
				item.next = time.Time{}
				d.log.Info("synced", "source", path, "duration", time.Since(started).String())
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
		timer := time.NewTimer(time.Until(wake))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case manifest := <-updates:
			timer.Stop()
			reconcile(queue, manifest)
		case <-timer.C:
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
		} else {
			d.log.Info("synced", "source", path)
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

func reconcile(queue map[string]*work, manifest lore.Manifest) {
	for path := range queue {
		if _, exists := manifest[path]; !exists {
			delete(queue, path)
		}
	}
	for path, entry := range manifest {
		old, exists := queue[path]
		if !exists {
			queue[path] = &work{entry: entry}
		} else {
			if old.entry.Fingerprint != entry.Fingerprint || old.entry.Modified != entry.Modified {
				old.done = false
			}
			// New mail must not erase Retry-After or continually retry a large
			// rejected epoch. Keep its deadline and position in the work queue.
			old.entry = entry
		}
	}
}

func nextWork(queue map[string]*work, now, wake time.Time) (string, time.Time) {
	chosen := ""
	for path, item := range queue {
		if item.done {
			continue
		}
		if item.next.After(now) {
			if item.next.Before(wake) {
				wake = item.next
			}
			continue
		}
		if chosen == "" || item.attempted.Before(queue[chosen].attempted) ||
			(item.attempted.Equal(queue[chosen].attempted) && (item.entry.Modified > queue[chosen].entry.Modified ||
				(item.entry.Modified == queue[chosen].entry.Modified && path < chosen))) {
			chosen = path
		}
	}
	return chosen, wake
}

func retryDelay(err error, failures int) time.Duration {
	if errors.Is(err, ErrPushTooLarge) {
		return 24 * time.Hour
	}
	var permanent interface{ Permanent() bool }
	if errors.As(err, &permanent) && permanent.Permanent() {
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

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func jitter(delay time.Duration) time.Duration {
	return delay + time.Duration(rand.Float64()*float64(delay)/10)
}
