package mirror

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"lore-mirror/internal/lore"
	"lore-mirror/internal/observe"
)

type daemon struct {
	fetch            func(context.Context) (lore.Manifest, error)
	sync             func(context.Context, string, lore.ManifestEntry) error
	log              *slog.Logger
	metrics          *observe.Telemetry
	queue            func(int64)
	health           *observe.Health
	recheck, timeout time.Duration
}

type work struct {
	entry    lore.ManifestEntry
	failures int
	next     time.Time
}

// Only successful manifests reach the worker. Failed discovery never empties
// the inventory, and a busy transfer cannot block discovery.
func (d *daemon) pollManifests(ctx context.Context, updates chan lore.Manifest) error {
	for {
		manifest, err := d.discover(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, lore.ErrUnknownInbox) {
				return err
			}
			d.log.Error("discovery_failed", "error", err)
		} else {
			d.log.Info("discovered", "repositories", len(manifest))
			select {
			case <-updates:
			default:
			}
			// This poller is the only sender, so the slot is available.
			updates <- manifest
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(jitter(d.recheck)):
		}
	}
}

func (d *daemon) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	updates := make(chan lore.Manifest, 1)
	var workers sync.WaitGroup
	pollerResult := make(chan error, 1)
	workers.Go(func() { pollerResult <- d.pollManifests(ctx, updates) })
	finished := make(chan error, 1)
	var active *work
	var activePath string
	var activeEntry lore.ManifestEntry
	defer func() {
		cancel()
		workers.Wait()
	}()
	queue := map[string]*work{}
	defer d.queue(0)
	var cooldown time.Time
	throttles := 0
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
		path, wake, pending := nextWork(queue, now, activePath)
		if active == nil && path != "" && !cooldown.After(now) {
			activePath, active = path, queue[path]
			entry := active.entry
			activeEntry = entry
			workers.Go(func() { finished <- d.syncWork(ctx, path, entry) })
			continue
		}
		d.queue(pending)
		if cooldown.After(now) && (wake.IsZero() || cooldown.Before(wake)) {
			wake = cooldown
		}
		var ready <-chan time.Time
		if !wake.IsZero() {
			ready = time.After(time.Until(wake))
		}
		select {
		case <-ctx.Done():
		case err := <-pollerResult:
			return err
		case manifest := <-updates:
			d.reconcile(queue, manifest)
		case err := <-finished:
			now := time.Now()
			if err == nil {
				throttles = 0
				active.failures = 0
				active.next = now.Add(jitter(d.recheck))
				if active.entry.Fingerprint != activeEntry.Fingerprint || active.entry.Modified != activeEntry.Modified {
					active.next = now
				}
			} else if ctx.Err() == nil {
				active.failures++
				delay := retryDelay(err, active.failures)
				if serverRetryAfter(err) > 0 {
					throttles++
					pause := retryDelay(err, throttles)
					cooldown = now.Add(pause)
					delay = max(delay, pause)
				}
				active.next = now.Add(delay)
				d.log.Error("sync_failed", "source", activePath, "error", err, "retry_after", delay.String())
			}
			active, activePath = nil, ""
		case <-ready:
		}
	}
}

func (d *daemon) runOnce(ctx context.Context) error {
	manifest, err := d.discover(ctx)
	if err != nil {
		return err
	}
	defer d.queue(0)
	failed := 0
	paths := slices.Collect(maps.Keys(manifest))
	rand.Shuffle(len(paths), func(i, j int) { paths[i], paths[j] = paths[j], paths[i] })
	for index, path := range paths {
		if err := ctx.Err(); err != nil {
			return err
		}
		d.queue(int64(len(manifest) - index - 1))
		err := d.syncWork(ctx, path, manifest[path])
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
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
			// Keep failure backoff and deadlines of work already due.
			old.entry = entry
		}
	}
}

func nextWork(queue map[string]*work, now time.Time, active string) (chosen string, wake time.Time, pending int64) {
	due := 0
	for path, item := range queue {
		if path == active {
			continue
		}
		if item.failures > 0 || !item.next.After(now) {
			pending++
		}
		if item.next.After(now) {
			if wake.IsZero() || item.next.Before(wake) {
				wake = item.next
			}
			continue
		}
		due++
		if rand.N(due) == 0 {
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
	return delay + min(rand.N(delay)/10, math.MaxInt64-delay)
}
