package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"strings"
	"time"
)

type daemon struct {
	fetch                 func(context.Context) (Manifest, error)
	sync                  func(context.Context, string, ManifestEntry) error
	log                   *slog.Logger
	metrics               *telemetry
	health                *healthState
	poll, rescan, timeout time.Duration
}

type work struct {
	entry     ManifestEntry
	done      bool
	failures  int
	next      time.Time
	attempted time.Time
}

// Only successful manifests reach the worker. Failed discovery never empties
// the inventory, and a busy transfer cannot block discovery.
func (d *daemon) pollManifests(ctx context.Context, updates chan Manifest) {
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
	updates := make(chan Manifest, 1)
	pollerDone := make(chan struct{})
	go func() { defer close(pollerDone); d.pollManifests(ctx, updates) }()
	defer func() { cancel(); <-pollerDone }()
	queue := map[string]*work{}
	defer d.metrics.queue(0)
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
		d.metrics.queue(pending)
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
	defer d.metrics.queue(0)
	failed := 0
	for index, path := range orderedPaths(manifest) {
		if err := ctx.Err(); err != nil {
			return err
		}
		d.metrics.queue(int64(len(manifest) - index - 1))
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

func (d *daemon) discover(ctx context.Context) (Manifest, error) {
	finish := d.health.begin("discovery")
	defer finish()
	started := time.Now()
	manifest, err := d.fetch(ctx)
	d.metrics.operation(ctx, "discovery", started, err)
	return manifest, err
}

func (d *daemon) syncWork(ctx context.Context, path string, entry ManifestEntry) error {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	finish := d.health.begin("sync")
	defer finish()
	d.metrics.busy(true)
	defer d.metrics.busy(false)
	started := time.Now()
	err := d.sync(ctx, path, entry)
	d.metrics.operation(ctx, "sync", started, err)
	return err
}

func reconcile(queue map[string]*work, manifest Manifest) {
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

func orderedPaths(manifest Manifest) []string {
	paths := make([]string, 0, len(manifest))
	for path := range manifest {
		paths = append(paths, path)
	}
	sort.Slice(paths, func(i, j int) bool {
		if manifest[paths[i]].Modified != manifest[paths[j]].Modified {
			return manifest[paths[i]].Modified > manifest[paths[j]].Modified
		}
		return paths[i] < paths[j]
	})
	return paths
}

func selectManifest(manifest Manifest, lists string) (Manifest, error) {
	selected := map[string]bool{}
	if lists != "" {
		for _, list := range strings.Split(lists, ",") {
			list = strings.TrimSpace(list)
			if _, err := RepositoryName("/" + list); err != nil {
				return nil, fmt.Errorf("invalid inbox %q: %w", list, err)
			}
			selected[list] = false
		}
	}
	result := Manifest{}
	names := map[string]string{}
	for path, entry := range manifest {
		name, err := RepositoryName(path)
		if err != nil {
			return nil, err
		}
		if previous, exists := names[name]; exists {
			return nil, fmt.Errorf("repository name collision: %q and %q map to %q", previous, path, name)
		}
		names[name] = path
		list := strings.Split(strings.TrimPrefix(path, "/"), "/")[0]
		if len(selected) > 0 {
			if _, ok := selected[list]; !ok {
				continue
			}
			selected[list] = true
		}
		result[path] = entry
	}
	for list, found := range selected {
		if !found {
			return nil, fmt.Errorf("selected inbox %q is absent from manifest", list)
		}
	}
	return result, nil
}
