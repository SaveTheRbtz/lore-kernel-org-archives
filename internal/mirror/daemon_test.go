package mirror

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"lore-mirror/internal/lore"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestReconcileKeepsFailuresAndDropsOnlyLocalWork(t *testing.T) {
	entry := lore.ManifestEntry{Fingerprint: "a", Modified: 1}
	queue := map[string]*work{}
	reconcile(queue, lore.Manifest{"/x/git/0.git": entry})
	retry := time.Now().Add(time.Hour)
	queue["/x/git/0.git"].failures = 2
	queue["/x/git/0.git"].next = retry
	reconcile(queue, lore.Manifest{"/x/git/0.git": entry})
	item := queue["/x/git/0.git"]
	if item.done || item.failures != 2 || !item.next.Equal(retry) {
		t.Fatal("unchanged manifest lost failed work or its backoff")
	}
	entry.Fingerprint = "b"
	reconcile(queue, lore.Manifest{"/x/git/0.git": entry})
	if queue["/x/git/0.git"].done || queue["/x/git/0.git"].failures != 2 || !queue["/x/git/0.git"].next.Equal(retry) {
		t.Fatal("changed source erased the retry deadline")
	}
	reconcile(queue, lore.Manifest{"/y/git/0.git": entry})
	if _, exists := queue["/x/git/0.git"]; exists {
		t.Fatal("removed source still queued")
	}
}

func TestHotInboxDoesNotStarveBackfill(t *testing.T) {
	now := time.Now()
	queue := map[string]*work{
		"hot":  {entry: lore.ManifestEntry{Modified: 100}, attempted: now.Add(-time.Minute)},
		"cold": {entry: lore.ManifestEntry{Modified: 1}},
	}
	path, _ := nextWork(queue, now, now.Add(time.Hour))
	if path != "cold" {
		t.Fatalf("backfill starved by %q", path)
	}
}

func TestRateLimitStopsOtherRepositoriesAndSurvivesRescan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()
	var attempts atomic.Int64
	d := daemon{
		fetch: func(context.Context) (lore.Manifest, error) {
			return lore.Manifest{"/a/git/0.git": {Modified: time.Now().UnixNano()}, "/b/git/0.git": {}}, nil
		},
		sync: func(context.Context, string, lore.ManifestEntry) error { attempts.Add(1); return delayedError{} },
		log:  testLogger(), poll: 5 * time.Millisecond, rescan: 10 * time.Millisecond, timeout: time.Second,
	}
	_ = d.run(ctx)
	if attempts.Load() != 1 {
		t.Fatalf("rate limit ignored across discovery/rescan: %d attempts", attempts.Load())
	}
}

func TestRunOnceContinuesPastFailure(t *testing.T) {
	var attempted []string
	d := daemon{
		fetch: func(context.Context) (lore.Manifest, error) {
			return lore.Manifest{"/a/git/0.git": {Modified: 2}, "/b/git/0.git": {Modified: 1}}, nil
		},
		sync: func(_ context.Context, path string, _ lore.ManifestEntry) error {
			attempted = append(attempted, path)
			if path == "/a/git/0.git" {
				return errors.New("failed")
			}
			return nil
		},
		log: testLogger(), timeout: time.Second,
	}
	if err := d.runOnce(context.Background()); err == nil {
		t.Fatal("failure was not reported")
	}
	if len(attempted) != 2 || attempted[1] != "/b/git/0.git" {
		t.Fatalf("attempts: %v", attempted)
	}
}

func TestDiscoveryContinuesDuringSyncAndDoesNotLoseChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var polls atomic.Int64
	first := make(chan struct{})
	changed := make(chan struct{})
	release := make(chan struct{})
	synced := make(chan string, 4)
	d := daemon{
		fetch: func(context.Context) (lore.Manifest, error) {
			n := polls.Add(1)
			fingerprint := "old"
			if n > 1 {
				fingerprint = "new"
			}
			if n == 2 {
				close(changed)
			}
			return lore.Manifest{"/x/git/0.git": {Fingerprint: fingerprint, Modified: n}}, nil
		},
		sync: func(ctx context.Context, _ string, entry lore.ManifestEntry) error {
			if entry.Fingerprint == "old" {
				close(first)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			select {
			case synced <- entry.Fingerprint:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		},
		log: testLogger(), poll: 20 * time.Millisecond, rescan: time.Hour, timeout: time.Second,
	}
	done := make(chan error, 1)
	go func() { done <- d.run(ctx) }()
	select {
	case <-first:
	case <-ctx.Done():
		t.Fatal("first sync did not start")
	}
	select {
	case <-changed:
	case <-ctx.Done():
		t.Fatal("discovery blocked behind sync")
	}
	close(release)
	for _, want := range []string{"old", "new"} {
		select {
		case got := <-synced:
			if got != want {
				t.Fatalf("got %s, want %s", got, want)
			}
		case <-ctx.Done():
			t.Fatal("lost source update")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("daemon did not stop")
	}
}

func TestNextWorkRespectsBackoffAndPrioritizesFreshEntries(t *testing.T) {
	now := time.Now()
	queue := map[string]*work{
		"/busy/git/0.git":  {entry: lore.ManifestEntry{Modified: 30}, next: now.Add(time.Minute)},
		"/fresh/git/0.git": {entry: lore.ManifestEntry{Modified: 20}},
		"/old/git/0.git":   {entry: lore.ManifestEntry{Modified: 10}},
	}
	path, _ := nextWork(queue, now, now.Add(time.Hour))
	if path != "/fresh/git/0.git" {
		t.Fatalf("selected %s", path)
	}
	queue[path].done = true
	queue["/old/git/0.git"].done = true
	path, wake := nextWork(queue, now, now.Add(time.Hour))
	if path != "" || !wake.Equal(now.Add(time.Minute)) {
		t.Fatalf("backoff ignored: %q %v", path, wake)
	}
}

type delayedError struct{}

func (delayedError) Error() string             { return "rate limit" }
func (delayedError) RetryAfter() time.Duration { return 3 * time.Hour }

func TestRetryDelayHonorsRateLimitAndOversize(t *testing.T) {
	if got := retryDelay(delayedError{}, 1); got != 3*time.Hour {
		t.Fatalf("Retry-After ignored: %v", got)
	}
	if got := retryDelay(ErrPushTooLarge, 1); got < 24*time.Hour {
		t.Fatalf("oversized transfer retries too soon: %v", got)
	}
}
