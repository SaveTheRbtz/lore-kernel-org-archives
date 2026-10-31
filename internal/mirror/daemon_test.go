package mirror

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"lore-mirror/internal/lore"
	"lore-mirror/internal/observe"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func startDaemon(t *testing.T, d *daemon) context.CancelFunc {
	t.Helper()
	if d.queue == nil {
		d.queue = func(int64) {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("shutdown: %v, want context.Canceled", err)
		}
	})
	return cancel
}

func TestNextRunSurvivesDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		err                    error
		before, after, recheck time.Duration
	}{
		{"recheck", nil, 10*time.Minute - time.Second, time.Minute + time.Second, 10 * time.Minute},
		{"transient", errors.New("temporary failure"), 59 * time.Second, 8 * time.Second, 10 * time.Second},
		{"oversized", ErrPushTooLarge, 23 * time.Hour, time.Hour + time.Second, time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				attempts := make(chan time.Time, 16)
				d := daemon{
					fetch: func(context.Context) (lore.Manifest, error) {
						entry := lore.ManifestEntry{}
						if tc.err != nil {
							entry.Modified = time.Now().UnixNano() // New mail must preserve retry backoff.
						}
						return lore.Manifest{"/a/git/0.git": entry}, nil
					},
					sync: func(ctx context.Context, _ string, _ lore.ManifestEntry) error {
						select {
						case attempts <- time.Now():
							return tc.err
						case <-ctx.Done():
							return ctx.Err()
						}
					},
					log: testLogger(), recheck: tc.recheck, timeout: time.Hour,
				}
				startDaemon(t, &d)
				<-attempts
				time.Sleep(tc.before)
				synctest.Wait()
				if len(attempts) != 0 {
					t.Fatalf("extra attempts before next run=%d, want 0", len(attempts))
				}
				time.Sleep(tc.after)
				synctest.Wait()
				if len(attempts) != 1 {
					t.Fatalf("extra attempts after next run=%d, want 1", len(attempts))
				}
			})
		})
	}
}

func TestRateLimitBackoffAcrossRepositories(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type attempt struct {
			path string
			at   time.Time
		}
		attempted := make(chan attempt)
		first := true
		d := daemon{
			fetch: func(ctx context.Context) (lore.Manifest, error) {
				if !first {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				first = false
				return lore.Manifest{"/a": {}, "/b": {}, "/c": {}, "/d": {}, "/e": {}, "/f": {}}, nil
			},
			sync: func(ctx context.Context, path string, _ lore.ManifestEntry) error {
				select {
				case attempted <- attempt{path, time.Now()}:
				case <-ctx.Done():
					return ctx.Err()
				}
				switch path {
				case "/c":
					return nil
				case "/e":
					return delayedError(3 * time.Hour)
				case "/f":
					<-ctx.Done()
					return ctx.Err()
				default:
					return delayedError(time.Minute)
				}
			},
			log: testLogger(), recheck: time.Nanosecond, timeout: time.Hour,
		}
		startDaemon(t, &d)
		var previous time.Time
		for i, delay := range []time.Duration{0, time.Minute, 2 * time.Minute, 0, time.Minute, 3 * time.Hour} {
			got := <-attempted
			if want := "/" + string(rune('a'+i)); got.path != want {
				t.Fatalf("attempted %q, want next untried repository %q", got.path, want)
			}
			if i > 0 {
				elapsed := got.at.Sub(previous)
				if elapsed < delay || elapsed > delay+delay/10 {
					t.Fatalf("delay before %s = %s, want %s plus at most 10%% jitter", got.path, elapsed, delay)
				}
			}
			previous = got.at
		}
	})
}

func TestPendingUpdatesWhileSyncIsBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var pending atomic.Int64
		var attempts atomic.Int64
		started := make(chan struct{})
		manifests := make(chan lore.Manifest, 1)
		manifests <- lore.Manifest{"/a": {}, "/b": {}}
		d := daemon{
			fetch: func(ctx context.Context) (lore.Manifest, error) {
				select {
				case manifest := <-manifests:
					return manifest, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
			sync: func(ctx context.Context, path string, _ lore.ManifestEntry) error {
				attempts.Add(1)
				if path == "/a" {
					return nil
				}
				close(started)
				<-ctx.Done()
				return ctx.Err()
			},
			queue: pending.Store, log: testLogger(), recheck: time.Nanosecond, timeout: time.Hour,
		}
		startDaemon(t, &d)
		<-started
		synctest.Wait()
		if got := pending.Load(); got != 0 {
			t.Fatalf("active and not-yet-due repositories counted as pending: %d", got)
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if got := pending.Load(); got != 1 {
			t.Fatalf("repository becoming due during sync: pending=%d, want 1", got)
		}
		manifests <- lore.Manifest{"/b": {}, "/c": {}, "/d": {}}
		synctest.Wait()
		if got := pending.Load(); got != 2 || attempts.Load() != 2 {
			t.Fatalf("reconciled pending=%d, attempts=%d; want 2 pending behind one blocked sync", got, attempts.Load())
		}
	})
}

func TestDiscoveryContinuesDuringSyncWithoutStarvingBackfill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var polls atomic.Int64
		release := make(chan struct{})
		started := make(chan struct{})
		attempted := make(chan string, 16)
		d := daemon{
			fetch: func(context.Context) (lore.Manifest, error) {
				poll := polls.Add(1)
				if poll > 2 {
					return nil, errors.New("discovery unavailable")
				}
				return lore.Manifest{"/hot": {Modified: poll}, "/cold": {}}, nil
			},
			sync: func(ctx context.Context, path string, entry lore.ManifestEntry) error {
				select {
				case attempted <- path:
				case <-ctx.Done():
					return ctx.Err()
				}
				if path == "/hot" && entry.Modified == 1 {
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			},
			log: testLogger(), recheck: 10 * time.Minute, timeout: 24 * time.Hour,
		}
		startDaemon(t, &d)
		<-started
		drainWork(attempted) // Either archive can win its independent initial delay.
		time.Sleep(3 * d.recheck)
		synctest.Wait()
		if got := drainWork(attempted); polls.Load() < 3 || len(got) != 0 {
			t.Fatalf("discovery blocked behind sync: polls=%d work=%v", polls.Load(), got)
		}
		close(release)
		synctest.Wait()
		if got := drainWork(attempted); !slices.Equal(got, []string{"/cold", "/hot"}) {
			t.Fatalf("lost update, inventory, or backfill fairness: %v", got)
		}
	})
}

func TestRemovedArchivesAreNotRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var removed atomic.Bool
		var polls atomic.Int64
		attempted := make(chan string, 16)
		d := daemon{
			fetch: func(context.Context) (lore.Manifest, error) {
				if !removed.Load() {
					return lore.Manifest{"/removed": {}}, nil
				}
				if polls.Add(1) == 1 {
					return lore.Manifest{}, nil
				}
				return lore.Manifest{"/remaining": {}}, nil
			},
			sync: func(ctx context.Context, path string, _ lore.ManifestEntry) error {
				select {
				case attempted <- path:
				case <-ctx.Done():
					return ctx.Err()
				}
				if path == "/removed" {
					removed.Store(true)
					return errors.New("temporary failure")
				}
				return nil
			},
			log: testLogger(), recheck: 10 * time.Second, timeout: time.Hour,
		}
		startDaemon(t, &d)
		for _, want := range []string{"/removed", "/remaining"} {
			if got := <-attempted; got != want {
				t.Fatalf("attempted %q, want %q", got, want)
			}
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if got := drainWork(attempted); slices.Contains(got, "/removed") {
			t.Fatalf("removed archive retried: %v", got)
		}
	})
}

func TestRunOnceContinuesPastFailure(t *testing.T) {
	var attempted []string
	d := daemon{
		fetch: func(context.Context) (lore.Manifest, error) { return lore.Manifest{"/a": {Modified: 2}, "/b": {}}, nil },
		sync: func(_ context.Context, path string, _ lore.ManifestEntry) error {
			attempted = append(attempted, path)
			if path == "/a" {
				return errors.New("failed")
			}
			return nil
		},
		log: testLogger(), queue: func(int64) {}, timeout: time.Minute,
	}
	if err := d.runOnce(context.Background()); err == nil {
		t.Fatal("failed sync was not reported")
	}
	if !slices.Equal(attempted, []string{"/a", "/b"}) {
		t.Fatalf("remaining work skipped: %v", attempted)
	}
}

func TestHealthReportsBlockedWorkAndRecoversAfterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		health := &observe.Health{}
		release := make(chan struct{})
		started := make(chan struct{})
		d := daemon{
			fetch: func(ctx context.Context) (lore.Manifest, error) {
				select {
				case <-release:
					return lore.Manifest{"/a": {}}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
			sync: func(ctx context.Context, _ string, _ lore.ManifestEntry) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			},
			log: testLogger(), health: health, recheck: 10 * time.Minute, timeout: 24 * time.Hour,
		}
		check := func(want bool) {
			t.Helper()
			if got := health.Stuck(); got != want {
				t.Fatalf("health stuck=%v, want %v", got, want)
			}
		}
		cancel := startDaemon(t, &d)
		time.Sleep(5 * time.Minute)
		synctest.Wait()
		check(true)
		close(release)
		<-started
		synctest.Wait()
		check(false)
		time.Sleep(5 * time.Minute)
		synctest.Wait()
		check(true)
		cancel()
		synctest.Wait()
		check(false)
	})
}

type delayedError time.Duration

func (delayedError) Error() string               { return "rate limit" }
func (d delayedError) RetryAfter() time.Duration { return time.Duration(d) }

func drainWork(events <-chan string) []string {
	var paths []string
	for {
		select {
		case path := <-events:
			paths = append(paths, path)
		default:
			return paths
		}
	}
}
