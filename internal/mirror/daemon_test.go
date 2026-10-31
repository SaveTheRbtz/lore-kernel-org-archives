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

type manifestResult struct {
	manifest lore.Manifest
	err      error
}

func readManifests(updates <-chan manifestResult) func(context.Context) (lore.Manifest, error) {
	return func(ctx context.Context) (lore.Manifest, error) {
		select {
		case update := <-updates:
			return update.manifest, update.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func TestNextRunSurvivesDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		err                       error
		minimum, maximum, recheck time.Duration
	}{
		{"recheck", nil, 10 * time.Minute, 11 * time.Minute, 10 * time.Minute},
		{"transient", errors.New("temporary failure"), time.Minute, 66 * time.Second, 10 * time.Second},
		{"oversized", ErrPushTooLarge, 24 * time.Hour, 24 * time.Hour, time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				attempts := make(chan time.Time, 1)
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
				first := <-attempts
				time.Sleep(tc.maximum)
				synctest.Wait()
				select {
				case second := <-attempts:
					if delay := second.Sub(first); delay < tc.minimum || delay > tc.maximum {
						t.Fatalf("next attempt after %s, want %s to %s", delay, tc.minimum, tc.maximum)
					}
				default:
					t.Fatalf("no next attempt within %s", tc.maximum)
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
		results := make(chan error)
		updates := make(chan manifestResult, 1)
		updates <- manifestResult{manifest: lore.Manifest{"/a": {}, "/b": {}, "/c": {}, "/d": {}, "/e": {}, "/f": {}}}
		d := daemon{
			fetch: readManifests(updates),
			sync: func(ctx context.Context, path string, _ lore.ManifestEntry) error {
				select {
				case attempted <- attempt{path, time.Now()}:
				case <-ctx.Done():
					return ctx.Err()
				}
				select {
				case err := <-results:
					return err
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			log: testLogger(), recheck: time.Nanosecond, timeout: time.Hour,
		}
		startDaemon(t, &d)
		previous := time.Now()
		expect := func(path string, minimum, maximum time.Duration) {
			t.Helper()
			got := <-attempted
			if got.path != path {
				t.Fatalf("attempted %q, want next untried repository %q", got.path, path)
			}
			if elapsed := got.at.Sub(previous); elapsed < minimum || elapsed > maximum {
				t.Fatalf("delay before %s = %s, want %s to %s", path, elapsed, minimum, maximum)
			}
			previous = got.at
		}
		expect("/a", 0, 0)
		results <- delayedError(time.Minute)
		expect("/b", time.Minute, 66*time.Second)
		results <- delayedError(time.Minute)
		expect("/c", 2*time.Minute, 132*time.Second)
		results <- nil
		expect("/d", 0, 0)
		results <- delayedError(time.Minute)
		expect("/e", time.Minute, 66*time.Second)
		results <- delayedError(3 * time.Hour)
		expect("/f", 3*time.Hour, 3*time.Hour)
	})
}

func TestPendingUpdatesWhileSyncIsBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var pending atomic.Int64
		started := make(chan struct{})
		updates := make(chan manifestResult, 1)
		updates <- manifestResult{manifest: lore.Manifest{"/a": {}, "/b": {}}}
		d := daemon{
			fetch: readManifests(updates),
			sync: func(ctx context.Context, path string, _ lore.ManifestEntry) error {
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
		updates <- manifestResult{manifest: lore.Manifest{"/b": {}, "/c": {}, "/d": {}}}
		synctest.Wait()
		if got := pending.Load(); got != 2 {
			t.Fatalf("reconciled pending=%d, want 2 behind the blocked sync", got)
		}
	})
}

func TestDiscoveryContinuesDuringSyncWithoutStarvingBackfill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		started := make(chan struct{})
		attempted := make(chan string, 16)
		updates := make(chan manifestResult, 1)
		updates <- manifestResult{manifest: lore.Manifest{"/hot": {Modified: 1}, "/cold": {}}}
		d := daemon{
			fetch: readManifests(updates),
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
			log: testLogger(), recheck: time.Nanosecond, timeout: time.Hour,
		}
		startDaemon(t, &d)
		<-started
		if got := <-attempted; got != "/hot" {
			t.Fatalf("initial work=%q, want newest archive /hot", got)
		}
		updates <- manifestResult{manifest: lore.Manifest{"/hot": {Modified: 2}, "/cold": {}}}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		updates <- manifestResult{err: errors.New("discovery unavailable")}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if got := drainWork(attempted); len(got) != 0 {
			t.Fatalf("work overlapped blocked sync: %v", got)
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
		attempted := make(chan string, 16)
		updates := make(chan manifestResult, 1)
		updates <- manifestResult{manifest: lore.Manifest{"/removed": {}}}
		d := daemon{
			fetch: readManifests(updates),
			sync: func(ctx context.Context, path string, _ lore.ManifestEntry) error {
				select {
				case attempted <- path:
				case <-ctx.Done():
					return ctx.Err()
				}
				if path == "/removed" {
					return errors.New("temporary failure")
				}
				return nil
			},
			log: testLogger(), recheck: 10 * time.Second, timeout: time.Hour,
		}
		startDaemon(t, &d)
		if got := <-attempted; got != "/removed" {
			t.Fatalf("initial work=%q, want /removed", got)
		}
		updates <- manifestResult{manifest: lore.Manifest{}}
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if got := drainWork(attempted); len(got) != 0 {
			t.Fatalf("removed archive retried: %v", got)
		}
		updates <- manifestResult{manifest: lore.Manifest{"/remaining": {}}}
		if got := <-attempted; got != "/remaining" {
			t.Fatalf("work after inventory repopulated=%q, want /remaining", got)
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
		updates := make(chan manifestResult)
		started := make(chan struct{})
		d := daemon{
			fetch: readManifests(updates),
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
		updates <- manifestResult{manifest: lore.Manifest{"/a": {}}}
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
