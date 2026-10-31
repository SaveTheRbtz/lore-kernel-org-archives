package mirror

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
		name                string
		err                 error
		before, after, poll time.Duration
	}{
		{"recheck", nil, 10*time.Minute - time.Second, time.Minute + time.Second, 10 * time.Second},
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
					log: testLogger(), poll: tc.poll, recheck: 10 * time.Minute, timeout: time.Hour,
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

func TestRateLimitPausesOtherRepositories(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var polls atomic.Int64
		attempted := make(chan string, 16)
		d := daemon{
			fetch: func(context.Context) (lore.Manifest, error) {
				if polls.Add(1) == 1 {
					return lore.Manifest{"/a": {}}, nil
				}
				return lore.Manifest{"/a": {}, "/b": {}}, nil
			},
			sync: func(ctx context.Context, path string, _ lore.ManifestEntry) error {
				select {
				case attempted <- path:
				case <-ctx.Done():
					return ctx.Err()
				}
				if path == "/a" {
					return delayedError{}
				}
				return nil
			},
			log: testLogger(), poll: time.Hour, recheck: 10 * time.Minute, timeout: time.Hour,
		}
		startDaemon(t, &d)
		<-attempted
		time.Sleep(2 * time.Hour)
		synctest.Wait()
		if got := drainWork(attempted); len(got) != 0 {
			t.Fatalf("rate limit allowed other work: %v", got)
		}
		time.Sleep(time.Hour + time.Second)
		synctest.Wait()
		if got := drainWork(attempted); len(got) == 0 || got[0] != "/b" {
			t.Fatalf("backfill did not resume first: %v", got)
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
			log: testLogger(), poll: time.Hour, recheck: 10 * time.Minute, timeout: 24 * time.Hour,
		}
		startDaemon(t, &d)
		<-started
		drainWork(attempted) // Either archive can win its independent initial delay.
		time.Sleep(3 * time.Hour)
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
			log: testLogger(), poll: 10 * time.Second, recheck: 10 * time.Minute, timeout: time.Hour,
		}
		startDaemon(t, &d)
		for _, want := range []string{"/removed", "/remaining"} {
			if got := <-attempted; got != want {
				t.Fatalf("attempted %q, want %q", got, want)
			}
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if got := drainWork(attempted); len(got) != 0 {
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
		log: testLogger(), timeout: time.Minute,
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
			log: testLogger(), health: health, poll: 24 * time.Hour, recheck: 10 * time.Minute, timeout: 24 * time.Hour,
		}
		check := func(want int) {
			t.Helper()
			response := httptest.NewRecorder()
			health.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
			if response.Code != want {
				t.Fatalf("health status=%d, want %d", response.Code, want)
			}
		}
		cancel := startDaemon(t, &d)
		time.Sleep(5 * time.Minute)
		synctest.Wait()
		check(http.StatusServiceUnavailable)
		close(release)
		<-started
		synctest.Wait()
		check(http.StatusOK)
		time.Sleep(5 * time.Minute)
		synctest.Wait()
		check(http.StatusServiceUnavailable)
		cancel()
		synctest.Wait()
		check(http.StatusOK)
	})
}

type delayedError struct{}

func (delayedError) Error() string             { return "rate limit" }
func (delayedError) RetryAfter() time.Duration { return 3 * time.Hour }

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
