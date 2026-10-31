package mirror

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
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

func TestRetryDeadlinesSurviveDiscoveryAndRescans(t *testing.T) {
	for _, tc := range []struct {
		name                string
		err                 error
		before, after, poll time.Duration
	}{
		{"transient", errors.New("temporary failure"), 59 * time.Second, 8 * time.Second, 10 * time.Second},
		{"oversized", ErrPushTooLarge, 23 * time.Hour, time.Hour + time.Second, time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				attempts := make(chan time.Time, 16)
				d := daemon{
					fetch: func(context.Context) (lore.Manifest, error) {
						return lore.Manifest{"/a/git/0.git": {Modified: time.Now().UnixNano()}}, nil
					},
					sync: func(ctx context.Context, _ string, _ lore.ManifestEntry) error {
						select {
						case attempts <- time.Now():
							return tc.err
						case <-ctx.Done():
							return ctx.Err()
						}
					},
					log: testLogger(), poll: tc.poll, rescan: tc.poll, timeout: time.Hour,
				}
				startDaemon(t, &d)
				synctest.Wait()
				time.Sleep(tc.before)
				synctest.Wait()
				if len(attempts) != 1 {
					t.Fatalf("attempts before retry deadline=%d, want 1", len(attempts))
				}
				time.Sleep(tc.after)
				synctest.Wait()
				if len(attempts) != 2 {
					t.Fatalf("attempts after retry deadline=%d, want 2", len(attempts))
				}
			})
		})
	}
}

func TestRateLimitPausesOtherRepositories(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempted := make(chan string, 16)
		d := daemon{
			fetch: func(context.Context) (lore.Manifest, error) { return lore.Manifest{"/a": {Modified: 2}, "/b": {}}, nil },
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
			log: testLogger(), poll: time.Hour, rescan: time.Hour, timeout: time.Hour,
		}
		startDaemon(t, &d)
		time.Sleep(2 * time.Hour)
		synctest.Wait()
		if got := drainWork(attempted); !reflect.DeepEqual(got, []string{"/a"}) {
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
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			},
			log: testLogger(), poll: time.Hour, rescan: 24 * time.Hour, timeout: 24 * time.Hour,
		}
		startDaemon(t, &d)
		time.Sleep(3 * time.Hour)
		synctest.Wait()
		if got := drainWork(attempted); polls.Load() < 3 || !reflect.DeepEqual(got, []string{"/hot"}) {
			t.Fatalf("discovery blocked behind sync: polls=%d work=%v", polls.Load(), got)
		}
		close(release)
		synctest.Wait()
		if got := drainWork(attempted); !reflect.DeepEqual(got, []string{"/cold", "/hot"}) {
			t.Fatalf("lost update, inventory, or backfill fairness: %v", got)
		}
	})
}

func TestRemovedArchivesAreNotRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var polls atomic.Int64
		attempted := make(chan string, 16)
		d := daemon{
			fetch: func(context.Context) (lore.Manifest, error) {
				if polls.Add(1) == 1 {
					return lore.Manifest{"/removed": {}}, nil
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
					return errors.New("temporary failure")
				}
				return nil
			},
			log: testLogger(), poll: 10 * time.Second, rescan: time.Hour, timeout: time.Hour,
		}
		startDaemon(t, &d)
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if got := drainWork(attempted); !reflect.DeepEqual(got, []string{"/removed", "/remaining"}) {
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
	if !reflect.DeepEqual(attempted, []string{"/a", "/b"}) {
		t.Fatalf("remaining work skipped: %v", attempted)
	}
}

func TestHealthReportsBlockedWorkAndRecoversAfterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		health := observe.NewHealth(time.Now)
		release := make(chan struct{})
		d := daemon{
			fetch: func(ctx context.Context) (lore.Manifest, error) {
				select {
				case <-release:
					return lore.Manifest{"/a": {}}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
			sync: func(ctx context.Context, _ string, _ lore.ManifestEntry) error { <-ctx.Done(); return ctx.Err() },
			log:  testLogger(), health: health, poll: 24 * time.Hour, rescan: 24 * time.Hour, timeout: 24 * time.Hour,
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
