package observe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const stuckAfter = 5 * time.Minute

// Activity identifies an operation tracked by health and telemetry.
type Activity uint8

const (
	Discovery Activity = iota
	Sync
	activityCount
)

// Discovery and sync run independently. Idle time and retry backoff are healthy;
// only an active operation that stops making progress can become stuck.
type Health struct {
	// SelectionPending stays true until the first manifest validates the requested inboxes.
	SelectionPending atomic.Bool
	mu               sync.Mutex
	last             [activityCount]time.Time
}

func (h *Health) Begin(activity Activity) func() {
	if h == nil || activity >= activityCount {
		return func() {}
	}
	h.mu.Lock()
	h.last[activity] = time.Now()
	h.mu.Unlock()
	return func() {
		h.mu.Lock()
		h.last[activity] = time.Time{}
		h.mu.Unlock()
	}
}

func (h *Health) SyncProgress() {
	if h == nil {
		return
	}
	h.mu.Lock()
	if !h.last[Sync].IsZero() {
		h.last[Sync] = time.Now()
	}
	h.mu.Unlock()
}

func (h *Health) Stuck() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	for _, progress := range h.last {
		if !progress.IsZero() && now.Sub(progress) >= stuckAfter {
			return true
		}
	}
	return false
}

func (h *Health) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/healthz" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if h != nil && h.SelectionPending.Load() {
		http.Error(w, "starting", http.StatusServiceUnavailable)
		return
	}
	if h.Stuck() {
		http.Error(w, "stuck", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ok\n"))
}

// Bind before returning so a port conflict fails startup. The caller monitors
// failures and calls shutdown with a fresh context when the daemon exits.
func StartHealthServer(health *Health) (func(context.Context) error, <-chan error, error) {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		return nil, nil, fmt.Errorf("health listener: %w", err)
	}
	server := &http.Server{Handler: health, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	failures := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			failures <- fmt.Errorf("health server: %w", err)
		}
	}()
	shutdown := func(ctx context.Context) error {
		if err := server.Shutdown(ctx); err != nil {
			return errors.Join(err, server.Close())
		}
		return nil
	}
	return shutdown, failures, nil
}
