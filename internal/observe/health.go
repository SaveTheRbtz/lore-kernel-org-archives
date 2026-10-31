package observe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

const stuckAfter = 5 * time.Minute

// Discovery and sync run independently. Idle time and retry backoff are healthy;
// only an active operation that stops making progress can become stuck.
type Health struct {
	// Ready, when set, reports whether startup validation has finished.
	Ready func() bool
	mu    sync.Mutex
	last  [2]time.Time
}

func healthActivity(operation string) int {
	switch operation {
	case "discovery":
		return 0
	case "sync":
		return 1
	default:
		return -1
	}
}

func (h *Health) Begin(operation string) func() {
	index := healthActivity(operation)
	if h == nil || index < 0 {
		return func() {}
	}
	h.mu.Lock()
	h.last[index] = time.Now()
	h.mu.Unlock()
	return func() {
		h.mu.Lock()
		h.last[index] = time.Time{}
		h.mu.Unlock()
	}
}

func (h *Health) Progress(operation string) {
	index := healthActivity(operation)
	if h == nil || index < 0 {
		return
	}
	h.mu.Lock()
	if !h.last[index].IsZero() {
		h.last[index] = time.Now()
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
	if h != nil && h.Ready != nil && !h.Ready() {
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
		defer close(failures)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
