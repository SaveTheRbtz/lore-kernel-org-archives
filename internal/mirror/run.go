package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	gh "lore-mirror/internal/github"
	"lore-mirror/internal/lore"
	"lore-mirror/internal/observe"
)

type Config struct {
	Org, Lists       string
	Discover, Once   bool
	Recheck, Timeout time.Duration
	MaxPushBytes     int64
}

func Run(ctx context.Context, cfg Config, log *slog.Logger) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	client := newHTTPClient()
	defer client.CloseIdleConnections()
	var selectionReady atomic.Bool
	selectionReady.Store(cfg.Lists == "")
	fetch := func(ctx context.Context) (lore.Manifest, error) {
		ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		manifest, err := lore.FetchManifest(ctx, client, log, cfg.Lists, !selectionReady.Load())
		if err == nil {
			selectionReady.Store(true)
		}
		return manifest, err
	}
	if cfg.Discover {
		manifest, err := fetch(ctx)
		if err != nil {
			return err
		}
		return writeInventory(os.Stdout, manifest)
	}
	health := &observe.Health{Ready: selectionReady.Load}
	metrics, shutdownMetrics, err := observe.StartTelemetry(ctx, os.Getenv("GOOGLE_CLOUD_PROJECT"), health)
	if err != nil {
		return fmt.Errorf("initialize metrics: %w", err)
	}
	defer shutdown(log, "metrics_shutdown_failed", 10*time.Second, shutdownMetrics)
	github, err := gh.New(ctx, client, cfg.Org)
	if err != nil {
		return err
	}
	relay := &Relay{Client: newGitHTTPClient(client), MaxPushBytes: cfg.MaxPushBytes, metrics: metrics, health: health}
	// The single worker retains approved IDs even when a sync fails.
	repositoryIDs := map[string]int64{}
	syncRepo := func(ctx context.Context, sourcePath string, entry lore.ManifestEntry) error {
		started := time.Now()
		summary, err := repositorySummary(ctx, client, log, sourcePath, entry)
		if err != nil {
			return err
		}
		repo, err := github.EnsureRepository(ctx, sourcePath, summary, repositoryIDs[sourcePath])
		if err != nil {
			return err
		}
		repositoryIDs[sourcePath] = repo.ID
		health.Progress("sync")
		credential, err := github.Token(ctx)
		if err != nil {
			return err
		}
		health.Progress("sync")
		result, err := relay.Sync(ctx, lore.Origin+sourcePath, repo.GitURL, credential, repo.DefaultBranch, func(branch string) error {
			return github.SetDefaultBranch(ctx, repo.Name, branch)
		})
		if err != nil {
			return fmt.Errorf("sync attempt failed after %s, request_bytes=%d: %w", time.Since(started).Round(time.Millisecond), result.Bytes, err)
		}
		log.Info("synced", "source", sourcePath, "repository", repo.Name, "duration", time.Since(started).String(),
			"changed", result.Changed, "refs", len(result.SourceRefs), "bytes", result.Bytes)
		return nil
	}
	daemon := daemon{fetch: fetch, sync: syncRepo, log: log, metrics: metrics, queue: metrics.Queue, health: health, recheck: cfg.Recheck, timeout: cfg.Timeout}
	shutdownHealth, healthFailures, err := observe.StartHealthServer(health)
	if err != nil {
		return fmt.Errorf("start health server: %w", err)
	}
	defer shutdown(log, "health_server_shutdown_failed", 5*time.Second, shutdownHealth)
	log.Info("starting", "source", lore.Origin, "org", cfg.Org, "once", cfg.Once)
	run := daemon.run
	if cfg.Once {
		run = daemon.runOnce
	}
	finished := make(chan error, 1)
	go func() { finished <- run(ctx) }()
	select {
	case err := <-finished:
		if !cfg.Once && errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case err := <-healthFailures:
		cancel()
		return errors.Join(fmt.Errorf("health server stopped: %w", err), <-finished)
	}
}

func shutdown(log *slog.Logger, failureEvent string, timeout time.Duration, stop func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := stop(ctx); err != nil {
		log.Error(failureEvent, "error", err)
	}
}

func writeInventory(w io.Writer, manifest lore.Manifest) error {
	type item struct {
		Source     string `json:"source"`
		Repository string `json:"repository"`
		lore.ManifestEntry
	}
	items := make([]item, 0, len(manifest))
	for _, p := range lore.OrderedPaths(manifest) {
		name, _ := lore.RepositoryName(p)
		items = append(items, item{p, name, manifest[p]})
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(items)
}

func repositorySummary(ctx context.Context, client *http.Client, log *slog.Logger, path string, entry lore.ManifestEntry) (*string, error) {
	address, err := lore.FetchAddress(ctx, client, path)
	if err == nil {
		return new(entry.Summary(path, address)), nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if serverRetryAfter(err) > 0 {
		return nil, err
	}
	log.Warn("posting_address_unavailable", "source", path, "error", err)
	return nil, nil
}

func (c Config) validate() error {
	if c.Recheck <= 0 || c.Timeout <= 0 {
		return errors.New("recheck and timeout must be positive")
	}
	if c.MaxPushBytes <= 0 || c.MaxPushBytes >= 2*(1<<30) {
		return errors.New("max-push-bytes must be positive and less than 2 GiB")
	}
	return lore.ValidateLists(c.Lists)
}
