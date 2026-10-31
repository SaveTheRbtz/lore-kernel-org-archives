package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	gh "lore-mirror/internal/github"
	"lore-mirror/internal/lore"
	"lore-mirror/internal/observe"
)

type Config struct {
	Org, Lists             string
	Discover, Once         bool
	Poll, Recheck, Timeout time.Duration
	MaxPushBytes           int64
}

func Run(ctx context.Context, cfg Config, log *slog.Logger) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	client := newHTTPClient()
	fetch := func(ctx context.Context) (lore.Manifest, error) {
		ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		manifest, err := lore.FetchManifest(ctx, client)
		if err != nil {
			return nil, err
		}
		return lore.SelectManifest(manifest, cfg.Lists)
	}
	if cfg.Discover {
		manifest, err := fetch(ctx)
		if err != nil {
			return err
		}
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
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(items)
	}
	health := &observe.Health{}
	metrics, shutdownMetrics, err := observe.StartTelemetry(ctx, os.Getenv("GOOGLE_CLOUD_PROJECT"), health)
	if err != nil {
		return fmt.Errorf("initialize metrics: %w", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := shutdownMetrics(ctx); err != nil {
			log.Error("metrics_shutdown_failed", "error", err)
		}
	}()
	github, err := gh.New(ctx, client, cfg.Org)
	if err != nil {
		return err
	}
	relay := &Relay{Client: client, MaxPushBytes: cfg.MaxPushBytes, metrics: metrics, health: health}
	// The single worker retains approved IDs even when a sync fails.
	repositories := map[string]gh.Repository{}
	syncRepo := func(ctx context.Context, sourcePath string, entry lore.ManifestEntry) error {
		started := time.Now()
		repo, err := github.EnsureRepository(ctx, sourcePath, entry.Description, repositories[sourcePath].ID)
		if err != nil {
			return err
		}
		repositories[sourcePath] = repo
		health.Progress("sync")
		credential, err := github.Token(ctx)
		if err != nil {
			return err
		}
		health.Progress("sync")
		result, err := relay.Sync(ctx, lore.Origin+sourcePath, repo.GitURL, credential)
		if err != nil {
			return err
		}
		if result.DefaultBranch != "" && repo.DefaultBranch != result.DefaultBranch {
			if err := github.SetDefaultBranch(ctx, repo.Name, result.DefaultBranch); err != nil {
				return err
			}
			repo.DefaultBranch = result.DefaultBranch
			repositories[sourcePath] = repo
		}
		log.Info("synced", "source", sourcePath, "repository", repo.Name, "duration", time.Since(started).String(),
			"changed", result.Changed, "refs", len(result.SourceRefs), "bytes", result.Bytes)
		return nil
	}
	daemon := daemon{fetch: fetch, sync: syncRepo, log: log, metrics: metrics, health: health, poll: cfg.Poll, recheck: cfg.Recheck, timeout: cfg.Timeout}
	shutdownHealth, healthFailures, err := observe.StartHealthServer(health)
	if err != nil {
		return fmt.Errorf("start health server: %w", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownHealth(ctx); err != nil {
			log.Error("health_server_shutdown_failed", "error", err)
		}
	}()
	log.Info("starting", "source", lore.Origin, "org", cfg.Org, "once", cfg.Once)
	finished := make(chan error, 1)
	go func() {
		if cfg.Once {
			finished <- daemon.runOnce(ctx)
		} else {
			finished <- daemon.run(ctx)
		}
	}()
	select {
	case err = <-finished:
	case err = <-healthFailures:
		cancel()
		return errors.Join(fmt.Errorf("health server stopped: %w", err), <-finished)
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (c Config) validate() error {
	if c.Poll <= 0 || c.Recheck <= 0 || c.Timeout <= 0 {
		return errors.New("poll, recheck, and timeout must be positive")
	}
	if c.MaxPushBytes <= 0 || c.MaxPushBytes >= 2*(1<<30) {
		return errors.New("max-push-bytes must be positive and less than 2 GiB")
	}
	return nil
}

func newHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 4
	transport.ResponseHeaderTimeout = time.Minute
	return &http.Client{
		Transport: transport,
		// No request, especially an authenticated push, may change origin.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}
