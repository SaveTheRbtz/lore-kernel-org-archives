package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type config struct {
	source, org, lists          string
	discover, once, healthcheck bool
	poll, rescan, timeout       time.Duration
	maxPushBytes                int64
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	var cfg config
	org := os.Getenv("GITHUB_ORG")
	if org == "" {
		org = "lore-kernel-org-archives"
	}
	flag.StringVar(&cfg.source, "source", "https://lore.kernel.org", "source origin, with matching /manifest.js.gz")
	flag.StringVar(&cfg.org, "org", org, "dedicated GitHub organization (or GITHUB_ORG)")
	flag.StringVar(&cfg.lists, "lists", "", "comma-separated inboxes to mirror; empty means all")
	flag.BoolVar(&cfg.discover, "discover", false, "print inventory and exit without contacting GitHub")
	flag.BoolVar(&cfg.once, "once", false, "attempt each selected repository once, then exit")
	flag.BoolVar(&cfg.healthcheck, "healthcheck", false, "check the local /healthz endpoint and exit")
	flag.DurationVar(&cfg.poll, "poll", time.Minute, "manifest polling interval")
	flag.DurationVar(&cfg.rescan, "rescan", 24*time.Hour, "reconcile unchanged repositories at this interval")
	flag.DurationVar(&cfg.timeout, "timeout", 30*time.Minute, "maximum time for one repository sync")
	flag.Int64Var(&cfg.maxPushBytes, "max-push-bytes", 7*(1<<28), "maximum receive-pack request bytes, below GitHub's 2 GiB limit")
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flag.Args())
	}
	if cfg.healthcheck {
		client := newHTTPClient()
		client.Timeout = 5 * time.Second
		response, err := client.Get("http://127.0.0.1:8080/healthz")
		if err != nil {
			return fmt.Errorf("healthcheck: %w", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("healthcheck: HTTP %d", response.StatusCode)
		}
		return nil
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := newHTTPClient()
	fetch := func(ctx context.Context) (Manifest, error) {
		ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		manifest, err := FetchManifest(ctx, client, cfg.source)
		if err != nil {
			return nil, err
		}
		return selectManifest(manifest, cfg.lists)
	}
	if cfg.discover {
		manifest, err := fetch(ctx)
		if err != nil {
			return err
		}
		type item struct {
			Source     string `json:"source"`
			Repository string `json:"repository"`
			ManifestEntry
		}
		items := make([]item, 0, len(manifest))
		for _, p := range orderedPaths(manifest) {
			name, _ := RepositoryName(p)
			items = append(items, item{p, name, manifest[p]})
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(items)
	}
	health := newHealthState()
	metrics, shutdownMetrics, err := startTelemetry(ctx, os.Getenv("GOOGLE_CLOUD_PROJECT"), health)
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
	token, err := tokenSource(ctx, client)
	if err != nil {
		return err
	}
	github := &GitHub{Client: client, Org: cfg.org, Token: token}
	relay := &Relay{Client: client, MaxPushBytes: cfg.maxPushBytes, metrics: metrics, health: health}
	// The single worker owns these caches. Restarting simply rechecks the API.
	repositories := map[string]Repository{}
	defaultBranches := map[string]string{}
	syncRepo := func(ctx context.Context, sourcePath string, entry ManifestEntry) error {
		repository, exists := repositories[sourcePath]
		if !exists {
			var err error
			repository, err = github.EnsureRepository(ctx, sourcePath, entry.Description)
			if err != nil {
				return err
			}
			repositories[sourcePath] = repository
		}
		health.progress("sync")
		sourceURL, err := SourceRepositoryURL(cfg.source, sourcePath)
		if err != nil {
			return err
		}
		credential, err := token(ctx)
		if err != nil {
			return err
		}
		health.progress("sync")
		result, err := relay.Sync(ctx, sourceURL, repository.GitURL, credential)
		if err != nil {
			delete(repositories, sourcePath)
			delete(defaultBranches, sourcePath)
			return err
		}
		if result.DefaultBranch != "" && defaultBranches[sourcePath] != result.DefaultBranch {
			if err := github.SetDefaultBranch(ctx, repository.Name, result.DefaultBranch); err != nil {
				return err
			}
			defaultBranches[sourcePath] = result.DefaultBranch
		}
		log.Info("refs_verified", "source", sourcePath, "repository", repository.Name,
			"changed", result.Changed, "refs", len(result.SourceRefs), "bytes", result.Bytes)
		return nil
	}
	daemon := daemon{fetch: fetch, sync: syncRepo, log: log, metrics: metrics, health: health, poll: cfg.poll, rescan: cfg.rescan, timeout: cfg.timeout}
	shutdownHealth, healthFailures, err := startHealthServer(health)
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
	log.Info("starting", "source", cfg.source, "org", cfg.org, "once", cfg.once)
	finished := make(chan error, 1)
	go func() {
		if cfg.once {
			finished <- daemon.runOnce(ctx)
		} else {
			finished <- daemon.run(ctx)
		}
	}()
	select {
	case err = <-finished:
	case err = <-healthFailures:
		stop()
		<-finished
		return fmt.Errorf("health server stopped: %w", err)
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (c config) validate() error {
	u, err := url.Parse(c.source)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("-source must be an HTTPS origin without credentials, path, query, or fragment")
	}
	if c.poll <= 0 || c.rescan <= 0 || c.timeout <= 0 {
		return errors.New("poll, rescan, and timeout must be positive")
	}
	if c.maxPushBytes <= 0 || c.maxPushBytes >= 2*(1<<30) {
		return errors.New("max-push-bytes must be positive and less than 2 GiB")
	}
	if !c.discover && (c.org == "" || strings.ContainsAny(c.org, "/\\ .:@?#")) {
		return errors.New("set -org or GITHUB_ORG to the dedicated GitHub organization")
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
