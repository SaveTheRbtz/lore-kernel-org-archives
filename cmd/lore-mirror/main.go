package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"lore-mirror/internal/mirror"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	var cfg mirror.Config
	var healthcheck bool
	org := os.Getenv("GITHUB_ORG")
	if org == "" {
		org = "lore-kernel-org-archives"
	}
	flag.StringVar(&cfg.Source, "source", "https://lore.kernel.org", "source origin, with matching /manifest.js.gz")
	flag.StringVar(&cfg.Org, "org", org, "dedicated GitHub organization (or GITHUB_ORG)")
	flag.StringVar(&cfg.Lists, "lists", "", "comma-separated inboxes to mirror; empty means all")
	flag.BoolVar(&cfg.Discover, "discover", false, "print inventory and exit without contacting GitHub")
	flag.BoolVar(&cfg.Once, "once", false, "attempt each selected repository once, then exit")
	flag.BoolVar(&healthcheck, "healthcheck", false, "check the local /healthz endpoint and exit")
	flag.DurationVar(&cfg.Poll, "poll", time.Minute, "manifest polling interval")
	flag.DurationVar(&cfg.Recheck, "recheck", 10*time.Minute, "recheck repositories at this interval after a successful sync")
	flag.DurationVar(&cfg.Timeout, "timeout", 30*time.Minute, "maximum time for one repository sync")
	flag.Int64Var(&cfg.MaxPushBytes, "max-push-bytes", 7*(1<<28), "maximum receive-pack request bytes, below GitHub's 2 GiB limit")
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flag.Args())
	}
	if healthcheck {
		client := &http.Client{Timeout: 5 * time.Second}
		response, err := client.Get("http://127.0.0.1:8080/healthz")
		if err != nil {
			return fmt.Errorf("healthcheck: %w", err)
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("healthcheck: HTTP %d", response.StatusCode)
		}
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return mirror.Run(ctx, cfg, log)
}
