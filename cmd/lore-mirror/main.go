package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
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
	org := os.Getenv("GITHUB_ORG")
	if org == "" {
		org = "lore-kernel-org-archives"
	}
	flag.StringVar(&cfg.Org, "org", org, "dedicated GitHub organization (or GITHUB_ORG)")
	flag.StringVar(&cfg.Lists, "lists", "", "comma-separated inboxes to mirror; empty means all")
	flag.BoolVar(&cfg.Discover, "discover", false, "print inventory and exit without contacting GitHub")
	flag.BoolVar(&cfg.Once, "once", false, "attempt each selected repository once, then exit")
	flag.DurationVar(&cfg.Recheck, "recheck", 10*time.Minute, "interval for manifest discovery and successful repository rechecks, plus jitter")
	flag.DurationVar(&cfg.Timeout, "timeout", 30*time.Minute, "maximum time for one repository sync")
	flag.Int64Var(&cfg.MaxPushBytes, "max-push-bytes", mirror.DefaultMaxPushBytes, "maximum receive-pack request bytes, below GitHub's 2 GiB limit")
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flag.Args())
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return mirror.Run(ctx, cfg, log)
}
