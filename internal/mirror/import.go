package mirror

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/go-git/go-git/v5/plumbing"
)

func checkpointRetry(err error) bool {
	if serverRetryAfter(err) > 0 {
		return false
	}
	var response *receivePackError
	return errors.Is(err, ErrPushTooLarge) || errors.As(err, &response) && response.status >= 500
}

func (s *relaySession) wait(ctx context.Context) error {
	if s.paced {
		return checkpointWait(ctx)
	}
	return ctx.Err()
}

// Intermediate imports contain only the checkpoint branch. A retry may resume
// from an exact checkpoint in the pinned source plan, never from an arbitrary
// destination commit. Metadata and other refs are published in the final batch.
func (s *relaySession) importCheckpoints(ctx context.Context, refs map[string]plumbing.Hash) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var branch string
	for name := range refs {
		if strings.HasPrefix(name, "refs/heads/") {
			if branch != "" {
				return errors.New("checkpoint import requires exactly one source branch")
			}
			branch = name
		}
	}
	if branch == "" {
		return errors.New("checkpoint import requires a source branch")
	}
	s.paced = true
	// The failed request may have reached GitHub despite its error response.
	if err := s.wait(ctx); err != nil {
		return err
	}
	var err error
	s.dst, err = s.down.AdvertisedReferencesContext(ctx)
	if err != nil {
		return fmt.Errorf("read back failed import: %w", err)
	}
	s.health.Progress("sync")
	destination, err := relayRefs(s.dst, false)
	if err != nil {
		return err
	}
	if maps.Equal(refs, destination) {
		return nil
	}
	current := destination[branch]
	if len(destination) > 1 || len(destination) == 1 && current == plumbing.ZeroHash {
		return errors.New("checkpoint import requires an empty destination or its single checkpoint branch")
	}
	plan, err := s.checkpoints(ctx, s.source.String(), refs[branch])
	if err != nil {
		return err
	}
	start := 0
	if current != plumbing.ZeroHash {
		index := slices.Index(plan, current)
		if index < 0 {
			return errors.New("destination is not an exact source checkpoint; refusing to resume")
		}
		start = index + 1
	}
	if s.log != nil {
		s.log.Info("import_checkpoints", "source", s.source.String(), "checkpoints", len(plan), "remaining", len(plan)-start)
	}
	for index := start; index < len(plan); index++ {
		target := map[string]plumbing.Hash{branch: plan[index]}
		if index == len(plan)-1 {
			target = refs
		}
		before := s.bytes
		if err := s.sync(ctx, target); err != nil {
			return fmt.Errorf("checkpoint %d of %d (%s), request_bytes=%d: %w", index+1, len(plan), plan[index], s.bytes-before, err)
		}
		s.batches++
		if s.log != nil {
			s.log.Info("import_checkpoint", "source", s.source.String(), "checkpoint", index+1, "total", len(plan), "commit", plan[index].String())
		}
	}
	// A previous attempt can have published the tip but not its metadata refs.
	return s.sync(ctx, refs)
}
