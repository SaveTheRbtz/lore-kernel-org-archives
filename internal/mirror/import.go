package mirror

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
)

const checkpointRefPrefix = "refs/meta/lore-mirror-import/"

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

type checkpointPosition struct {
	branch, marker string
	tip, current   plumbing.Hash
}

func importPosition(refs, destination map[string]plumbing.Hash) (checkpointPosition, error) {
	var p checkpointPosition
	for name := range refs {
		if strings.HasPrefix(name, "refs/heads/") {
			if p.branch != "" {
				return p, errors.New("checkpoint import requires exactly one source branch")
			}
			p.branch = name
		}
	}
	if p.branch == "" {
		return p, errors.New("checkpoint import requires a source branch")
	}
	p.tip, p.current = refs[p.branch], destination[p.branch]
	for name := range destination {
		if strings.HasPrefix(name, checkpointRefPrefix) {
			p.marker = name
		}
	}
	if p.marker == "" {
		if len(destination) > 1 || len(destination) == 1 && p.current == plumbing.ZeroHash {
			return p, errors.New("checkpoint import requires an empty destination or its single checkpoint branch")
		}
		return p, nil
	}
	hash := strings.TrimPrefix(p.marker, checkpointRefPrefix)
	if !plumbing.IsHash(hash) || plumbing.NewHash(hash) == plumbing.ZeroHash ||
		len(destination) != 2 || p.current == plumbing.ZeroHash || destination[p.marker] != p.current {
		return p, errors.New("invalid pinned checkpoint state")
	}
	p.tip = plumbing.NewHash(hash)
	return p, nil
}

// The marker pins the original source tip and advances atomically with the
// branch. Finishing that pinned import removes the marker before syncing mail
// that arrived later. Ineligible fallback preserves the original push error.
func (s *relaySession) importCheckpoints(ctx context.Context, refs map[string]plumbing.Hash, initialErr error) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	destination, err := relayRefs(s.dst, false)
	if err != nil {
		return err
	}
	p, positionErr := importPosition(refs, destination)
	supported := s.src.Capabilities.Supports(capability.Shallow) && s.src.Capabilities.Supports(capability.AllowReachableSHA1InWant) &&
		s.dst.Capabilities.Supports(capability.Atomic) && s.dst.Capabilities.Supports(capability.DeleteRefs)
	if !supported || positionErr != nil || initialErr != nil && p.marker != "" {
		if initialErr != nil {
			return initialErr
		}
		if positionErr != nil {
			return positionErr
		}
		return errors.New("source or destination does not support pinned checkpoint imports")
	}
	s.paced = true
	if initialErr != nil {
		// An error response does not prove that the failed push changed no refs.
		if err := s.wait(ctx); err != nil {
			return err
		}
		s.dst, err = s.down.AdvertisedReferencesContext(ctx)
		if err != nil {
			return fmt.Errorf("initial push: %v; read back failed import: %w", initialErr, err)
		}
		s.health.Progress("sync")
		destination, err = relayRefs(s.dst, false)
		if err != nil {
			return err
		}
		if maps.Equal(refs, destination) {
			return nil
		}
		p, positionErr = importPosition(refs, destination)
		if positionErr != nil || p.marker != "" || !s.dst.Capabilities.Supports(capability.Atomic) || !s.dst.Capabilities.Supports(capability.DeleteRefs) {
			return initialErr
		}
	}
	plan, err := s.checkpoints(ctx, s.source.String(), p.tip)
	if err != nil {
		if initialErr != nil {
			return fmt.Errorf("initial push: %v; checkpoint discovery: %w", initialErr, err)
		}
		return err
	}
	start := 0
	if p.current != plumbing.ZeroHash {
		index := slices.Index(plan, p.current)
		if index < 0 {
			if initialErr != nil {
				return initialErr
			}
			return errors.New("destination is not an exact source checkpoint; refusing to resume")
		}
		start = index + 1
	}
	if initialErr != nil {
		// Recovery errors now control retry timing. The failed full push's
		// size error must not impose a 24-hour delay on a failed small batch.
		defer func() {
			if err != nil {
				err = fmt.Errorf("initial push: %v; checkpoint import: %w", initialErr, err)
			}
		}()
		if s.log != nil {
			s.log.Warn("push_fallback", "source", s.source.String(), "error", initialErr, "request_bytes", s.bytes)
		}
	}
	// Even a marker already at the pinned tip still needs final removal.
	start = min(start, len(plan)-1)
	marker := checkpointRefPrefix + p.tip.String()
	final := maps.Clone(refs)
	final[p.branch] = p.tip
	if s.log != nil {
		s.log.Info("import_checkpoints", "source", s.source.String(), "checkpoints", len(plan), "remaining", len(plan)-start)
	}
	for index := start; index < len(plan); index++ {
		target := map[string]plumbing.Hash{p.branch: plan[index], marker: plan[index]}
		if index == len(plan)-1 {
			target = final
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
	return s.sync(ctx, refs)
}
