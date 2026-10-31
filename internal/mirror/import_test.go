package mirror

import (
	"errors"
	"maps"
	"testing"
	"testing/synctest"
	"time"

	gh "lore-mirror/internal/github"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/google/go-cmp/cmp"
)

func TestCheckpointRetryHonorsCooldownWhenTheRequestAlsoExceedsItsLimit(t *testing.T) {
	err := errors.Join(ErrPushTooLarge, gh.WithRetryAfter(errors.New("throttled"), time.Minute))
	if checkpointRetry(err) {
		t.Fatal("checkpoint discovery bypassed the server cooldown")
	}
}

func checkpointTestServer(t *testing.T) (*relayTestServer, plumbing.Hash, plumbing.Hash, plumbing.Hash) {
	t.Helper()
	s := newRelayTestServer(t)
	first := relayTestCommit(t, s.source, "oldest mail")
	middle := relayTestCommit(t, s.source, "middle mail", first)
	tip := relayTestCommit(t, s.source, "newest mail", middle)
	origin := relayTestCommit(t, s.source, "independent origin metadata")
	relayTestSetRef(t, s.source, "refs/heads/master", tip)
	relayTestSetRef(t, s.source, "refs/meta/origins", origin)
	s.boundaries = map[plumbing.Hash][]plumbing.Hash{tip: {middle}, middle: {first}}
	return s, first, middle, tip
}

func checkpointTestRefs(t *testing.T, store storer.ReferenceStorer) map[string]plumbing.Hash {
	t.Helper()
	refs := map[string]plumbing.Hash{}
	iter, err := store.IterReferences()
	if err != nil {
		t.Fatal(err)
	}
	if err := iter.ForEach(func(ref *plumbing.Reference) error {
		if ref.Type() == plumbing.HashReference {
			refs[ref.Name().String()] = ref.Hash()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return refs
}

func TestCheckpointImportPublishesCompleteHistoryAndFinalMetadata(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, first, middle, _ := checkpointTestServer(t)
		var attempted []map[string]plumbing.Hash
		var previousPush time.Time
		s.rejectPush = func(req *packp.ReferenceUpdateRequest) bool {
			if !previousPush.IsZero() && time.Since(previousPush) < 10*time.Second {
				t.Error("checkpoint pushes exceeded six per minute")
			}
			previousPush = time.Now()
			targets := map[string]plumbing.Hash{}
			for _, command := range req.Commands {
				targets[command.Name.String()] = command.New
			}
			attempted = append(attempted, targets)
			return len(attempted) == 1
		}
		result, err := s.sync(t, 0)
		if err != nil || !result.Changed || result.Batches != 3 {
			t.Fatalf("checkpoint import = %+v, %v", result, err)
		}
		final := checkpointTestRefs(t, s.source)
		want := []map[string]plumbing.Hash{
			final,
			{"refs/heads/master": first},
			{"refs/heads/master": middle},
			final,
		}
		if diff := cmp.Diff(want, attempted); diff != "" {
			t.Fatalf("receive-pack targets (-want +got):\n%s", diff)
		}
		if got := checkpointTestRefs(t, s.destination); !maps.Equal(got, final) {
			t.Fatalf("final refs = %v, want %v", got, final)
		}
		objects, err := s.source.IterEncodedObjects(plumbing.AnyObject)
		if err != nil {
			t.Fatal(err)
		}
		if err := objects.ForEach(func(object plumbing.EncodedObject) error {
			_, err := s.destination.EncodedObject(object.Type(), object.Hash())
			return err
		}); err != nil {
			t.Fatalf("checkpoint import omitted a source commit, tree, or blob: %v", err)
		}
	})
}

func TestCheckpointImportResumesAfterFailedBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, first, middle, tip := checkpointTestServer(t)
		var attempted []map[string]plumbing.Hash
		failedBatch := false
		s.rejectPush = func(req *packp.ReferenceUpdateRequest) bool {
			targets := map[string]plumbing.Hash{}
			for _, command := range req.Commands {
				targets[command.Name.String()] = command.New
			}
			attempted = append(attempted, targets)
			for _, command := range req.Commands {
				if command.Name == plumbing.Master {
					if command.New == tip && command.Old != middle {
						return true // Reject the full import, including the next Sync's attempt.
					}
					if command.New == middle && !failedBatch {
						failedBatch = true
						return true
					}
				}
			}
			return false
		}
		result, err := s.sync(t, 0)
		if err == nil || result.Batches != 1 || len(attempted) != 3 {
			t.Fatalf("failed batch did not stop the import: %+v, %v, targets=%v", result, err, attempted)
		}
		checkpoint := map[string]plumbing.Hash{"refs/heads/master": first}
		if got := checkpointTestRefs(t, s.destination); !maps.Equal(got, checkpoint) {
			t.Fatalf("failed batch changed the published checkpoint: %v", got)
		}
		attempted = nil
		result, err = s.sync(t, 0)
		if err != nil || !result.Changed || result.Batches != 2 {
			t.Fatalf("resume = %+v, %v", result, err)
		}
		final := checkpointTestRefs(t, s.source)
		want := []map[string]plumbing.Hash{final, {"refs/heads/master": middle}, final}
		if diff := cmp.Diff(want, attempted); diff != "" {
			t.Fatalf("resumed receive-pack targets (-want +got):\n%s", diff)
		}
		if got := checkpointTestRefs(t, s.destination); !maps.Equal(got, final) {
			t.Fatalf("resumed refs = %v, want %v", got, final)
		}
	})
}

func TestCheckpointImportRefusesAmbiguousState(t *testing.T) {
	for name, prepare := range map[string]func(*relayTestServer, plumbing.Hash, plumbing.Hash, plumbing.Hash) map[string]plumbing.Hash{
		"destination between checkpoints": func(s *relayTestServer, first, middle, tip plumbing.Hash) map[string]plumbing.Hash {
			s.boundaries[tip] = []plumbing.Hash{first}
			return map[string]plumbing.Hash{"refs/heads/master": middle}
		},
		"additional destination ref": func(_ *relayTestServer, first, _, _ plumbing.Hash) map[string]plumbing.Hash {
			return map[string]plumbing.Hash{"refs/heads/master": first, "refs/meta/manual": first}
		},
		"ambiguous boundary after a valid one": func(s *relayTestServer, first, middle, tip plumbing.Hash) map[string]plumbing.Hash {
			s.boundaries[middle] = []plumbing.Hash{first, tip}
			return map[string]plumbing.Hash{}
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, first, middle, tip := checkpointTestServer(t)
				original := prepare(s, first, middle, tip)
				objects, err := s.source.IterEncodedObjects(plumbing.AnyObject)
				if err != nil {
					t.Fatal(err)
				}
				if err := objects.ForEach(func(object plumbing.EncodedObject) error {
					_, err := s.destination.SetEncodedObject(object)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				for name, hash := range original {
					relayTestSetRef(t, s.destination, name, hash)
				}
				s.rejectPush = func(*packp.ReferenceUpdateRequest) bool { return s.pushes == 1 }
				if _, err := s.sync(t, 0); err == nil {
					t.Fatal("ambiguous checkpoint import succeeded")
				}
				if s.pushes != 1 {
					t.Fatalf("ambiguous import attempted checkpoint writes: %d pushes", s.pushes)
				}
				if got := checkpointTestRefs(t, s.destination); !maps.Equal(got, original) {
					t.Fatalf("ambiguous import changed destination refs: %v, want %v", got, original)
				}
			})
		})
	}
}
