package mirror

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"lore-mirror/internal/observe"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/sideband"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestRelayInitialIncrementalAndRefChanges(t *testing.T) {
	s := newRelayTestServer(t)
	first := relayTestCommit(t, s.source, "first mail")
	relayTestSetRef(t, s.source, "refs/heads/master", first)
	relayTestSetRef(t, s.source, "refs/meta/origins", first)
	if err := s.source.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.Master)); err != nil {
		t.Fatal(err)
	}
	result, err := s.sync(t, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := SyncResult{Changed: true, DefaultBranch: "master", SourceRefs: map[string]plumbing.Hash{
		"refs/heads/master": first,
		"refs/meta/origins": first,
	}}
	if diff := cmp.Diff(want, result, cmpopts.IgnoreFields(SyncResult{}, "Bytes")); diff != "" {
		t.Fatalf("initial sync (-want +got):\n%s", diff)
	}
	if _, err := s.destination.EncodedObject(plumbing.CommitObject, first); err != nil {
		t.Fatalf("destination lacks commit: %v", err)
	}
	result, err = s.sync(t, 0)
	if err != nil || result.Changed || s.pushes != 1 || s.uploads != 1 {
		t.Fatalf("unchanged sync did work: %+v %v pushes=%d uploads=%d", result, err, s.pushes, s.uploads)
	}
	second := relayTestCommit(t, s.source, "second mail", first)
	relayTestSetRef(t, s.source, "refs/heads/master", second)
	result, err = s.sync(t, 0)
	if err != nil || !result.Changed {
		t.Fatalf("incremental sync: %+v %v", result, err)
	}
	if ref, err := s.destination.Reference(plumbing.Master); err != nil || ref.Hash() != second {
		t.Fatalf("incremental destination ref: %v, error %v; want %s", ref, err, second)
	}
	if len(s.haves) != 1 || s.haves[0] != first {
		t.Fatalf("destination tips not used as haves: %v", s.haves)
	}
	// A new ref to an object already at GitHub needs an empty pack, not a fetch.
	relayTestSetRef(t, s.source, "refs/tags/existing", second)
	_, err = s.sync(t, 0)
	if err != nil || s.uploads != 2 {
		t.Fatalf("ref-only sync: %v, uploads=%d", err, s.uploads)
	}
	// Deleted source metadata is removed; GitHub's own refs remain untouched.
	if err := s.source.RemoveReference("refs/meta/origins"); err != nil {
		t.Fatal(err)
	}
	relayTestSetRef(t, s.destination, "refs/pull/1/head", first)
	_, err = s.sync(t, 0)
	if err != nil {
		t.Fatalf("delete-only sync: %v", err)
	}
	if _, err := s.destination.Reference("refs/meta/origins"); err != plumbing.ErrReferenceNotFound {
		t.Fatal("removed source ref retained at destination")
	}
	if _, err := s.destination.Reference("refs/pull/1/head"); err != nil {
		t.Fatal("GitHub-managed ref was removed")
	}
	// Rewinds are authoritative and can reuse objects already at the target.
	relayTestSetRef(t, s.source, "refs/heads/master", first)
	if _, err := s.sync(t, 0); err != nil {
		t.Fatalf("source correction: %v", err)
	}
	if ref, err := s.destination.Reference(plumbing.Master); err != nil || ref.Hash() != first {
		t.Fatalf("rewound destination ref: %v, error %v; want %s", ref, err, first)
	}
}

func TestRelayLimitsWholeRequestsAndRetriesWithoutPublishingPartialRefs(t *testing.T) {
	for _, mode := range []string{"pack truncated", "command overhead"} {
		t.Run(mode, func(t *testing.T) {
			s := newRelayTestServer(t)
			hash := relayTestCommit(t, s.source, "mail")
			name, limit := plumbing.Master, int64(180)
			relayTestSetRef(t, s.source, string(name), hash)
			if mode == "command overhead" {
				if _, err := s.sync(t, 0); err != nil {
					t.Fatal(err)
				}
				name, limit = "refs/tags/existing", 32 // The empty pack fits; update commands do not.
				relayTestSetRef(t, s.source, string(name), hash)
			}
			failed, err := s.sync(t, limit)
			if !errors.Is(err, ErrPushTooLarge) || failed.Bytes != limit {
				t.Fatalf("bounded request: %+v %v", failed, err)
			}
			if _, err := s.destination.Reference(name); !errors.Is(err, plumbing.ErrReferenceNotFound) {
				t.Fatalf("partial request published %s: %v", name, err)
			}
			succeeded, err := s.sync(t, 0)
			if err != nil || !succeeded.Changed || succeeded.Bytes <= limit {
				t.Fatalf("retry: %+v %v", succeeded, err)
			}
			ref, err := s.destination.Reference(name)
			if err != nil || ref.Hash() != hash {
				t.Fatalf("retry did not publish %s: %v", name, err)
			}
		})
	}
}

func TestRelaySourceFailureAndConcurrentDestinationChange(t *testing.T) {
	t.Run("source sideband failure", func(t *testing.T) {
		s := newRelayTestServer(t)
		hash := relayTestCommit(t, s.source, "mail")
		relayTestSetRef(t, s.source, "refs/heads/master", hash)
		s.failSource = true
		_, err := s.sync(t, 0)
		if err == nil || !strings.Contains(err.Error(), "injected source failure") {
			t.Fatalf("source error lost: %v", err)
		}
		if _, err := s.destination.Reference(plumbing.Master); err != plumbing.ErrReferenceNotFound {
			t.Fatal("failed source transfer published a ref")
		}
	})
	t.Run("CAS detects concurrent creation", func(t *testing.T) {
		s := newRelayTestServer(t)
		hash := relayTestCommit(t, s.source, "mail")
		relayTestSetRef(t, s.source, "refs/heads/master", hash)
		other := relayTestCommit(t, s.destination, "concurrent change")
		s.beforePush = func() { relayTestSetRef(t, s.destination, "refs/heads/master", other) }
		if _, err := s.sync(t, 0); err == nil || !strings.Contains(err.Error(), "stale old object") {
			t.Fatalf("concurrent update not rejected: %v", err)
		}
		ref, err := s.destination.Reference(plumbing.Master)
		if err != nil {
			t.Fatal(err)
		}
		if ref.Hash() != other {
			t.Fatal("concurrent ref overwritten")
		}
	})
}

func TestRelayRequiresCompleteStatusAndReadBack(t *testing.T) {
	for _, mode := range []string{"missing status", "false success"} {
		t.Run(mode, func(t *testing.T) {
			s := newRelayTestServer(t)
			hash := relayTestCommit(t, s.source, "mail")
			relayTestSetRef(t, s.source, "refs/heads/master", hash)
			s.omitStatus = mode == "missing status"
			s.skipUpdate = mode == "false success"
			if _, err := s.sync(t, 0); err == nil {
				t.Fatal("invalid receiver success accepted")
			}
		})
	}
}

func TestRelayPreservesThrottlingAtEveryHTTPStage(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		request    int32
		status     int
		header     string
		delay      time.Duration
	}{
		{"source advertisement", "/source.git/info/refs", 1, 403, "42", 42 * time.Second},
		{"destination advertisement", "/destination.git/info/refs", 1, 429, "", time.Minute},
		{"upload-pack", "/source.git/git-upload-pack", 1, 503, "42", 42 * time.Second},
		{"verification", "/destination.git/info/refs", 2, 429, "42", 42 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newRelayTestServer(t)
			hash := relayTestCommit(t, s.source, "mail")
			relayTestSetRef(t, s.source, "refs/heads/master", hash)
			var requests atomic.Int32
			s.beforeHTTP = func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != tc.path || requests.Add(1) != tc.request {
					return false
				}
				w.Header().Set("Retry-After", tc.header)
				http.Error(w, "throttled test-token", tc.status)
				return true
			}
			_, err := s.sync(t, 0)
			if delay := serverRetryAfter(err); delay != tc.delay {
				t.Fatalf("scheduler delay = %s, want %s; error=%v", delay, tc.delay, err)
			}
			if strings.Contains(err.Error(), "test-token") {
				t.Fatalf("throttle error disclosed a credential: %v", err)
			}
		})
	}
}

func TestRelaySidebandProgressKeepsSyncHealthy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		s := newRelayTestServer(t)
		hash := relayTestCommit(t, s.source, "mail")
		relayTestSetRef(t, s.source, "refs/heads/master", hash)
		progress := make(chan struct{}, 6)
		s.beforePack = func(w http.ResponseWriter) error {
			mux := sideband.NewMuxer(sideband.Sideband64k, w)
			for range 6 {
				time.Sleep(time.Minute)
				if _, err := mux.WriteChannel(sideband.ProgressMessage, []byte("preparing pack\n")); err != nil {
					return err
				}
				w.(http.Flusher).Flush()
				progress <- struct{}{}
			}
			<-ctx.Done() // No archive bytes are sent during this preparation.
			return ctx.Err()
		}
		health := &observe.Health{}
		finish := health.Begin("sync")
		defer finish()
		r := Relay{Client: s.client, MaxPushBytes: DefaultMaxPushBytes, health: health}
		done := make(chan error, 1)
		go func() {
			_, err := r.Sync(ctx, "https://example.com/source.git", "https://example.com/destination.git", "test-token")
			done <- err
		}()
		for range 6 {
			select {
			case <-progress:
			case err := <-done:
				t.Fatalf("sync ended during preparation: %v", err)
			}
			synctest.Wait()
			if health.Stuck() {
				t.Fatal("sync marked stuck while receiving source progress")
			}
		}
		time.Sleep(5 * time.Minute)
		if !health.Stuck() {
			t.Error("sync stayed healthy after source progress stopped")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel stalled preparation: %v", err)
		}
	})
}

func TestRelayRejectsUnsafeEndpointsAndRedirects(t *testing.T) {
	for _, endpoint := range []string{"file:///tmp/repo", "/tmp/repo", "git@example.org:repo", "http://example.org/repo", "https://user:secret@example.org/repo", "https://example.org/repo?x=1"} {
		if _, err := relayEndpoint(endpoint); err == nil {
			t.Errorf("accepted %q", endpoint)
		}
	}
	var hit atomic.Bool
	origin := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host == "target.example" {
			hit.Store(true)
			return
		}
		http.Redirect(w, r, "https://target.example/repo", http.StatusFound)
	}))
	relay := Relay{Client: relayTestClient(origin), MaxPushBytes: 4096}
	pack := io.NopCloser(bytes.NewReader(relayEmptyPack()))
	if _, err := relay.receivePack(t.Context(), "https://origin.example/repo", "secret", testUpdate(t, pack)); err == nil || hit.Load() {
		t.Fatal("credential-bearing redirect followed")
	}
}

func testUpdate(t *testing.T, pack io.ReadCloser) *packp.ReferenceUpdateRequest {
	t.Helper()
	update := packp.NewReferenceUpdateRequest()
	if err := update.Capabilities.Set(capability.ReportStatus); err != nil {
		t.Fatal(err)
	}
	update.Commands = []*packp.Command{{Name: plumbing.Master, New: plumbing.NewHash(strings.Repeat("1", 40))}}
	update.Packfile = pack
	return update
}

func TestReceivePackForwardsPrefixBeforeSourceTailExists(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		prefixReceived := make(chan struct{})
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		pack := relayEmptyPack()
		receiver := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			req := packp.NewReferenceUpdateRequest()
			if err := req.Decode(r.Body); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			prefix := make([]byte, 12)
			if _, err := io.ReadFull(req.Packfile, prefix); err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(prefix, pack[:12]) {
				t.Errorf("received prefix %x, want %x", prefix, pack[:12])
			}
			close(prefixReceived)
			tail, err := io.ReadAll(req.Packfile)
			if err != nil || !bytes.Equal(tail, pack[12:]) {
				t.Errorf("received tail %x, error %v", tail, err)
			}
			w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
			if err := (&packp.ReportStatus{UnpackStatus: "ok", CommandStatuses: []*packp.CommandStatus{{ReferenceName: plumbing.Master, Status: "ok"}}}).Encode(w); err != nil {
				t.Error(err)
			}
		}))
		relay := Relay{Client: relayTestClient(receiver), MaxPushBytes: 4096}
		packReader, packWriter := io.Pipe()
		producerDone := make(chan struct{})
		go func() {
			defer close(producerDone)
			_, err := packWriter.Write(pack[:12])
			if err == nil {
				select {
				case <-prefixReceived:
					_, err = packWriter.Write(pack[12:])
				case <-ctx.Done():
					err = ctx.Err()
				}
			}
			if closeErr := packWriter.CloseWithError(err); closeErr != nil {
				t.Error(closeErr)
			}
		}()
		if _, err := relay.receivePack(ctx, "https://example.com", "test-token", testUpdate(t, packReader)); err != nil {
			t.Fatal(err)
		}
		<-producerDone
	})
}

// A source that has entered Read but cannot produce bytes until closed.
type blockedPack struct {
	started, closed     chan struct{}
	readOnce, closeOnce sync.Once
}

func newBlockedPack() *blockedPack {
	return &blockedPack{started: make(chan struct{}), closed: make(chan struct{})}
}
func (p *blockedPack) Read([]byte) (int, error) {
	p.readOnce.Do(func() { close(p.started) })
	<-p.closed
	return 0, io.ErrClosedPipe
}
func (p *blockedPack) Close() error { p.closeOnce.Do(func() { close(p.closed) }); return nil }

func TestReceivePackEarlyRejectionClosesBlockedSourceAndRedactsToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pack := newBlockedPack()
		receiver := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-pack.started
			w.Header().Set("Connection", "close")
			http.Error(w, "rejected test-token", http.StatusForbidden)
			w.(http.Flusher).Flush()
		}))
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		relay := Relay{Client: relayTestClient(receiver), MaxPushBytes: 4096}
		_, err := relay.receivePack(ctx, "https://example.com", "test-token", testUpdate(t, pack))
		if err == nil || strings.Contains(err.Error(), "test-token") || !strings.Contains(err.Error(), "403") {
			t.Fatalf("unexpected error: %v", err)
		}
		if ctx.Err() != nil {
			t.Fatal("early rejection waited for the job timeout")
		}
		select {
		case <-pack.closed:
		default:
			t.Fatal("source was not closed")
		}
	})
}

func TestReceivePackPreservesRetryAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			status int
			header string
			delay  time.Duration
		}{
			{429, "180", 3 * time.Minute},
			{429, time.Now().Add(3 * time.Minute).UTC().Format(http.TimeFormat), 3 * time.Minute},
			{429, "", time.Minute},
			{429, "invalid", time.Minute},
			{503, "180", 3 * time.Minute},
			{403, "", 0},
		} {
			receiver := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", tc.header)
				http.Error(w, "rejected", tc.status)
			}))
			relay := Relay{Client: relayTestClient(receiver), MaxPushBytes: 4096}
			pack := io.NopCloser(bytes.NewReader(relayEmptyPack()))
			_, err := relay.receivePack(t.Context(), "https://example.com", "test-token", testUpdate(t, pack))
			if delay := serverRetryAfter(err); delay != tc.delay {
				t.Errorf("HTTP %d Retry-After %q: scheduler delay=%s, want %s; error=%v", tc.status, tc.header, delay, tc.delay, err)
			}
		}
	})
}

func TestReceivePackCancellationStopsBlockedTransfer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pack := newBlockedPack()
		headersReceived := make(chan struct{})
		receiver := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			update := packp.NewReferenceUpdateRequest()
			if err := update.Decode(r.Body); err != nil {
				t.Error(err)
				return
			}
			close(headersReceived)
			if _, err := io.Copy(io.Discard, update.Packfile); err != nil {
				return
			}
		}))
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		relay := Relay{Client: relayTestClient(receiver), MaxPushBytes: 4096}
		update := testUpdate(t, pack)
		done := make(chan error, 1)
		go func() {
			_, err := relay.receivePack(ctx, "https://example.com", "test-token", update)
			done <- err
		}()
		defer func() { _ = pack.Close() }()
		for _, started := range []<-chan struct{}{pack.started, headersReceived} {
			select {
			case <-started:
			case err := <-done:
				t.Fatalf("transfer ended before cancellation: %v", err)
			case <-ctx.Done():
				t.Fatal("transfer did not start")
			}
		}
		cancel()
		var err error
		select {
		case err = <-done:
		case <-time.After(5 * time.Second):
			t.Error("cancellation did not unblock the active transfer")
			if closeErr := pack.Close(); closeErr != nil {
				t.Error(closeErr)
			}
			err = <-done
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("live transfer cancellation: %v", err)
		}
		select {
		case <-pack.closed:
		default:
			t.Fatal("cancellation left the source blocked")
		}
	})
}
