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
	"lore-mirror/internal/retryafter"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/sideband"
)

func TestRelayInitialIncrementalAndRefChanges(t *testing.T) {
	s := newRelayTestServer(t)
	s.receiveBand = true
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
	if result.RefCount != 2 {
		t.Fatalf("source ref count = %d, want 2", result.RefCount)
	}
	if result.DefaultBranch != "master" {
		t.Fatalf("default branch = %q, want master", result.DefaultBranch)
	}
	for _, name := range []plumbing.ReferenceName{plumbing.Master, "refs/meta/origins"} {
		if ref, err := s.destination.Reference(name); err != nil || ref.Hash() != first {
			t.Fatalf("destination %s = %v, error %v; want %s", name, ref, err, first)
		}
	}
	if _, err := s.destination.EncodedObject(plumbing.CommitObject, first); err != nil {
		t.Fatalf("destination lacks commit: %v", err)
	}
	s.pushes, s.uploads = 0, 0
	result, err = s.sync(t, 0)
	if err != nil || result.Changed || s.pushes != 0 || s.uploads != 0 {
		t.Fatalf("unchanged sync did work: %+v %v pushes=%d uploads=%d", result, err, s.pushes, s.uploads)
	}
	second := relayTestCommit(t, s.source, "second mail", first)
	relayTestSetRef(t, s.source, "refs/heads/master", second)
	if _, err := s.sync(t, 0); err != nil {
		t.Fatal(err)
	}
	if ref, err := s.destination.Reference(plumbing.Master); err != nil || ref.Hash() != second {
		t.Fatalf("incremental destination ref: %v, error %v; want %s", ref, err, second)
	}
	if len(s.haves) != 1 || s.haves[0] != first {
		t.Fatalf("destination tips not used as haves: %v", s.haves)
	}
	s.uploads = 0
	relayTestSetRef(t, s.source, "refs/tags/existing", second)
	if _, err := s.sync(t, 0); err != nil {
		t.Fatal(err)
	}
	if ref, err := s.destination.Reference("refs/tags/existing"); err != nil || ref.Hash() != second {
		t.Fatalf("ref-only addition: %v, error %v; want %s", ref, err, second)
	}
	// Deleted source metadata is removed; GitHub's own refs remain untouched.
	if err := s.source.RemoveReference("refs/meta/origins"); err != nil {
		t.Fatal(err)
	}
	relayTestSetRef(t, s.destination, "refs/pull/1/head", first)
	if _, err := s.sync(t, 0); err != nil {
		t.Fatalf("delete-only sync: %v", err)
	}
	if _, err := s.destination.Reference("refs/meta/origins"); err != plumbing.ErrReferenceNotFound {
		t.Fatal("removed source ref retained at destination")
	}
	if ref, err := s.destination.Reference("refs/pull/1/head"); err != nil || ref.Hash() != first {
		t.Fatalf("GitHub-managed ref changed: %v, error %v; want %s", ref, err, first)
	}
	// Rewinds are authoritative and can reuse objects already at the target.
	relayTestSetRef(t, s.source, "refs/heads/master", first)
	if _, err := s.sync(t, 0); err != nil {
		t.Fatalf("source correction: %v", err)
	}
	if ref, err := s.destination.Reference(plumbing.Master); err != nil || ref.Hash() != first {
		t.Fatalf("rewound destination ref: %v, error %v; want %s", ref, err, first)
	}
	if s.uploads != 0 {
		t.Fatalf("ref-only changes fetched known objects %d times", s.uploads)
	}
}

func TestRelayStagesDefaultBranchRename(t *testing.T) {
	s := newRelayTestServer(t)
	old := relayTestCommit(t, s.destination, "old")
	relayTestSetRef(t, s.destination, "refs/heads/master", old)
	relayTestSetRef(t, s.destination, "refs/tags/obsolete", old)
	tip := relayTestCommit(t, s.source, "new", relayTestCommit(t, s.source, "old"))
	relayTestSetRef(t, s.source, string(plumbing.Main), tip)
	if err := s.source.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.Main)); err != nil {
		t.Fatal(err)
	}

	switchFailed := errors.New("default branch change failed")
	switches := 0
	s.setDefault = func(branch string) error {
		switches++
		if branch != "main" {
			t.Fatalf("switch to %q, want main", branch)
		}
		// GitHub can select the new branch now, and no deletions have happened.
		for ref, want := range map[plumbing.ReferenceName]plumbing.Hash{plumbing.Main: tip, plumbing.Master: old, "refs/tags/obsolete": old} {
			if got, err := s.destination.Reference(ref); err != nil || got.Hash() != want {
				t.Fatalf("before switch: %s = %v, %v; want %s", ref, got, err, want)
			}
		}
		if switches == 1 {
			return switchFailed
		}
		return nil
	}
	staged, err := s.sync(t, 0)
	if !errors.Is(err, switchFailed) || !staged.Changed || staged.Bytes == 0 {
		t.Fatalf("first sync = %+v, %v; want staged push and default branch change failure", staged, err)
	}
	if _, err := s.sync(t, 0); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if switches != 2 || s.pushes != 2 || s.uploads != 1 {
		t.Fatalf("got %d switches, %d pushes, %d uploads; want two switch attempts, one staging push and one cleanup push", switches, s.pushes, s.uploads)
	}
	for _, ref := range []plumbing.ReferenceName{plumbing.Master, "refs/tags/obsolete"} {
		if _, err := s.destination.Reference(ref); err != plumbing.ErrReferenceNotFound {
			t.Fatalf("after switch: %s lookup = %v, want absent", ref, err)
		}
	}
	if got, err := s.destination.Reference(plumbing.Main); err != nil || got.Hash() != tip {
		t.Fatalf("after switch: main = %v, %v; want %s", got, err, tip)
	}
}

func TestRelayLimitsWholeRequestsAndRetriesWithoutPublishingPartialRefs(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ref         plumbing.ReferenceName
		limit       int64
		initialSync bool
	}{
		{"pack truncated", plumbing.Master, 180, false},
		{"command overhead", "refs/tags/existing", 32, true}, // The empty pack fits; update commands do not.
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newRelayTestServer(t)
			hash := relayTestCommit(t, s.source, "mail")
			relayTestSetRef(t, s.source, "refs/heads/master", hash)
			if tc.initialSync {
				if _, err := s.sync(t, 0); err != nil {
					t.Fatal(err)
				}
			}
			relayTestSetRef(t, s.source, string(tc.ref), hash)
			failed, err := s.sync(t, tc.limit)
			if !errors.Is(err, ErrPushTooLarge) || failed.Bytes > tc.limit {
				t.Fatalf("bounded request: %+v %v", failed, err)
			}
			if _, err := s.destination.Reference(tc.ref); !errors.Is(err, plumbing.ErrReferenceNotFound) {
				t.Fatalf("partial request published %s: %v", tc.ref, err)
			}
			if _, err := s.sync(t, 0); err != nil {
				t.Fatalf("retry: %v", err)
			}
			ref, err := s.destination.Reference(tc.ref)
			if err != nil || ref.Hash() != hash {
				t.Fatalf("retry did not publish %s: %v", tc.ref, err)
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
		if err != nil || ref.Hash() != other {
			t.Fatalf("concurrent ref overwritten: %v, error %v; want %s", ref, err, other)
		}
	})
}

func TestRelayRequiresCompleteStatusAndReadBack(t *testing.T) {
	for _, tc := range []struct {
		name                                     string
		omitStatus, skipUpdate, shallowAfterPush bool
	}{
		{name: "missing status", omitStatus: true},
		{name: "false success", skipUpdate: true},
		{name: "shallow readback", shallowAfterPush: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newRelayTestServer(t)
			hash := relayTestCommit(t, s.source, "mail")
			relayTestSetRef(t, s.source, "refs/heads/master", hash)
			s.omitStatus = tc.omitStatus
			s.skipUpdate = tc.skipUpdate
			s.shallowAfterPush = tc.shallowAfterPush
			if _, err := s.sync(t, 0); err == nil {
				t.Fatal("invalid receiver success accepted")
			}
		})
	}
}

func TestRelayPreservesThrottlingThroughGoGit(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		status     int
	}{
		{"advertisement", "/source.git/info/refs", 403},
		{"upload-pack", "/source.git/git-upload-pack", 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newRelayTestServer(t)
			hash := relayTestCommit(t, s.source, "mail")
			relayTestSetRef(t, s.source, "refs/heads/master", hash)
			s.beforeHTTP = func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != tc.path {
					return false
				}
				w.Header().Set("Retry-After", "42")
				http.Error(w, "throttled test-token", tc.status)
				return true
			}
			_, err := s.sync(t, 0)
			if delay := retryafter.FromError(err); delay != 42*time.Second {
				t.Fatalf("scheduler delay = %s, want 42s; error=%v", delay, err)
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
		s.beforePack = func(w http.ResponseWriter) error {
			mux := sideband.NewMuxer(sideband.Sideband64k, w)
			for range 6 {
				time.Sleep(time.Minute)
				if _, err := mux.WriteChannel(sideband.ProgressMessage, []byte("preparing pack\n")); err != nil {
					return err
				}
				w.(http.Flusher).Flush()
			}
			<-ctx.Done() // No archive bytes are sent during this preparation.
			return ctx.Err()
		}
		health := &observe.Health{}
		finish := health.Begin(observe.Sync)
		defer finish()
		r := Relay{Client: s.client, MaxPushBytes: DefaultMaxPushBytes, health: health}
		var syncErr error
		go func() {
			_, syncErr = r.Sync(ctx, "https://example.com/source.git", "https://example.com/destination.git", "test-token", "master", s.setDefault)
		}()
		synctest.Wait()
		time.Sleep(6 * time.Minute)
		synctest.Wait()
		if health.Stuck() {
			t.Error("sync marked stuck while receiving source progress")
		}
		cancel()
		synctest.Wait()
		if !errors.Is(syncErr, context.Canceled) {
			t.Fatalf("cancel stalled preparation: %v", syncErr)
		}
	})
}

func TestGitHTTPClientUsesDefaultTransport(t *testing.T) {
	base := &http.Client{}
	git := newGitHTTPClient(base)
	if got := git.Transport.(gitTransport).RoundTripper; got != http.DefaultTransport || base.Transport != nil {
		t.Fatalf("wrapped transport %T, original transport %T", got, base.Transport)
	}
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
		go func() {
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
	const leaked = "test-token eC1hY2Nlc3MtdG9rZW46dGVzdC10b2tlbg=="
	for _, test := range []struct {
		name     string
		sideband bool
		reply    func(http.ResponseWriter) error
		want     []string
	}{
		{
			name: "HTTP rejection", want: []string{"403", "request_id=failed-push-id"},
			reply: func(w http.ResponseWriter) error {
				http.Error(w, "rejected "+leaked, http.StatusForbidden)
				return nil
			},
		},
		{
			name: "remote fatal error", sideband: true, want: []string{"invalid Git object [redacted]"},
			reply: func(w http.ResponseWriter) error {
				_, err := sideband.NewMuxer(sideband.Sideband64k, w).WriteChannel(sideband.ErrorMessage, []byte("invalid Git object "+leaked))
				return err
			},
		},
		{
			name: "unpack rejection with remote context", sideband: true,
			want: []string{"unpack error: rejected [redacted]", "remote: detailed receiver failure [redacted]"},
			reply: func(w http.ResponseWriter) error {
				mux := sideband.NewMuxer(sideband.Sideband64k, w)
				for _, chunk := range []string{"detailed receiver failure test-", "token eC1hY2Nlc3MtdG9rZW46dGVzdC10b2tlbg==\n"} {
					if _, err := mux.WriteChannel(sideband.ProgressMessage, []byte(chunk)); err != nil {
						return err
					}
				}
				return (&packp.ReportStatus{UnpackStatus: "rejected " + leaked}).Encode(mux)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pack := newBlockedPack()
				receiver := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					<-pack.started
					w.Header().Set("Connection", "close")
					w.Header().Set("X-GitHub-Request-Id", "failed-push-id")
					w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
					if err := test.reply(w); err != nil {
						t.Error(err)
					}
					w.(http.Flusher).Flush()
				}))
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				relay := Relay{Client: relayTestClient(receiver), MaxPushBytes: 4096}
				update := testUpdate(t, pack)
				if test.sideband {
					_ = update.Capabilities.Set(capability.Sideband64k)
				}
				_, err := relay.receivePack(ctx, "https://example.com", "test-token", update)
				if err == nil || strings.Contains(err.Error(), "test-token") || strings.Contains(err.Error(), "eC1hY2Nlc3MtdG9rZW46dGVzdC10b2tlbg==") {
					t.Fatalf("missing or unsafe error: %v", err)
				}
				for _, want := range test.want {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not include %q", err, want)
					}
				}
				if ctx.Err() != nil || errors.Is(err, context.Canceled) {
					t.Fatalf("early rejection classified as cancellation: %v (context: %v)", err, ctx.Err())
				}
			})
		})
	}
}

type countedPack struct {
	io.Reader
	closes atomic.Int32
}

func (p *countedPack) Close() error { p.closes.Add(1); return nil }

func TestReceivePackOwnsSource(t *testing.T) {
	receiver := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
		report := packp.ReportStatus{UnpackStatus: "ok", CommandStatuses: []*packp.CommandStatus{{ReferenceName: plumbing.Master, Status: "ok"}}}
		if err := report.Encode(w); err != nil {
			t.Error(err)
		}
	}))
	relay := Relay{Client: relayTestClient(receiver), MaxPushBytes: 4096}
	for _, destination := range []string{"https://example.com", "://invalid"} {
		pack := &countedPack{Reader: bytes.NewReader(relayEmptyPack())}
		_, err := relay.receivePack(t.Context(), destination, "token", testUpdate(t, pack))
		if (err == nil) != (destination == "https://example.com") || pack.closes.Load() != 1 {
			t.Fatalf("destination %q: closes=%d, error=%v", destination, pack.closes.Load(), err)
		}
	}
}

func TestReceivePackDropsProgressCutOffByReportLimit(t *testing.T) {
	const token = "test-token"
	const prefix = "test-toke"
	var wire bytes.Buffer
	mux := sideband.NewMuxer(sideband.Sideband64k, &wire)
	// Each progress packet has a four-byte length and a one-byte channel.
	padding := bytes.Repeat([]byte{' '}, (16<<10)-5)
	for range 63 {
		if _, err := mux.WriteChannel(sideband.ProgressMessage, padding); err != nil {
			t.Fatal(err)
		}
	}
	copy(padding[len(padding)-len(prefix):], prefix)
	if _, err := mux.WriteChannel(sideband.ProgressMessage, padding); err != nil {
		t.Fatal(err)
	}
	if wire.Len() != 1<<20 {
		t.Fatalf("cutoff after %d bytes", wire.Len())
	}
	if _, err := mux.WriteChannel(sideband.ProgressMessage, []byte(token[len(prefix):])); err != nil {
		t.Fatal(err)
	}
	receiver := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
		_, _ = w.Write(wire.Bytes())
	}))
	relay := Relay{Client: relayTestClient(receiver), MaxPushBytes: 4096}
	update := testUpdate(t, nil)
	if err := update.Capabilities.Set(capability.Sideband64k); err != nil {
		t.Fatal(err)
	}
	_, err := relay.receivePack(t.Context(), "https://example.com", token, update)
	if err == nil || strings.Contains(err.Error(), prefix) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("unsafe or missing protocol error: %v", err)
	}
}

func TestReceivePackOmitsTruncatedHTTPDiagnostics(t *testing.T) {
	const token = "test-token"
	for name, credential := range map[string]string{"token": token, "basic": "eC1hY2Nlc3MtdG9rZW46dGVzdC10b2tlbg=="} {
		t.Run(name, func(t *testing.T) {
			prefix := credential[:len(credential)-1]
			receiver := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				http.Error(w, strings.Repeat(" ", 4096-len(prefix))+credential, http.StatusForbidden)
			}))
			relay := Relay{Client: relayTestClient(receiver), MaxPushBytes: 4096}
			_, err := relay.receivePack(t.Context(), "https://example.com", token, testUpdate(t, nil))
			if err == nil || !strings.Contains(err.Error(), "HTTP 403") || strings.Contains(err.Error(), prefix) {
				t.Fatalf("unsafe or missing HTTP diagnostic: %v", err)
			}
		})
	}
}

func TestReceivePackPreservesRetryAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			header string
			delay  time.Duration
		}{
			{"180", 3 * time.Minute},
			{time.Now().Add(3 * time.Minute).UTC().Format(http.TimeFormat), 3 * time.Minute},
			{"", time.Minute},
			{"invalid", time.Minute},
		} {
			receiver := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", tc.header)
				http.Error(w, "rejected", http.StatusTooManyRequests)
			}))
			relay := Relay{Client: relayTestClient(receiver), MaxPushBytes: 4096}
			pack := io.NopCloser(bytes.NewReader(relayEmptyPack()))
			_, err := relay.receivePack(t.Context(), "https://example.com", "test-token", testUpdate(t, pack))
			if delay := retryafter.FromError(err); delay != tc.delay {
				t.Errorf("Retry-After %q: scheduler delay=%s, want %s; error=%v", tc.header, delay, tc.delay, err)
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
		synctest.Wait()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("live transfer cancellation: %v", err)
			}
		default:
			t.Fatal("cancellation did not unblock the active transfer")
		}
		select {
		case <-pack.closed:
		default:
			t.Fatal("cancellation left the source blocked")
		}
	})
}
