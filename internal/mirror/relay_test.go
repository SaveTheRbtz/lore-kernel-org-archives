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
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/sideband"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// Both ends are real in-memory Git object stores behind smart-HTTP handlers.
// The test server uses go-git's pack encoder/decoder, without invoking Git.
type relayTestServer struct {
	t           *testing.T
	server      *httptest.Server
	sourceMu    sync.Mutex
	destMu      sync.Mutex
	source      *memory.Storage
	destination *memory.Storage
	uploads     int
	pushes      int
	haves       []plumbing.Hash
	beforePush  func()
	beforePack  func(http.ResponseWriter) error
	failSource  bool
	omitStatus  bool
	skipUpdate  bool
}

func newRelayTestServer(t *testing.T) *relayTestServer {
	t.Helper()
	s := &relayTestServer{t: t, source: memory.NewStorage(), destination: memory.NewStorage()}
	s.server = httptest.NewTestServer(t, http.HandlerFunc(s.serveHTTP))
	return s
}

func (s *relayTestServer) sync(t *testing.T, limit int64) (SyncResult, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := Relay{Client: s.server.Client(), MaxPushBytes: limit}
	result, err := r.Sync(ctx, "https://example.com/source.git", "https://example.com/destination.git", "test-token")
	// Failed HTTP calls can return before their server handlers unwind.
	s.sourceMu.Lock()
	defer s.sourceMu.Unlock()
	s.destMu.Lock()
	defer s.destMu.Unlock()
	return result, err
}

func (s *relayTestServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if err := s.handleHTTP(w, r); err != nil && w.Header().Get("Content-Type") == "" {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

func (s *relayTestServer) handleHTTP(w http.ResponseWriter, r *http.Request) error {
	var store *memory.Storage
	var base string
	if strings.HasPrefix(r.URL.Path, "/source.git/") {
		s.sourceMu.Lock()
		defer s.sourceMu.Unlock()
		store, base = s.source, "/source.git"
		if r.Header.Get("Authorization") != "" {
			s.t.Error("destination credentials sent to source")
		}
	} else {
		s.destMu.Lock()
		defer s.destMu.Unlock()
		store, base = s.destination, "/destination.git"
		user, pass, ok := r.BasicAuth()
		if !ok || user != "x-access-token" || pass != "test-token" {
			http.Error(w, "missing authentication", http.StatusUnauthorized)
			return nil
		}
	}
	ep, err := transport.NewEndpoint("https://example.com" + base)
	if err != nil {
		return err
	}
	service := server.NewServer(server.MapLoader{ep.String(): store})
	if strings.HasSuffix(r.URL.Path, "/info/refs") {
		name := r.URL.Query().Get("service")
		var adv *packp.AdvRefs
		if name == "git-upload-pack" {
			session, sessionErr := service.NewUploadPackSession(ep, nil)
			if sessionErr != nil {
				return sessionErr
			}
			adv, err = session.AdvertisedReferencesContext(r.Context())
			if err == nil {
				err = adv.Capabilities.Set(capability.Sideband64k)
			}
		} else {
			session, sessionErr := service.NewReceivePackSession(ep, nil)
			if sessionErr != nil {
				return sessionErr
			}
			adv, err = session.AdvertisedReferencesContext(r.Context())
		}
		if err != nil {
			return err
		}
		adv.Prefix = [][]byte{[]byte("# service=" + name + "\n"), pktline.Flush}
		w.Header().Set("Content-Type", "application/x-"+name+"-advertisement")
		return adv.Encode(w)
	}
	if strings.HasSuffix(r.URL.Path, "/git-upload-pack") {
		s.uploads++
		req := packp.NewUploadPackRequest()
		if err := req.Decode(r.Body); err != nil {
			return err
		}
		scan := pktline.NewScanner(r.Body)
		for scan.Scan() {
			line := strings.TrimSpace(string(scan.Bytes()))
			if hash, ok := strings.CutPrefix(line, "have "); ok {
				req.Haves = append(req.Haves, plumbing.NewHash(hash))
			}
			if line == "done" {
				break
			}
		}
		s.haves = append([]plumbing.Hash(nil), req.Haves...)
		if !req.Depth.IsZero() || len(req.Shallows) > 0 || req.Capabilities.Supports(capability.ThinPack) {
			s.t.Error("relay requested an incomplete or thin pack")
		}
		useSideband := req.Capabilities.Supports(capability.Sideband64k)
		req.Capabilities.Delete(capability.Sideband64k)
		up, err := service.NewUploadPackSession(ep, nil)
		if err != nil {
			return err
		}
		response, err := up.UploadPack(r.Context(), req)
		if err != nil {
			return err
		}
		defer func() {
			if err := response.Close(); err != nil {
				s.t.Error(err)
			}
		}()
		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		if err := pktline.NewEncoder(w).EncodeString("NAK\n"); err != nil {
			return err
		}
		if useSideband {
			mux := sideband.NewMuxer(sideband.Sideband64k, w)
			if _, err := mux.WriteChannel(sideband.ProgressMessage, []byte("generating pack\n")); err != nil {
				return err
			}
			if s.beforePack != nil {
				if err := s.beforePack(w); err != nil {
					return err
				}
			}
			if s.failSource {
				_, err = mux.WriteChannel(sideband.ErrorMessage, []byte("injected source failure"))
			} else {
				_, err = io.Copy(mux, response)
			}
			if err != nil {
				return err
			}
			return pktline.NewEncoder(w).Flush()
		}
		_, err = io.Copy(w, response)
		return err
	}
	if strings.HasSuffix(r.URL.Path, "/git-receive-pack") {
		s.pushes++
		req := packp.NewReferenceUpdateRequest()
		if err := req.Decode(r.Body); err != nil {
			return err
		}
		if s.beforePush != nil {
			s.beforePush()
			s.beforePush = nil
		}
		// The in-memory go-git server does not enforce expected old OIDs; the
		// HTTP fixture adds the receive-pack CAS rule explicitly.
		for _, command := range req.Commands {
			ref, err := store.Reference(command.Name)
			var old plumbing.Hash
			if err == nil {
				old = ref.Hash()
			}
			if old != command.Old {
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					return err
				}
				w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
				return (&packp.ReportStatus{UnpackStatus: "ok", CommandStatuses: []*packp.CommandStatus{{ReferenceName: command.Name, Status: "stale old object ID"}}}).Encode(w)
			}
		}
		allDelete := true
		for _, command := range req.Commands {
			allDelete = allDelete && command.Action() == packp.Delete
		}
		if allDelete {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				return err
			}
			if len(data) != 0 {
				s.t.Error("delete-only request included pack data")
			}
			req.Packfile = nil
		}
		down, err := service.NewReceivePackSession(ep, nil)
		if err != nil {
			return err
		}
		var report *packp.ReportStatus
		if s.skipUpdate {
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				return err
			}
			report = &packp.ReportStatus{UnpackStatus: "ok"}
			for _, command := range req.Commands {
				report.CommandStatuses = append(report.CommandStatuses, &packp.CommandStatus{ReferenceName: command.Name, Status: "ok"})
			}
		} else {
			report, err = down.ReceivePack(r.Context(), req)
			if err != nil {
				return err
			}
		}
		if report == nil {
			return errors.New("missing report")
		}
		if s.omitStatus && len(report.CommandStatuses) > 0 {
			report.CommandStatuses = report.CommandStatuses[:len(report.CommandStatuses)-1]
		}
		w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
		return report.Encode(w)
	}
	http.NotFound(w, r)
	return nil
}

func relayTestCommit(t *testing.T, store *memory.Storage, content string, parents ...plumbing.Hash) plumbing.Hash {
	t.Helper()
	blob := store.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	w, err := blob.Writer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(w, content); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	blobHash, err := store.SetEncodedObject(blob)
	if err != nil {
		t.Fatal(err)
	}
	tree := &object.Tree{Entries: []object.TreeEntry{{Name: "m", Mode: filemode.Regular, Hash: blobHash}}}
	treeObject := store.NewEncodedObject()
	if err := tree.Encode(treeObject); err != nil {
		t.Fatal(err)
	}
	treeHash, err := store.SetEncodedObject(treeObject)
	if err != nil {
		t.Fatal(err)
	}
	signature := object.Signature{Name: "test", Email: "test@example.org", When: time.Unix(1, 0)}
	commit := &object.Commit{Author: signature, Committer: signature, Message: content, TreeHash: treeHash, ParentHashes: parents}
	encoded := store.NewEncodedObject()
	if err := commit.Encode(encoded); err != nil {
		t.Fatal(err)
	}
	hash, err := store.SetEncodedObject(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func relayTestSetRef(t *testing.T, store storer.ReferenceStorer, name string, hash plumbing.Hash) {
	t.Helper()
	if err := store.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(name), hash)); err != nil {
		t.Fatal(err)
	}
}

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
		r := Relay{Client: s.server.Client(), health: health}
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
		http.Redirect(w, r, "https://target.example/repo", http.StatusTemporaryRedirect)
	}))
	client := (&Relay{Client: origin.Client()}).httpClient()
	request, err := http.NewRequest(http.MethodGet, "https://origin.example/repo", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.SetBasicAuth("x-access-token", "secret")
	if _, err := client.Do(request); err == nil || hit.Load() {
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
	prefixReceived := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	if _, err := relayReceivePack(ctx, receiver.Client(), "https://example.com", "test-token", testUpdate(t, packReader), 4096, nil, nil); err != nil {
		t.Fatal(err)
	}
	<-producerDone
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
	pack := newBlockedPack()
	receiver := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-pack.started
		w.Header().Set("Connection", "close")
		http.Error(w, "rejected test-token", http.StatusForbidden)
		w.(http.Flusher).Flush()
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := relayReceivePack(ctx, receiver.Client(), "https://example.com", "test-token", testUpdate(t, pack), 4096, nil, nil)
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
			pack := io.NopCloser(bytes.NewReader(relayEmptyPack()))
			_, err := relayReceivePack(t.Context(), receiver.Client(), "https://example.com", "test-token", testUpdate(t, pack), 4096, nil, nil)
			if delay := serverRetryAfter(err); delay != tc.delay {
				t.Errorf("HTTP %d Retry-After %q: scheduler delay=%s, want %s; error=%v", tc.status, tc.header, delay, tc.delay, err)
			}
		}
	})
}

func TestReceivePackCancellationStopsBlockedTransfer(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	update := testUpdate(t, pack)
	done := make(chan error, 1)
	go func() {
		_, err := relayReceivePack(ctx, receiver.Client(), "https://example.com", "test-token", update, 4096, nil, nil)
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
}
