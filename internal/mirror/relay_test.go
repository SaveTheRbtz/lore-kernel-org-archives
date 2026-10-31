package mirror

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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
)

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
	chunked     bool
	beforePush  func()
	failSource  bool
	omitStatus  bool
	skipUpdate  bool
}

func newRelayTestServer(t *testing.T) *relayTestServer {
	t.Helper()
	s := &relayTestServer{t: t, source: memory.NewStorage(), destination: memory.NewStorage()}
	s.server = httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	t.Cleanup(s.server.Close)
	return s
}

func (s *relayTestServer) sync(t *testing.T, limit int64) (SyncResult, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := Relay{Client: s.server.Client(), AllowHTTP: true, MaxPushBytes: limit}
	result, err := r.Sync(ctx, s.server.URL+"/source.git", s.server.URL+"/destination.git", "test-token")
	// Failed HTTP calls can return before their server handlers unwind.
	s.sourceMu.Lock()
	s.sourceMu.Unlock()
	s.destMu.Lock()
	s.destMu.Unlock()
	return result, err
}

func (s *relayTestServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
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
			return
		}
	}
	ep, _ := transport.NewEndpoint(s.server.URL + base)
	service := server.NewServer(server.MapLoader{ep.String(): store})
	if strings.HasSuffix(r.URL.Path, "/info/refs") {
		name := r.URL.Query().Get("service")
		var adv *packp.AdvRefs
		var err error
		if name == "git-upload-pack" {
			session, _ := service.NewUploadPackSession(ep, nil)
			adv, err = session.AdvertisedReferencesContext(r.Context())
			_ = adv.Capabilities.Set(capability.Sideband64k)
		} else {
			session, _ := service.NewReceivePackSession(ep, nil)
			adv, err = session.AdvertisedReferencesContext(r.Context())
		}
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		adv.Prefix = [][]byte{[]byte("# service=" + name + "\n"), pktline.Flush}
		w.Header().Set("Content-Type", "application/x-"+name+"-advertisement")
		if err := adv.Encode(w); err != nil {
			s.t.Error(err)
		}
		return
	}
	if strings.HasSuffix(r.URL.Path, "/git-upload-pack") {
		s.uploads++
		req := packp.NewUploadPackRequest()
		if err := req.UploadRequest.Decode(r.Body); err != nil {
			http.Error(w, err.Error(), 500)
			return
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
		up, _ := service.NewUploadPackSession(ep, nil)
		response, err := up.UploadPack(r.Context(), req)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer response.Close()
		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		_ = pktline.NewEncoder(w).EncodeString("NAK\n")
		if useSideband {
			mux := sideband.NewMuxer(sideband.Sideband64k, w)
			_, _ = mux.WriteChannel(sideband.ProgressMessage, []byte("generating pack\n"))
			if s.failSource {
				_, _ = mux.WriteChannel(sideband.ErrorMessage, []byte("injected source failure"))
			} else {
				_, _ = io.Copy(mux, response)
			}
			_ = pktline.NewEncoder(w).Flush()
		} else {
			_, _ = io.Copy(w, response)
		}
		return
	}
	if strings.HasSuffix(r.URL.Path, "/git-receive-pack") {
		s.pushes++
		s.chunked = len(r.TransferEncoding) == 1 && r.TransferEncoding[0] == "chunked"
		req := packp.NewReferenceUpdateRequest()
		if err := req.Decode(r.Body); err != nil {
			http.Error(w, err.Error(), 400)
			return
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
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
				_ = (&packp.ReportStatus{UnpackStatus: "ok", CommandStatuses: []*packp.CommandStatus{{ReferenceName: command.Name, Status: "stale old object ID"}}}).Encode(w)
				return
			}
		}
		allDelete := true
		for _, command := range req.Commands {
			allDelete = allDelete && command.Action() == packp.Delete
		}
		if allDelete {
			data, _ := io.ReadAll(r.Body)
			if len(data) != 0 {
				s.t.Error("delete-only request included pack data")
			}
			req.Packfile = nil
		}
		down, _ := service.NewReceivePackSession(ep, nil)
		var report *packp.ReportStatus
		if s.skipUpdate {
			_, _ = io.Copy(io.Discard, r.Body)
			report = &packp.ReportStatus{UnpackStatus: "ok"}
			for _, command := range req.Commands {
				report.CommandStatuses = append(report.CommandStatuses, &packp.CommandStatus{ReferenceName: command.Name, Status: "ok"})
			}
		} else {
			report, _ = down.ReceivePack(r.Context(), req)
		}
		if report == nil {
			http.Error(w, "missing report", 500)
			return
		}
		if s.omitStatus && len(report.CommandStatuses) > 0 {
			report.CommandStatuses = report.CommandStatuses[:len(report.CommandStatuses)-1]
		}
		w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
		_ = report.Encode(w)
		return
	}
	http.NotFound(w, r)
}

func relayTestCommit(t *testing.T, store *memory.Storage, content string, parents ...plumbing.Hash) plumbing.Hash {
	t.Helper()
	blob := store.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	w, _ := blob.Writer()
	_, _ = io.WriteString(w, content)
	_ = w.Close()
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
	_ = s.source.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.Master))
	result, err := s.sync(t, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Changed || result.DefaultBranch != "master" || result.Bytes == 0 || !s.chunked {
		t.Fatalf("unexpected initial result: %+v, chunked=%v", result, s.chunked)
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
	_ = s.source.RemoveReference("refs/meta/origins")
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

func TestRelayRejectsOversizedPushAndCanRetry(t *testing.T) {
	s := newRelayTestServer(t)
	hash := relayTestCommit(t, s.source, "mail")
	relayTestSetRef(t, s.source, "refs/heads/master", hash)
	result, err := s.sync(t, 180)
	if !errors.Is(err, ErrPushTooLarge) || result.Bytes > 180 {
		t.Fatalf("expected bounded overflow, got %+v %v", result, err)
	}
	if _, err := s.destination.Reference(plumbing.Master); err != plumbing.ErrReferenceNotFound {
		t.Fatal("truncated push published a ref")
	}
	if _, err := s.sync(t, 0); err != nil {
		t.Fatalf("retry after interrupted transfer: %v", err)
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
		ref, _ := s.destination.Reference(plumbing.Master)
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

func TestRelayRejectsUnsafeEndpointsAndRedirects(t *testing.T) {
	for _, endpoint := range []string{"file:///tmp/repo", "/tmp/repo", "git@example.org:repo", "http://example.org/repo", "https://user:secret@example.org/repo", "https://example.org/repo?x=1"} {
		if _, err := relayEndpoint(endpoint, false); err == nil {
			t.Errorf("accepted %q", endpoint)
		}
	}
	var hit bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit = true }))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	client := (&Relay{}).httpClient()
	request, _ := http.NewRequest(http.MethodGet, origin.URL, nil)
	request.SetBasicAuth("x-access-token", "secret")
	if _, err := client.Do(request); err == nil || hit {
		t.Fatal("credential-bearing redirect followed")
	}
}

func TestRelayPushSizeLimitIncludesCommands(t *testing.T) {
	var output bytes.Buffer
	writer := relayLimitedWriter{w: &output, limit: 3}
	if n, err := writer.Write([]byte("abcd")); n != 3 || !errors.Is(err, ErrPushTooLarge) || output.String() != "abc" {
		t.Fatal(fmt.Sprintf("size limit: n=%d err=%v data=%q", n, err, output.String()))
	}
}

func TestReceivePackStreamsBeforeReadingWholePack(t *testing.T) {
	bodyStarted := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := packp.NewReferenceUpdateRequest()
		if err := req.Decode(r.Body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		close(bodyStarted)
		pack, err := io.ReadAll(req.Packfile)
		if err != nil || !bytes.Equal(pack, relayEmptyPack()) {
			t.Errorf("received pack %x, error %v", pack, err)
		}
		w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
		_ = (&packp.ReportStatus{UnpackStatus: "ok", CommandStatuses: []*packp.CommandStatus{{ReferenceName: plumbing.Master, Status: "ok"}}}).Encode(w)
	}))
	defer receiver.Close()
	packReader, packWriter := io.Pipe()
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		select {
		case <-bodyStarted:
			_, err := packWriter.Write(relayEmptyPack())
			packWriter.CloseWithError(err)
		case <-ctx.Done():
			packWriter.CloseWithError(ctx.Err())
		}
	}()
	update := packp.NewReferenceUpdateRequest()
	_ = update.Capabilities.Set(capability.ReportStatus)
	update.Commands = []*packp.Command{{Name: plumbing.Master, New: plumbing.NewHash(strings.Repeat("1", 40))}}
	update.Packfile = packReader
	if _, err := relayReceivePack(ctx, receiver.Client(), receiver.URL, "test-token", update, 4096, nil, nil); err != nil {
		t.Fatal(err)
	}
	<-producerDone
}

func TestReceivePackEarlyRejectionClosesSourceAndRedactsToken(t *testing.T) {
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "close")
		http.Error(w, "rejected test-token", http.StatusForbidden)
		w.(http.Flusher).Flush()
	}))
	defer receiver.Close()
	packReader, packWriter := io.Pipe()
	defer packWriter.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	update := packp.NewReferenceUpdateRequest()
	_ = update.Capabilities.Set(capability.ReportStatus)
	update.Commands = []*packp.Command{{Name: plumbing.Master, New: plumbing.NewHash(strings.Repeat("1", 40))}}
	update.Packfile = packReader // Never produces data; the rejection must unblock it.
	_, err := relayReceivePack(ctx, receiver.Client(), receiver.URL, "test-token", update, 4096, nil, nil)
	if err == nil || strings.Contains(err.Error(), "test-token") || !strings.Contains(err.Error(), "403") {
		t.Fatalf("unexpected error: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("early rejection waited for the job timeout")
	}
}
