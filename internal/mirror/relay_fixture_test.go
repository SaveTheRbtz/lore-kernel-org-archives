package mirror

import (
	"context"
	"errors"
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
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// Both ends are real in-memory Git object stores behind smart-HTTP handlers.
// The test server uses go-git's pack encoder/decoder, without invoking Git.
type relayTestServer struct {
	t                *testing.T
	client           *http.Client
	sourceMu         sync.Mutex
	destMu           sync.Mutex
	source           *memory.Storage
	destination      *memory.Storage
	uploads          int
	pushes           int
	haves            []plumbing.Hash
	beforePush       func()
	beforePack       func(http.ResponseWriter) error
	beforeHTTP       func(http.ResponseWriter, *http.Request) bool
	setDefault       func(string) error
	failSource       bool
	receiveBand      bool
	omitStatus       bool
	skipUpdate       bool
	shallowAfterPush bool
}

func newRelayTestServer(t *testing.T) *relayTestServer {
	t.Helper()
	s := &relayTestServer{t: t, source: memory.NewStorage(), destination: memory.NewStorage()}
	s.setDefault = func(branch string) error { return errors.New("unexpected default branch switch to " + branch) }
	s.client = relayTestClient(httptest.NewTestServer(t, http.HandlerFunc(s.serveHTTP)))
	return s
}

func (s *relayTestServer) sync(limit int64) (SyncResult, error) {
	ctx, cancel := context.WithTimeout(s.t.Context(), 5*time.Second)
	defer cancel()
	if limit == 0 {
		limit = DefaultMaxPushBytes
	}
	r := Relay{Client: s.client, MaxPushBytes: limit}
	result, err := r.Sync(ctx, "https://example.com/source.git", "https://example.com/destination.git", "test-token", "master", s.setDefault)
	// Failed HTTP calls can return before their server handlers unwind.
	s.sourceMu.Lock()
	defer s.sourceMu.Unlock()
	s.destMu.Lock()
	defer s.destMu.Unlock()
	return result, err
}

func (s *relayTestServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if s.beforeHTTP != nil && s.beforeHTTP(w, r) {
		return
	}
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
			if err == nil && s.shallowAfterPush && s.pushes > 0 {
				adv.Shallows = []plumbing.Hash{adv.References[string(plumbing.Master)]}
			}
			if err == nil && s.receiveBand {
				err = adv.Capabilities.Set(capability.Sideband64k)
			}
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
		useSideband := req.Capabilities.Supports(capability.Sideband64k)
		if s.receiveBand != useSideband {
			return errors.New("relay did not negotiate the advertised receiver sideband")
		}
		writeReport := func(report *packp.ReportStatus) error {
			w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
			if !useSideband {
				return report.Encode(w)
			}
			mux := sideband.NewMuxer(sideband.Sideband64k, w)
			if _, err := mux.WriteChannel(sideband.ProgressMessage, []byte("checking received objects\n")); err != nil {
				return err
			}
			if err := report.Encode(mux); err != nil {
				return err
			}
			return pktline.NewEncoder(w).Flush()
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
				return writeReport(&packp.ReportStatus{UnpackStatus: "ok", CommandStatuses: []*packp.CommandStatus{{ReferenceName: command.Name, Status: "stale old object ID"}}})
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
		req.Capabilities.Delete(capability.Sideband64k)
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
		return writeReport(report)
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

func relayTestClient(server *httptest.Server) *http.Client {
	client := server.Client()
	client.CheckRedirect = rejectRedirects
	return newGitHTTPClient(client)
}
