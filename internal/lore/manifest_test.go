package lore

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const manifestTestFingerprint = "0123456789abcdef0123456789abcdef01234567"

func gzipManifest(t *testing.T, data string) []byte {
	t.Helper()
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write([]byte(data)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func fetchResponse(t *testing.T, status int, body []byte) (Manifest, error) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /manifest.js.gz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	return FetchManifest(t.Context(), server.Client(), server.URL+"/")
}

func TestFetchManifest(t *testing.T) {
	input := fmt.Sprintf(`{"/lkml/git/20.git":{"fingerprint":%q,"modified":123,"description":"Linux kernel","owner":null,"forkgroup":"ignored"},"/legacy":{"fingerprint":%q,"modified":0}}`, manifestTestFingerprint, manifestTestFingerprint)
	manifest, err := fetchResponse(t, http.StatusOK, gzipManifest(t, input))
	if err != nil {
		t.Fatal(err)
	}
	want := Manifest{
		"/lkml/git/20.git": {Fingerprint: manifestTestFingerprint, Modified: 123, Description: "Linux kernel"},
		"/legacy":          {Fingerprint: manifestTestFingerprint},
	}
	if !maps.Equal(manifest, want) {
		t.Fatalf("manifest = %#v, want %#v", manifest, want)
	}
}

func TestInvalidInventoryNeverReturnsPartialManifest(t *testing.T) {
	valid := fmt.Sprintf(`{"fingerprint":%q,"modified":123}`, manifestTestFingerprint)
	tests := map[string]string{
		"empty":              `{}`,
		"trailing":           `{"/list":` + valid + `} {}`,
		"duplicate":          `{"/list":` + valid + `,"/list":` + valid + `}`,
		"missing digest":     `{"/list":{"modified":123}}`,
		"bad digest length":  `{"/list":{"fingerprint":"0123456789abcdef","modified":123}}`,
		"bad digest content": `{"/list":{"fingerprint":"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz","modified":123}}`,
		"missing modified":   fmt.Sprintf(`{"/list":{"fingerprint":%q}}`, manifestTestFingerprint),
		"negative modified":  fmt.Sprintf(`{"/list":{"fingerprint":%q,"modified":-1}}`, manifestTestFingerprint),
		"invalid path":       `{"/../list":` + valid + `}`,
		"one invalid entry":  `{"/good":` + valid + `,"/bad":{}}`,
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			manifest, err := decodeManifest([]byte(input))
			if err == nil || manifest != nil {
				t.Fatalf("expected rejected whole inventory; got %#v, %v", manifest, err)
			}
		})
	}
}

func TestFetchManifestRejectsHTTPFailureAndTruncation(t *testing.T) {
	valid := gzipManifest(t, fmt.Sprintf(`{"/list":{"fingerprint":%q,"modified":1}}`, manifestTestFingerprint))
	for _, test := range []struct {
		name   string
		status int
		body   []byte
	}{
		{"server error with valid inventory", http.StatusServiceUnavailable, valid},
		{"valid inventory without gzip trailer", http.StatusOK, valid[:len(valid)-8]},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, err := fetchResponse(t, test.status, test.body); err == nil || got != nil {
				t.Fatalf("failed response accepted: %v, %v", got, err)
			}
		})
	}
}

func TestFetchManifestBoundsValidPayloads(t *testing.T) {
	// Incompressible text exceeds the wire limit while staying below the expanded
	// limit. Both payloads are valid inventories if their bound is removed.
	random := make([]byte, 9<<20)
	_, _ = rand.New(rand.NewSource(1)).Read(random)
	for name, description := range map[string]string{
		"compressed": base64.StdEncoding.EncodeToString(random),
		"expanded":   strings.Repeat("x", maxManifestExpanded),
	} {
		t.Run(name, func(t *testing.T) {
			body := gzipManifest(t, fmt.Sprintf(`{"/list":{"fingerprint":%q,"modified":1,"description":%q}}`, manifestTestFingerprint, description))
			if got, err := fetchResponse(t, http.StatusOK, body); err == nil || got != nil {
				t.Fatalf("oversized inventory accepted: entries=%d, err=%v", len(got), err)
			}
		})
	}
}

func TestFetchManifestCancelsActiveRequest(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	client := server.Client()
	client.Timeout = 5 * time.Second
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := FetchManifest(ctx, client, server.URL); done <- err }()
	select {
	case <-started:
		cancel()
	case err := <-done:
		t.Fatalf("request failed before cancellation: %v", err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request returned %v", err)
	}
}

func TestRepositoryName(t *testing.T) {
	valid := map[string]string{
		"/lkml/git/20.git":       "lkml.20",
		"/netdev/git/0.git":      "netdev.0",
		"/linux-mm/git/2.git":    "linux-mm.2",
		"/List.Name_1/git/2.git": "list.name_1.2",
		"/legacy":                "legacy.v1",
	}
	for path, want := range valid {
		got, err := RepositoryName(path)
		if err != nil || got != want {
			t.Errorf("RepositoryName(%q) = %q, %v; want %q", path, got, err, want)
		}
	}
	invalid := []string{"", "lkml/git/0.git", "//evil/list", "/../list", "/list/../other", "/list%2fgit%2f0.git", "/list?x=1", "/list#fragment", "/list\\git\\0.git", "/list/git/-1.git", "/list/git/01.git", "/list/git/0", "/list/git/0.git/", "/list/other/0.git", "/.hidden", "/" + strings.Repeat("x", 100)}
	for _, path := range invalid {
		if got, err := RepositoryName(path); err == nil {
			t.Errorf("RepositoryName(%q) accepted: %q", path, got)
		}
	}
}

func TestSourceRepositoryURL(t *testing.T) {
	got, err := SourceRepositoryURL("/lkml/git/0.git")
	if err != nil || got != "https://lore.kernel.org/lkml/git/0.git" {
		t.Fatalf("unexpected URL: %q, %v", got, err)
	}
	if _, err := SourceRepositoryURL("//evil.example/list"); err == nil {
		t.Error("accepted host-replacing repository path")
	}
}
