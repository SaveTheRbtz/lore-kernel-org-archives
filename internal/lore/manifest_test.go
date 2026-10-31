package lore

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
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
	w, err := gzip.NewWriterLevel(&b, gzip.BestSpeed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(data)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func fetchResponse(t *testing.T, status int, body []byte, log *slog.Logger, lists string) (Manifest, error) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET lore.kernel.org/manifest.js.gz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
	server := httptest.NewTestServer(t, mux)
	names, err := ParseLists(lists)
	if err != nil {
		t.Fatal(err)
	}
	return FetchManifest(t.Context(), server.Client(), log, names, true)
}

func TestFetchManifest(t *testing.T) {
	input := fmt.Sprintf(`{
		"/git/git/0.git":{"fingerprint":%[1]q,"modified":1,"description":"Git development [epoch 0]  ","owner":null,"forkgroup":"ignored"},
		"/git/git/1.git":{"fingerprint":%[1]q,"modified":123,"description":"Git development [epoch 19]"},
		"/linux-alpha/git/0.git":{"fingerprint":%[1]q,"modified":42,"description":"Alpha arch development list [epoch 0]"},
		"/legacy":{"modified":"unused"}
	}`, manifestTestFingerprint)
	var logs bytes.Buffer
	manifest, err := fetchResponse(t, http.StatusOK, gzipManifest(t, input), slog.New(slog.NewTextHandler(&logs, nil)), "git,legacy,linux-alpha")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "level=WARN msg=unsharded_archive_skipped source=/legacy") {
		t.Fatalf("missing warning for skipped archive: %s", &logs)
	}
	for path, entry := range manifest {
		entry.Description = entry.Summary(path, "list@example.org")
		manifest[path] = entry
	}
	want := Manifest{
		"/git/git/0.git":         {Fingerprint: manifestTestFingerprint, Modified: 1, Description: "Git development <list@example.org> (1 out of 2)", Epochs: 2},
		"/git/git/1.git":         {Fingerprint: manifestTestFingerprint, Modified: 123, Description: "Git development [epoch 19] <list@example.org> (2 out of 2)", Epochs: 2},
		"/linux-alpha/git/0.git": {Fingerprint: manifestTestFingerprint, Modified: 42, Description: "Alpha arch development list <list@example.org> (1 out of 1)", Epochs: 1},
	}
	if !maps.Equal(manifest, want) {
		t.Fatalf("manifest = %#v, want %#v", manifest, want)
	}
}

func TestManualRepositoryIsReservedRegardlessOfSourceCase(t *testing.T) {
	for _, list := range []string{"lkml", "LKML"} {
		t.Run(list, func(t *testing.T) {
			entry := fmt.Sprintf(`{"fingerprint":%q,"modified":0}`, manifestTestFingerprint)
			input := fmt.Sprintf(`{"/%[1]s/git/1.git":%[2]s,"/%[1]s/git/2.git":%[2]s}`, list, entry)
			got, err := fetchResponse(t, http.StatusOK, gzipManifest(t, input), slog.Default(), list)
			if err != nil {
				t.Fatal(err)
			}
			want := Manifest{"/" + list + "/git/2.git": {Fingerprint: manifestTestFingerprint, Epochs: 2}}
			if !maps.Equal(got, want) {
				t.Fatalf("manifest = %#v, want %#v", got, want)
			}
		})
	}
}

func TestInvalidInventoryNeverReturnsPartialManifest(t *testing.T) {
	valid := fmt.Sprintf(`{"fingerprint":%q,"modified":123}`, manifestTestFingerprint)
	tests := map[string]string{
		"empty":              `{}`,
		"null":               `null`,
		"null entry":         `{"/list/git/0.git":null}`,
		"trailing":           `{"/list/git/0.git":` + valid + `} {}`,
		"duplicate":          `{"/list/git/0.git":` + valid + `,"/list/git/0.git":` + valid + `}`,
		"name collision":     `{"/list/git/0.git":` + valid + `,"/List/git/0.git":` + valid + `}`,
		"missing digest":     `{"/list/git/0.git":{"modified":123}}`,
		"bad digest length":  `{"/list/git/0.git":{"fingerprint":"0123456789abcdef","modified":123}}`,
		"bad digest content": `{"/list/git/0.git":{"fingerprint":"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz","modified":123}}`,
		"missing modified":   fmt.Sprintf(`{"/list/git/0.git":{"fingerprint":%q}}`, manifestTestFingerprint),
		"negative modified":  fmt.Sprintf(`{"/list/git/0.git":{"fingerprint":%q,"modified":-1}}`, manifestTestFingerprint),
		"invalid list name":  `{"/.hidden":` + valid + `}`,
		"invalid path":       `{"/../list":` + valid + `}`,
		"one invalid entry":  `{"/good/git/0.git":` + valid + `,"/bad/git/0.git":{}}`,
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			manifest, err := fetchResponse(t, http.StatusOK, gzipManifest(t, input), slog.Default(), "")
			if err == nil || manifest != nil {
				t.Fatalf("expected rejected whole inventory; got %#v, %v", manifest, err)
			}
		})
	}
}

func TestFetchManifestRejectsHTTPFailureAndTruncation(t *testing.T) {
	valid := gzipManifest(t, fmt.Sprintf(`{"/list/git/0.git":{"fingerprint":%q,"modified":1}}`, manifestTestFingerprint))
	corrupted := bytes.Clone(valid)
	corrupted[len(corrupted)-8] ^= 1 // Corrupt the gzip CRC without changing the inventory.
	for _, test := range []struct {
		name   string
		status int
		body   []byte
	}{
		{"server error with valid inventory", http.StatusServiceUnavailable, valid},
		{"valid inventory without gzip trailer", http.StatusOK, valid[:len(valid)-8]},
		{"valid inventory with bad gzip checksum", http.StatusOK, corrupted},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, err := fetchResponse(t, test.status, test.body, slog.Default(), ""); err == nil || got != nil {
				t.Fatalf("failed response accepted: %v, %v", got, err)
			}
		})
	}
}

func TestFetchManifestBoundsValidPayloads(t *testing.T) {
	// Incompressible text exceeds the wire limit while staying below the expanded
	// limit. Both payloads are valid inventories if their bound is removed.
	random := make([]byte, 9<<20)
	_, _ = rand.NewChaCha8([32]byte{}).Read(random)
	for name, description := range map[string]string{
		"compressed": base64.StdEncoding.EncodeToString(random),
		"expanded":   strings.Repeat("x", 32<<20),
	} {
		t.Run(name, func(t *testing.T) {
			body := gzipManifest(t, fmt.Sprintf(`{"/list/git/0.git":{"fingerprint":%q,"modified":1,"description":%q}}`, manifestTestFingerprint, description))
			if got, err := fetchResponse(t, http.StatusOK, body, slog.Default(), ""); err == nil || got != nil {
				t.Fatalf("oversized inventory accepted: entries=%d, err=%v", len(got), err)
			}
		})
	}
}

func TestFetchManifestCancelsActiveRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server := httptest.NewTestServer(t, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		cancel()
		<-r.Context().Done()
	}))
	client := server.Client()
	client.Timeout = 5 * time.Second
	_, err := FetchManifest(ctx, client, slog.Default(), nil, true)
	if ctx.Err() == nil {
		t.Fatalf("request returned before reaching the server: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request returned %v", err)
	}
}

func TestRepositoryName(t *testing.T) {
	valid := map[string]string{
		"/lkml/git/20.git":       "lkml.20",
		"/netdev/git/0.git":      "netdev.0",
		"/linux-mm/git/2.git":    "linux-mm.2",
		"/List.Name_1/git/2.git": "list.name_1.2",
	}
	for path, want := range valid {
		got, err := RepositoryName(path)
		if err != nil || got != want {
			t.Errorf("RepositoryName(%q) = %q, %v; want %q", path, got, err, want)
		}
	}
	invalid := []string{"", "/legacy", "lkml/git/0.git", "//evil/list", "/../list", "/list/../other", "/list%2fgit%2f0.git", "/list?x=1", "/list#fragment", "/list\\git\\0.git", "/list/git/-1.git", "/list/git/18446744073709551615.git", "/list/git/01.git", "/list/git/0", "/list/git/0.git/", "/list/other/0.git", "/.hidden", "/" + strings.Repeat("x", 100) + "/git/0.git"}
	for _, path := range invalid {
		if got, err := RepositoryName(path); err == nil {
			t.Errorf("RepositoryName(%q) accepted: %q", path, got)
		}
	}
}
