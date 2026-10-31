package lore

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

func TestFetchManifest(t *testing.T) {
	input := fmt.Sprintf(`{"/lkml/git/20.git":{"fingerprint":%q,"modified":123,"description":"Linux kernel","owner":null,"forkgroup":"ignored"},"/legacy":{"fingerprint":%q,"modified":0}}`, manifestTestFingerprint, manifestTestFingerprint)
	compressed := gzipManifest(t, input)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/manifest.js.gz" || r.Method != http.MethodGet {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("compressed byte limit needs undecoded response")
		}
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write(compressed)
	}))
	defer srv.Close()
	manifest, err := FetchManifest(context.Background(), srv.Client(), srv.URL+"/")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest) != 2 || manifest["/lkml/git/20.git"] != (ManifestEntry{manifestTestFingerprint, 123, "Linux kernel"}) {
		t.Fatalf("unexpected manifest: %#v", manifest)
	}
}

func TestManifestRejectsInvalidInventory(t *testing.T) {
	valid := fmt.Sprintf(`{"fingerprint":%q,"modified":123}`, manifestTestFingerprint)
	tests := map[string]string{
		"empty":              `{}`,
		"null":               `null`,
		"array":              `[]`,
		"invalid json":       `{"/list":`,
		"trailing":           `{"/list":` + valid + `} {}`,
		"duplicate":          `{"/list":` + valid + `,"/list":` + valid + `}`,
		"null entry":         `{"/list":null}`,
		"missing digest":     `{"/list":{"modified":123}}`,
		"bad digest length":  `{"/list":{"fingerprint":"abc","modified":123}}`,
		"bad digest content": `{"/list":{"fingerprint":"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz","modified":123}}`,
		"missing modified":   fmt.Sprintf(`{"/list":{"fingerprint":%q}}`, manifestTestFingerprint),
		"negative modified":  fmt.Sprintf(`{"/list":{"fingerprint":%q,"modified":-1}}`, manifestTestFingerprint),
		"fraction modified":  fmt.Sprintf(`{"/list":{"fingerprint":%q,"modified":1.2}}`, manifestTestFingerprint),
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

func TestFetchManifestErrors(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   []byte
		want   string
	}{
		{"http error", http.StatusServiceUnavailable, []byte("unavailable"), "503"},
		{"html", http.StatusOK, []byte("<html>challenge</html>"), "gzip"},
		{"truncated gzip", http.StatusOK, gzipManifest(t, `{}`)[:12], "manifest"},
		{"compressed limit", http.StatusOK, bytes.Repeat([]byte{'x'}, maxManifestCompressed+1), "compressed manifest"},
		{"expanded limit", http.StatusOK, gzipManifest(t, strings.Repeat(" ", maxManifestExpanded+1)), "expand manifest"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write(tt.body)
			}))
			defer srv.Close()
			_, err := FetchManifest(context.Background(), srv.Client(), srv.URL)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
	}
}

func TestFetchManifestContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := FetchManifest(ctx, &http.Client{}, "https://lore.kernel.org")
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected cancellation, got %v", err)
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
	for _, origin := range []string{"https://erol.kernel.org", "https://erol.kernel.org/"} {
		got, err := SourceRepositoryURL(origin, "/lkml/git/0.git")
		if err != nil || got != "https://erol.kernel.org/lkml/git/0.git" {
			t.Fatalf("unexpected URL: %q, %v", got, err)
		}
	}
	for _, origin := range []string{"file:///tmp", "ssh://example.com", "https://example.com/nested", "https://user:secret@example.com", "https://example.com?x=y", "https://example.com?", "https://example.com#x", "https://example.com/%2f", "//example.com", "https://"} {
		if got, err := SourceRepositoryURL(origin, "/list"); err == nil {
			t.Errorf("accepted invalid origin %q: %q", origin, got)
		}
	}
	if _, err := SourceRepositoryURL("https://erol.kernel.org", "//evil.example/list"); err == nil {
		t.Error("accepted host-replacing repository path")
	}
}
