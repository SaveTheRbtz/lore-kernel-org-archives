package github

import (
	"context"
	"encoding/json"
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

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-cmp/cmp"
	sdk "github.com/google/go-github/v88/github"
)

func TestEnsureRepositoryCreatesEmptyPublicMirror(t *testing.T) {
	const sourcePath = "/lkml/git/20.git"
	const name = "lkml.20"
	var stored *sdk.Repository
	var mu sync.Mutex
	var creates atomic.Int32
	g := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodGet && r.URL.Path == "/repos/testorg/"+name {
			if stored == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(stored)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/orgs/testorg/repos" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		creates.Add(1)
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["name"] != name || body["homepage"] != "https://lore.kernel.org"+sourcePath {
			t.Errorf("unexpected source mapping: %v", body)
		}
		for _, field := range []string{"private", "auto_init"} {
			if value, ok := body[field]; !ok || value != false {
				t.Errorf("%s should be explicitly false, got %v", field, value)
			}
		}
		stored = testRepository(name, sourcePath)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(stored)
	})
	for range 2 {
		got, err := g.EnsureRepository(context.Background(), sourcePath, "LKML [epoch 20]")
		if err != nil {
			t.Fatal(err)
		}
		want := Repository{Name: name, GitURL: "https://github.com/testorg/lkml.20.git"}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("repository (-want +got):\n%s", diff)
		}
	}
	if creates.Load() != 1 {
		t.Fatalf("created %d times", creates.Load())
	}
}

func TestEnsureRepositoryRefusesUnsafeAdoption(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*sdk.Repository)
	}{
		{"unrelated", func(r *sdk.Repository) { r.Homepage = sdk.Ptr("https://example.org/unrelated") }},
		{"private", func(r *sdk.Repository) { r.Private = sdk.Ptr(true) }},
		{"archived", func(r *sdk.Repository) { r.Archived = sdk.Ptr(true) }},
		{"fork", func(r *sdk.Repository) { r.Fork = sdk.Ptr(true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := testRepository("lkml.0", "/lkml/git/0.git")
			tc.change(remote)
			g := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("must not mutate an unsafe repository: %s", r.Method)
				}
				_ = json.NewEncoder(w).Encode(remote)
			})
			if _, err := g.EnsureRepository(context.Background(), "/lkml/git/0.git", ""); err == nil {
				t.Fatal("unsafe repository adopted")
			}
		})
	}
}

func TestEnsureRepositoryRecoversLostCreationResponse(t *testing.T) {
	var created atomic.Bool
	g := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			created.Store(true)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if !created.Load() {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(testRepository("netdev.0", "/netdev/git/0.git"))
	})
	if _, err := g.EnsureRepository(context.Background(), "/netdev/git/0.git", ""); err != nil {
		t.Fatal(err)
	}
}

func TestSetDefaultBranchSkipsUnchangedBranch(t *testing.T) {
	branch := "main"
	patches := 0
	var mu sync.Mutex
	g := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodPatch {
			patches++
			var update sdk.Repository
			if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
				t.Error(err)
			}
			branch = update.GetDefaultBranch()
		}
		_ = json.NewEncoder(w).Encode(sdk.Repository{DefaultBranch: sdk.Ptr(branch)})
	})
	for range 2 {
		if err := g.SetDefaultBranch(context.Background(), "lkml.0", "master"); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if branch != "master" || patches != 1 {
		t.Fatalf("branch=%s patches=%d", branch, patches)
	}
}

func TestRepositoryOperationsPropagateRateLimits(t *testing.T) {
	for _, operation := range []string{"get", "create", "edit"} {
		t.Run(operation, func(t *testing.T) {
			g := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && operation == "create" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if r.Method == http.MethodGet && operation == "edit" {
					_ = json.NewEncoder(w).Encode(sdk.Repository{DefaultBranch: sdk.Ptr("main")})
					return
				}
				w.Header().Set("Retry-After", "42")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"message":"secondary rate limit","documentation_url":"https://docs.github.com/rest/using-the-rest-api/rate-limits-for-the-rest-api#about-secondary-rate-limits"}`))
			})
			var err error
			if operation == "edit" {
				err = g.SetDefaultBranch(context.Background(), "lkml.0", "master")
			} else {
				_, err = g.EnsureRepository(context.Background(), "/lkml/git/0.git", "")
			}
			var limited interface{ RetryAfter() time.Duration }
			if !errors.As(err, &limited) || limited.RetryAfter() <= 0 {
				t.Fatalf("%s did not expose a scheduler cooldown: %v", operation, err)
			}
		})
	}
}

func TestRetryPolicyPreservesSDKRateLimits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			err   error
			delay time.Duration
		}{
			{"primary reset", &sdk.RateLimitError{Rate: sdk.Rate{Reset: sdk.Timestamp{Time: time.Now().Add(2 * time.Minute)}}}, 2 * time.Minute},
			{"secondary specified", &sdk.AbuseRateLimitError{RetryAfter: sdk.Ptr(42 * time.Second)}, 42 * time.Second},
			{"secondary unspecified", &sdk.AbuseRateLimitError{}, time.Minute},
			{"bounded", &sdk.AbuseRateLimitError{RetryAfter: sdk.Ptr(48 * time.Hour)}, 24 * time.Hour},
		} {
			got := retryPolicy(tc.err)
			var limited interface{ RetryAfter() time.Duration }
			if !errors.As(got, &limited) || limited.RetryAfter() != tc.delay || !errors.Is(got, tc.err) {
				t.Fatalf("%s: SDK rate limit was not preserved with the expected retry delay", tc.name)
			}
		}
		ordinary := errors.New("not rate limited")
		if retryPolicy(ordinary) != ordinary || retryPolicy(nil) != nil {
			t.Fatal("non-rate-limit errors changed")
		}
	})
}

func TestRetryPolicyClosesAuthResponseAndPreservesRateLimit(t *testing.T) {
	body := &trackedBody{Reader: strings.NewReader(`{"documentation_url":"https://docs.github.com/rest/using-the-rest-api/rate-limits-for-the-rest-api#about-secondary-rate-limits"}`)}
	auth := &ghinstallation.HTTPError{
		Message: "token endpoint rejected request",
		Response: &http.Response{
			StatusCode: http.StatusForbidden,
			Header:     http.Header{"Retry-After": []string{"42"}},
			Body:       body,
		},
	}
	client := &http.Client{
		Timeout: time.Second,
		Transport: authTransport{roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			body.ctx = req.Context()
			return nil, auth
		})},
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://api.github.com/repos/testorg/lkml.0", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Do(req)
	err = retryPolicy(err)
	var limited interface{ RetryAfter() time.Duration }
	if !errors.As(err, &limited) || limited.RetryAfter() != 42*time.Second || !errors.Is(err, auth) {
		t.Fatalf("auth failure lost its cause or cooldown: %v", err)
	}
	if !body.closed {
		t.Fatal("auth response body was not closed")
	}
}

type trackedBody struct {
	io.Reader
	ctx    context.Context
	closed bool
}

func (b *trackedBody) Read(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	return b.Reader.Read(p)
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func newTestGitHub(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := sdk.NewClient(sdk.WithHTTPClient(server.Client()), sdk.WithURLs(sdk.Ptr(server.URL+"/"), nil))
	if err != nil {
		t.Fatal(err)
	}
	return &Client{api: client, org: "testorg"}
}

func testRepository(name, path string) *sdk.Repository {
	return &sdk.Repository{
		ID: sdk.Ptr[int64](123), Name: sdk.Ptr(name), FullName: sdk.Ptr("testorg/" + name),
		Homepage: sdk.Ptr("https://lore.kernel.org" + path), CloneURL: sdk.Ptr("https://untrusted.example/repo.git"),
	}
}
