package github

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-cmp/cmp"
	sdk "github.com/google/go-github/v88/github"
)

func TestNewRejectsInvalidOrganizationBeforeCredentials(t *testing.T) {
	t.Setenv("GITHUB_APP_SECRET", "")
	for _, tc := range []struct{ org, problem string }{
		{"", "organization"},
		{"owner/repo", "organization"},
		{"lore-kernel-org-archives", "GITHUB_APP_SECRET"},
	} {
		if _, err := New(t.Context(), http.DefaultClient, tc.org); err == nil || !strings.Contains(err.Error(), tc.problem) {
			t.Errorf("New(%q) returned %v; want %s validation", tc.org, err, tc.problem)
		}
	}
}

func TestEnsureRepositoryCreatesEmptyPublicMirror(t *testing.T) {
	type creation struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Homepage    string `json:"homepage"`
		Private     *bool  `json:"private"`
		AutoInit    *bool  `json:"auto_init"`
	}
	created := make(chan creation, 1)
	g := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/repos/testorg/git.1" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/orgs/testorg/repos" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var body creation
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		select {
		case created <- body:
		default:
			t.Error("mirror created more than once")
		}
		w.WriteHeader(http.StatusCreated)
		remote := testRepository("git.1", "/git/git/1.git")
		remote.Description = sdk.Ptr(body.Description)
		_ = json.NewEncoder(w).Encode(remote)
	})
	got, err := g.EnsureRepository(t.Context(), "/git/git/1.git", sdk.Ptr("Git development <git@vger.kernel.org> (2 out of 2)"), 0)
	if err != nil {
		t.Fatal(err)
	}
	want := Repository{ID: 123, Name: "git.1", GitURL: "https://github.com/testorg/git.1.git", DefaultBranch: "master"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("repository (-want +got):\n%s", diff)
	}
	select {
	case got := <-created:
		want := creation{Name: "git.1", Description: "Mirror of https://lore.kernel.org/git/git/1.git: Git development <git@vger.kernel.org> (2 out of 2)", Homepage: "https://lore.kernel.org/git/", Private: sdk.Ptr(false), AutoInit: sdk.Ptr(false)}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("creation (-want +got):\n%s", diff)
		}
	default:
		t.Fatal("mirror was not created")
	}
}

func TestEnsureRepositoryReconcilesMetadata(t *testing.T) {
	const description = "Mirror of https://lore.kernel.org/lkml/git/0.git: LKML (1 out of 21)"
	summary := sdk.Ptr("LKML (1 out of 21)")
	for _, tc := range []struct {
		name, homepage, description string
		summary                     *string
		expectedID                  int64
		patches                     int32
	}{
		{"old format", "https://lore.kernel.org/lkml/git/0.git", "Mirror of https://lore.kernel.org/lkml/git/0.git: LKML [epoch 0]", summary, 0, 1},
		{"legacy source homepage", "https://lore.kernel.org/lkml/git/0.git", "", summary, 0, 1},
		{"new epoch", "https://lore.kernel.org/lkml/", "Mirror of https://lore.kernel.org/lkml/git/0.git: LKML (1 out of 20)", summary, 0, 1},
		{"current", "https://lore.kernel.org/lkml/", description, summary, 0, 0},
		{"approved ID with cleared description", "https://lore.kernel.org/lkml/", "", summary, 123, 1},
		{"unavailable address", "https://lore.kernel.org/lkml/", "Mirror of https://lore.kernel.org/lkml/git/0.git: LKML <linux-kernel@vger.kernel.org>", nil, 0, 0},
		{"unavailable address with legacy homepage", "https://lore.kernel.org/lkml/git/0.git", "", nil, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var patches atomic.Int32
			g := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
				remote := testRepository("lkml.0", "/lkml/git/0.git")
				remote.Homepage, remote.Description = sdk.Ptr(tc.homepage), sdk.Ptr(tc.description)
				if r.Method == http.MethodPatch {
					patches.Add(1)
					var update sdk.Repository
					if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
						t.Error(err)
					}
					if update.GetHomepage() != "https://lore.kernel.org/lkml/" || update.GetDescription() != description {
						t.Errorf("metadata update = %s, %s", update.GetHomepage(), update.GetDescription())
					}
					remote.Homepage, remote.Description = update.Homepage, update.Description
				} else if r.Method != http.MethodGet {
					t.Errorf("unexpected method %s", r.Method)
				}
				_ = json.NewEncoder(w).Encode(remote)
			})
			if _, err := g.EnsureRepository(t.Context(), "/lkml/git/0.git", tc.summary, tc.expectedID); err != nil {
				t.Fatal(err)
			}
			if patches.Load() != tc.patches {
				t.Fatalf("metadata patched %d times, want %d", patches.Load(), tc.patches)
			}
		})
	}
}

func TestEnsureRepositoryRefusesUnsafeAdoption(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*sdk.Repository)
	}{
		{"unrelated", func(r *sdk.Repository) { r.Homepage = sdk.Ptr("https://example.org/unrelated") }},
		{"wrong epoch", func(r *sdk.Repository) {
			r.Description = sdk.Ptr("Mirror of https://lore.kernel.org/lkml/git/2.git: LKML")
		}},
		{"missing source", func(r *sdk.Repository) { r.Description = nil }},
		{"source URL prefix", func(r *sdk.Repository) {
			r.Description = sdk.Ptr("Mirror of https://lore.kernel.org/lkml/git/0.git.bak")
		}},
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
			if _, err := g.EnsureRepository(context.Background(), "/lkml/git/0.git", nil, 0); err == nil {
				t.Fatal("unsafe repository adopted")
			}
		})
	}
}

func TestEnsureRepositoryRevalidatesApprovedIdentity(t *testing.T) {
	approvedRemote := testRepository("lkml.0", "/lkml/git/0.git")
	replacement := *approvedRemote
	replacement.ID = sdk.Ptr[int64](456)
	unrelated := *approvedRemote
	unrelated.Homepage = sdk.Ptr("https://example.org/unrelated")
	for _, tc := range []struct {
		name    string
		current *sdk.Repository
		wantErr bool
	}{
		{"unchanged", approvedRemote, false},
		{"replacement", &replacement, true},
		{"provenance changed", &unrelated, true},
		{"deleted", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var remote atomic.Pointer[sdk.Repository]
			remote.Store(approvedRemote)
			g := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("must not mutate a previously approved repository: %s", r.Method)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				current := remote.Load()
				if current == nil {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_ = json.NewEncoder(w).Encode(current)
			})
			approved, err := g.EnsureRepository(t.Context(), "/lkml/git/0.git", nil, 0)
			if err != nil || approved.ID != 123 {
				t.Fatalf("initial approval = %+v, %v", approved, err)
			}
			remote.Store(tc.current)
			current, err := g.EnsureRepository(t.Context(), "/lkml/git/0.git", nil, approved.ID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("revalidation error = %v, want error=%v", err, tc.wantErr)
			}
			if !tc.wantErr && current != approved {
				t.Fatalf("unchanged repository = %+v; want %+v", current, approved)
			}
		})
	}
}

func TestEnsureRepositoryRecoversLostCreationResponse(t *testing.T) {
	var created atomic.Bool
	g := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var create sdk.Repository
			if err := json.NewDecoder(r.Body).Decode(&create); err != nil {
				t.Error(err)
			}
			if got := create.GetDescription(); got != "Mirror of https://lore.kernel.org/netdev/git/0.git" {
				t.Errorf("creation without posting metadata: description = %q", got)
			}
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
	got, err := g.EnsureRepository(t.Context(), "/netdev/git/0.git", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := Repository{ID: 123, Name: "netdev.0", GitURL: "https://github.com/testorg/netdev.0.git", DefaultBranch: "master"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("recovered repository (-want +got):\n%s", diff)
	}
}

func TestSetDefaultBranch(t *testing.T) {
	var patches atomic.Int32
	g := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/repos/testorg/lkml.0" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		patches.Add(1)
		var update sdk.Repository
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
			t.Error(err)
		}
		if update.GetDefaultBranch() != "master" {
			t.Errorf("default branch = %q, want master", update.GetDefaultBranch())
		}
		_ = json.NewEncoder(w).Encode(update)
	})
	if err := g.SetDefaultBranch(t.Context(), "lkml.0", "master"); err != nil {
		t.Fatal(err)
	}
	if patches.Load() != 1 {
		t.Fatalf("branch updated %d times, want once", patches.Load())
	}
}

func TestRepositoryOperationsPropagateRateLimits(t *testing.T) {
	for _, operation := range []string{"get", "create", "metadata", "edit"} {
		t.Run(operation, func(t *testing.T) {
			var requests atomic.Int32
			g := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method == http.MethodGet && operation == "create" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if r.Method == http.MethodGet && operation == "metadata" {
					remote := testRepository("lkml.0", "/lkml/git/0.git")
					remote.Description = sdk.Ptr(remote.GetDescription() + ": outdated")
					_ = json.NewEncoder(w).Encode(remote)
					return
				}
				w.Header().Set("Retry-After", "42")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"message":"secondary rate limit","documentation_url":"https://docs.github.com/rest/using-the-rest-api/rate-limits-for-the-rest-api#about-secondary-rate-limits"}`))
			})
			// Let an unintended readback reach the handler instead of SDK throttling.
			ctx := context.WithValue(t.Context(), sdk.BypassRateLimitCheck, true)
			var err error
			if operation == "edit" {
				err = g.SetDefaultBranch(ctx, "lkml.0", "master")
			} else {
				_, err = g.EnsureRepository(ctx, "/lkml/git/0.git", sdk.Ptr(""), 0)
			}
			var limited interface{ RetryAfter() time.Duration }
			if !errors.As(err, &limited) || limited.RetryAfter() != 42*time.Second {
				t.Fatalf("%s did not expose a 42-second scheduler cooldown: %v", operation, err)
			}
			wantRequests := int32(1)
			if operation == "create" || operation == "metadata" {
				wantRequests = 2 // Initial GET and throttled mutation, with no readback.
			}
			if requests.Load() != wantRequests {
				t.Fatalf("%s made %d requests, want %d", operation, requests.Load(), wantRequests)
			}
		})
	}
}

func TestCreateHonorsBareRetryAfter(t *testing.T) {
	var requests atomic.Int32
	g := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	_, err := g.EnsureRepository(t.Context(), "/lkml/git/0.git", nil, 0)
	var limited interface{ RetryAfter() time.Duration }
	if !errors.As(err, &limited) || limited.RetryAfter() != 2*time.Minute {
		t.Errorf("expected a two-minute scheduler cooldown, got %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Errorf("got %d requests, want only the lookup and creation attempt", got)
	}
}

func TestRetryPolicyPreservesSDKRateLimits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		response := &http.Response{StatusCode: http.StatusForbidden, Header: make(http.Header)}
		response.Header.Set("Retry-After", time.Now().Add(30*time.Second).UTC().Format(http.TimeFormat))
		for _, tc := range []struct {
			name  string
			err   error
			delay time.Duration
		}{
			{"primary reset", &sdk.RateLimitError{Rate: sdk.Rate{Reset: sdk.Timestamp{Time: time.Now().Add(2 * time.Minute)}}}, 2 * time.Minute},
			{"primary reset later than header", &sdk.RateLimitError{Response: response, Rate: sdk.Rate{Reset: sdk.Timestamp{Time: time.Now().Add(time.Minute)}}}, time.Minute},
			{"secondary HTTP-date", &sdk.AbuseRateLimitError{Response: response}, 30 * time.Second},
			{"secondary unspecified", &sdk.AbuseRateLimitError{}, time.Minute},
			{"bounded", &sdk.AbuseRateLimitError{RetryAfter: sdk.Ptr(48 * time.Hour)}, 24 * time.Hour},
		} {
			got := retryPolicy(tc.err)
			var limited interface{ RetryAfter() time.Duration }
			if !errors.As(got, &limited) || limited.RetryAfter() != tc.delay || !errors.Is(got, tc.err) {
				t.Fatalf("%s: SDK rate limit was not preserved with the expected retry delay", tc.name)
			}
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
	server := httptest.NewTestServer(t, handler)
	client, err := sdk.NewClient(sdk.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	return &Client{api: client, org: "testorg"}
}

func testRepository(name, path string) *sdk.Repository {
	list, _, _ := strings.Cut(path[1:], "/")
	return &sdk.Repository{
		ID: sdk.Ptr[int64](123), Name: sdk.Ptr(name), FullName: sdk.Ptr("testorg/" + name),
		Homepage: sdk.Ptr("https://lore.kernel.org/" + list + "/"), Description: sdk.Ptr("Mirror of https://lore.kernel.org" + path),
		CloneURL:      sdk.Ptr("https://untrusted.example/repo.git"),
		DefaultBranch: sdk.Ptr("master"),
	}
}
