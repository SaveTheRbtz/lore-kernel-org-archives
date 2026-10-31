package main

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGitHubEnsureRepository(t *testing.T) {
	const sourcePath = "/lkml/git/20.git"
	name, err := RepositoryName(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	var stored *githubRepository
	var mu sync.Mutex
	var creates atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing authentication")
		}
		if r.Method == http.MethodGet && r.URL.Path == "/repos/testorg/"+name {
			if stored == nil {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"message":"Not Found"}`))
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
		for _, field := range []string{"private", "auto_init", "has_issues", "has_wiki", "has_projects"} {
			if value, ok := body[field]; !ok || value != false {
				t.Errorf("%s should be explicitly false, got %v", field, value)
			}
		}
		stored = &githubRepository{ID: 123, Name: name, FullName: "testorg/" + name, Homepage: "https://lore.kernel.org" + sourcePath}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(stored)
	}))
	defer server.Close()
	g := testGitHub(server)
	for range 2 {
		repo, err := g.EnsureRepository(context.Background(), sourcePath, "LKML [epoch 20]")
		if err != nil {
			t.Fatal(err)
		}
		if repo.Name != name || repo.GitURL != "https://github.com/testorg/"+name+".git" {
			t.Fatalf("unexpected repository: %+v", repo)
		}
	}
	if creates.Load() != 1 {
		t.Fatalf("created %d times", creates.Load())
	}
}

func TestGitHubRefusesUnrelatedRepository(t *testing.T) {
	name, _ := RepositoryName("/lkml/git/0.git")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("must not mutate unrelated repository: %s", r.Method)
		}
		_ = json.NewEncoder(w).Encode(githubRepository{ID: 1, Name: name, FullName: "testorg/" + name, Homepage: "https://example.org/unrelated"})
	}))
	defer server.Close()
	_, err := testGitHub(server).EnsureRepository(context.Background(), "/lkml/git/0.git", "")
	if err == nil || !strings.Contains(err.Error(), "provenance") {
		t.Fatalf("expected source collision rejection, got %v", err)
	}
}

func TestGitHubCreateLostResponse(t *testing.T) {
	name, _ := RepositoryName("/netdev/git/0.git")
	var created atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			created.Store(true)
			// Model a server that accepted the mutation but returned an error.
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if !created.Load() {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(githubRepository{ID: 1, Name: name, FullName: "testorg/" + name, Homepage: "https://lore.kernel.org/netdev/git/0.git"})
	}))
	defer server.Close()
	if _, err := testGitHub(server).EnsureRepository(context.Background(), "/netdev/git/0.git", ""); err != nil {
		t.Fatal(err)
	}
}

func TestGitHubDefaultBranch(t *testing.T) {
	branch := "main"
	patches := 0
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodPatch {
			patches++
			var update struct {
				DefaultBranch string `json:"default_branch"`
			}
			if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
				t.Error(err)
			}
			branch = update.DefaultBranch
		}
		_ = json.NewEncoder(w).Encode(githubRepository{DefaultBranch: branch})
	}))
	defer server.Close()
	for range 2 {
		if err := testGitHub(server).SetDefaultBranch(context.Background(), "lkml.0", "master"); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if branch != "master" || patches != 1 {
		t.Fatalf("branch=%s patches=%d", branch, patches)
	}
}

func TestGitHubRateLimitAndRedaction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "42")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"message":"denied test-token"}`))
	}))
	defer server.Close()
	_, err := testGitHub(server).EnsureRepository(context.Background(), "/lkml/git/0.git", "")
	var apiErr *GitHubAPIError
	if !errors.As(err, &apiErr) || apiErr.RetryAfter() != 42*time.Second {
		t.Fatalf("expected retryable HTTP error, got %v", err)
	}
	if strings.Contains(err.Error(), "test-token") {
		t.Fatal("token leaked in error")
	}
}

func TestGitHubRejectsRedirectAndPlainHTTP(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	g := testGitHub(server)
	_, err := g.EnsureRepository(context.Background(), "/lkml/git/0.git", "")
	var apiErr *GitHubAPIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusTemporaryRedirect || redirected.Load() != 0 {
		t.Fatalf("redirect followed or misreported: %v", err)
	}
	g.AllowHTTP = false
	_, err = g.EnsureRepository(context.Background(), "/lkml/git/0.git", "")
	if err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("expected HTTP rejection, got %v", err)
	}
}

func TestGitHubRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", (2<<20)+1)))
	}))
	defer server.Close()
	_, err := testGitHub(server).EnsureRepository(context.Background(), "/lkml/git/0.git", "")
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected response limit error, got %v", err)
	}
}

func TestAppAuthJWTAndRefresh(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var clock atomic.Int64
	clock.Store(time.Date(2026, 10, 30, 22, 0, 0, 0, time.UTC).Unix())
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestNumber := requests.Add(1)
		now := time.Unix(clock.Load(), 0)
		if r.Method != http.MethodPost || r.URL.Path != "/app/installations/42/access_tokens" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		jwt := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		parts := strings.Split(jwt, ".")
		if len(parts) != 3 {
			t.Error("invalid JWT structure")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
		signature, err := base64.RawURLEncoding.DecodeString(parts[2])
		if err != nil || rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], signature) != nil {
			t.Error("invalid JWT signature")
		}
		claimsJSON, _ := base64.RawURLEncoding.DecodeString(parts[1])
		var claims struct {
			Issuer string `json:"iss"`
			Issued int64  `json:"iat"`
			Expiry int64  `json:"exp"`
		}
		if err := json.Unmarshal(claimsJSON, &claims); err != nil {
			t.Error(err)
		}
		if claims.Issuer != "123" || claims.Issued != now.Add(-time.Minute).Unix() || claims.Expiry != now.Add(9*time.Minute).Unix() {
			t.Errorf("unexpected claims: %+v", claims)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"token": fmt.Sprintf("token-%d", requestNumber), "expires_at": now.Add(time.Hour)})
	}))
	defer server.Close()
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	auth, err := NewAppAuth("123", 42, pemKey, server.Client(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	auth.now = func() time.Time { return time.Unix(clock.Load(), 0) }
	for range 2 {
		token, err := auth.Token(context.Background())
		if err != nil || token != "token-1" {
			t.Fatalf("token=%q err=%v", token, err)
		}
	}
	if requests.Load() != 1 {
		t.Fatalf("expected cached token, made %d requests", requests.Load())
	}
	clock.Add(int64(56 * time.Minute / time.Second))
	token, err := auth.Token(context.Background())
	if err != nil || token != "token-2" || requests.Load() != 2 {
		t.Fatalf("refresh token=%q requests=%d err=%v", token, requests.Load(), err)
	}
}

func TestAppAuthPKCS8AndInvalidKeys(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewAppAuth("123", 42, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}), nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAppAuth("123", 42, []byte("not a key"), nil, ""); err == nil {
		t.Fatal("invalid key accepted")
	}
}

func testGitHub(server *httptest.Server) *GitHub {
	return &GitHub{
		Client: server.Client(), APIBase: server.URL, Org: "testorg", AllowHTTP: true,
		Token: func(context.Context) (string, error) { return "test-token", nil },
	}
}
