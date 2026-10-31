package main

import (
	"bytes"
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
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const githubAPI = "https://api.github.com"

// GitHub provisions a dedicated mirror namespace. Token is evaluated for each
// request so an App installation token can be renewed without restarting.
type GitHub struct {
	Client    *http.Client
	APIBase   string
	Org       string
	Token     func(context.Context) (string, error)
	AllowHTTP bool // Only for local protocol tests.
}

type Repository struct {
	Name   string
	GitURL string
}

type githubRepository struct {
	ID            int64  `json:"id"`
	Name          string `json:"name"`
	FullName      string `json:"full_name"`
	Homepage      string `json:"homepage"`
	Private       bool   `json:"private"`
	Archived      bool   `json:"archived"`
	Fork          bool   `json:"fork"`
	DefaultBranch string `json:"default_branch"`
}

// EnsureRepository never adopts a repository merely because its name matches.
// The exact source URL is also recorded in the GitHub homepage field.
func (g *GitHub) EnsureRepository(ctx context.Context, sourcePath, description string) (Repository, error) {
	name, err := RepositoryName(sourcePath)
	if err != nil {
		return Repository{}, err
	}
	if !validGitHubName(g.Org) {
		return Repository{}, errors.New("invalid GitHub organization")
	}
	sourceURL := "https://lore.kernel.org" + sourcePath
	path := "/repos/" + g.Org + "/" + name
	var remote githubRepository
	err = g.request(ctx, http.MethodGet, path, nil, &remote)
	if err == nil {
		return g.validateRepository(remote, name, sourceURL)
	}
	var apiErr *GitHubAPIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
		return Repository{}, err
	}
	// Descriptions are metadata, never commits in the archived history.
	desc := "Mirror of " + sourceURL
	if description != "" {
		desc += ": " + description
	}
	if runes := []rune(desc); len(runes) > 350 {
		desc = string(runes[:350])
	}
	create := struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Homepage    string `json:"homepage"`
		Private     bool   `json:"private"`
		AutoInit    bool   `json:"auto_init"`
		HasIssues   bool   `json:"has_issues"`
		HasWiki     bool   `json:"has_wiki"`
		HasProjects bool   `json:"has_projects"`
	}{Name: name, Description: desc, Homepage: sourceURL}
	err = g.request(ctx, http.MethodPost, "/orgs/"+g.Org+"/repos", create, &remote)
	if err != nil {
		// A conflict or lost response may mean creation already succeeded. Read
		// back once; the same provenance check applies to both paths.
		createErr := err
		if err = g.request(ctx, http.MethodGet, path, nil, &remote); err != nil {
			return Repository{}, createErr
		}
	}
	return g.validateRepository(remote, name, sourceURL)
}

func (g *GitHub) validateRepository(remote githubRepository, name, sourceURL string) (Repository, error) {
	if remote.ID <= 0 || !strings.EqualFold(remote.Name, name) ||
		!strings.EqualFold(remote.FullName, g.Org+"/"+name) || remote.Homepage != sourceURL {
		return Repository{}, fmt.Errorf("refusing to adopt %s/%s: source provenance does not match %s", g.Org, name, sourceURL)
	}
	if remote.Private || remote.Archived || remote.Fork {
		return Repository{}, fmt.Errorf("refusing %s: mirror repository must be public, writable, and not a fork", remote.FullName)
	}
	// Do not follow an arbitrary clone URL returned by the API.
	return Repository{Name: name, GitURL: "https://github.com/" + g.Org + "/" + name + ".git"}, nil
}

func (g *GitHub) SetDefaultBranch(ctx context.Context, repoName, branch string) error {
	if !validGitHubName(g.Org) || !validGitHubName(repoName) || branch == "" {
		return errors.New("invalid repository or default branch")
	}
	path := "/repos/" + g.Org + "/" + repoName
	var remote githubRepository
	if err := g.request(ctx, http.MethodGet, path, nil, &remote); err != nil {
		return err
	}
	if remote.DefaultBranch == branch {
		return nil
	}
	return g.request(ctx, http.MethodPatch, path, map[string]string{"default_branch": branch}, nil)
}

func validGitHubName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	for _, c := range name {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' {
			continue
		}
		return false
	}
	return true
}

func (g *GitHub) request(ctx context.Context, method, path string, in, out any) error {
	if g.Token == nil {
		return errors.New("GitHub authentication is not configured")
	}
	token, err := g.Token(ctx)
	if err != nil {
		return fmt.Errorf("GitHub authentication: %w", err)
	}
	return githubRequest(ctx, g.Client, g.APIBase, g.AllowHTTP, method, path, token, in, out)
}

// GitHubAPIError exposes server-requested delays to the scheduler.
type GitHubAPIError struct {
	StatusCode int
	Method     string
	Path       string
	Message    string
	retryAfter time.Duration
}

func (e *GitHubAPIError) Error() string {
	return fmt.Sprintf("GitHub %s %s: HTTP %d: %s", e.Method, e.Path, e.StatusCode, e.Message)
}

func (e *GitHubAPIError) RetryAfter() time.Duration { return e.retryAfter }

func githubRequest(ctx context.Context, client *http.Client, base string, allowHTTP bool, method, path, token string, in, out any) error {
	if base == "" {
		base = githubAPI
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		(u.Scheme != "https" && !(allowHTTP && u.Scheme == "http")) {
		return errors.New("GitHub API base must be an HTTPS URL without credentials, query, or fragment")
	}
	if token == "" {
		return errors.New("GitHub authentication token is empty")
	}
	var body io.Reader
	if in != nil {
		encoded, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "lore-mirror")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if client == nil {
		client = http.DefaultClient
	}
	// Do not forward credentials or replay mutations through redirects.
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	res, err := copyClient.Do(req)
	if err != nil {
		return fmt.Errorf("GitHub %s %s: %w", method, path, err)
	}
	defer res.Body.Close()
	const maxResponseBytes = 2 << 20
	data, err := io.ReadAll(io.LimitReader(res.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read GitHub response: %w", err)
	}
	if len(data) > maxResponseBytes {
		return errors.New("GitHub response exceeds 2 MiB")
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var response struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(data, &response)
		message := strings.ReplaceAll(response.Message, token, "[redacted]")
		if len(message) > 512 {
			message = message[:512]
		}
		return &GitHubAPIError{StatusCode: res.StatusCode, Method: method, Path: path, Message: message, retryAfter: githubRetryAfter(res.Header, time.Now())}
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode GitHub response: %w", err)
		}
	}
	return nil
}

func githubRetryAfter(h http.Header, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(h.Get("Retry-After"), 10, 64); err == nil && seconds > 0 {
		return time.Duration(min(seconds, 86400)) * time.Second
	}
	if at, err := http.ParseTime(h.Get("Retry-After")); err == nil && at.After(now) {
		return min(at.Sub(now), 24*time.Hour)
	}
	if h.Get("X-RateLimit-Remaining") == "0" {
		if seconds, err := strconv.ParseInt(h.Get("X-RateLimit-Reset"), 10, 64); err == nil {
			return max(0, min(time.Unix(seconds, 0).Sub(now), 24*time.Hour))
		}
	}
	return 0
}

// AppAuth holds only an in-memory installation token. Its key is loaded by the
// caller from Secret Manager; no long-lived credential is sent to GitHub.
type AppAuth struct {
	client         *http.Client
	apiBase        string
	appID          string
	installationID int64
	key            *rsa.PrivateKey
	now            func() time.Time
	mu             sync.Mutex
	token          string
	expires        time.Time
}

func NewAppAuth(appID string, installationID int64, pemKey []byte, client *http.Client, apiBase string) (*AppAuth, error) {
	if appID == "" || installationID <= 0 {
		return nil, errors.New("GitHub App ID and positive installation ID are required")
	}
	block, rest := pem.Decode(pemKey)
	if block == nil || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("expected one PEM-encoded GitHub App private key")
	}
	var key *rsa.PrivateKey
	switch block.Type {
	case "RSA PRIVATE KEY":
		var err error
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse GitHub App RSA key: %w", err)
		}
	case "PRIVATE KEY":
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse GitHub App key: %w", err)
		}
		var ok bool
		key, ok = parsed.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("GitHub App key must be RSA")
		}
	default:
		return nil, errors.New("GitHub App key must be an unencrypted RSA private key")
	}
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("invalid GitHub App key: %w", err)
	}
	return &AppAuth{client: client, apiBase: apiBase, appID: appID, installationID: installationID, key: key, now: time.Now}, nil
}

func (a *AppAuth) Token(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	if a.token != "" && now.Add(5*time.Minute).Before(a.expires) {
		return a.token, nil
	}
	jwt, err := a.jwt(now)
	if err != nil {
		return "", err
	}
	var response struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	path := "/app/installations/" + strconv.FormatInt(a.installationID, 10) + "/access_tokens"
	if err := githubRequest(ctx, a.client, a.apiBase, false, http.MethodPost, path, jwt, struct{}{}, &response); err != nil {
		return "", err
	}
	if response.Token == "" || !response.ExpiresAt.After(now) {
		return "", errors.New("GitHub returned an empty or expired installation token")
	}
	a.token, a.expires = response.Token, response.ExpiresAt
	return a.token, nil
}

func (a *AppAuth) jwt(now time.Time) (string, error) {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, err := json.Marshal(struct {
		IssuedAt int64  `json:"iat"`
		Expires  int64  `json:"exp"`
		Issuer   string `json:"iss"`
	}{IssuedAt: now.Add(-time.Minute).Unix(), Expires: now.Add(9 * time.Minute).Unix(), Issuer: a.appID})
	if err != nil {
		return "", err
	}
	message := header + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(message))
	signature, err := rsa.SignPKCS1v15(rand.Reader, a.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign GitHub App JWT: %w", err)
	}
	return message + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}
