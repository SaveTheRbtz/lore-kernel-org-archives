package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"lore-mirror/internal/lore"
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
	name, err := lore.RepositoryName(sourcePath)
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
		(u.Scheme != "https" && (u.Scheme != "http" || !allowHTTP)) {
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
	defer func() { _ = res.Body.Close() }()
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
