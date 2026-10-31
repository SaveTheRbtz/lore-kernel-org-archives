package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	sdk "github.com/google/go-github/v88/github"

	"lore-mirror/internal/lore"
)

// Client provisions mirrors and shares installation-token refresh with Git pushes.
type Client struct {
	api          *sdk.Client
	org          string
	installation *ghinstallation.Transport
}

type Repository struct {
	Name   string
	GitURL string
}

func New(ctx context.Context, client *http.Client, org string) (*Client, error) {
	secret, err := readAppSecret(ctx)
	if err != nil {
		return nil, err
	}
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	installation, err := ghinstallation.New(transport, secret.AppID, secret.InstallationID, []byte(secret.PrivateKey))
	if err != nil {
		return nil, fmt.Errorf("GitHub App credentials: %w", err)
	}
	api, err := sdk.NewClient(sdk.WithHTTPClient(client), sdk.WithTransport(authTransport{installation}), sdk.WithTimeout(30*time.Second), sdk.WithUserAgent("lore-mirror"))
	if err != nil {
		return nil, err
	}
	return &Client{api: api, org: org, installation: installation}, nil
}

func (g *Client) Token(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	token, err := g.installation.Token(ctx)
	return token, retryPolicy(err)
}

// Normalize auth failures before http.Client cancels their request context.
type authTransport struct{ http.RoundTripper }

func (t authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	response, err := t.RoundTripper.RoundTrip(req)
	return response, retryPolicy(err)
}

// EnsureRepository only adopts repositories with the exact source homepage.
func (g *Client) EnsureRepository(ctx context.Context, sourcePath, description string) (Repository, error) {
	name, err := lore.RepositoryName(sourcePath)
	if err != nil {
		return Repository{}, err
	}
	if !validGitHubName(g.org) {
		return Repository{}, errors.New("invalid GitHub organization")
	}
	sourceURL := "https://lore.kernel.org" + sourcePath
	remote, response, err := g.api.Repositories.Get(ctx, g.org, name)
	if err == nil {
		return g.validateRepository(remote, name, sourceURL)
	}
	if response == nil || response.StatusCode != http.StatusNotFound {
		return Repository{}, retryPolicy(err)
	}
	desc := "Mirror of " + sourceURL
	if description != "" {
		desc += ": " + description
	}
	if runes := []rune(desc); len(runes) > 350 {
		desc = string(runes[:350])
	}
	remote, _, err = g.api.Repositories.Create(ctx, g.org, &sdk.Repository{
		Name: sdk.Ptr(name), Description: sdk.Ptr(desc), Homepage: sdk.Ptr(sourceURL),
		Private: sdk.Ptr(false), AutoInit: sdk.Ptr(false),
		HasIssues: sdk.Ptr(false), HasWiki: sdk.Ptr(false), HasProjects: sdk.Ptr(false),
	})
	if err != nil {
		createErr := retryPolicy(err)
		var limited *rateLimitError
		if errors.As(createErr, &limited) {
			return Repository{}, createErr
		}
		// Creation can succeed even when its response is lost. Read back once
		// and apply the same provenance check before accepting the repository.
		remote, _, err = g.api.Repositories.Get(ctx, g.org, name)
		if err != nil {
			return Repository{}, errors.Join(createErr, retryPolicy(err))
		}
	}
	return g.validateRepository(remote, name, sourceURL)
}

func (g *Client) validateRepository(remote *sdk.Repository, name, sourceURL string) (Repository, error) {
	if remote.GetID() <= 0 || !strings.EqualFold(remote.GetName(), name) ||
		!strings.EqualFold(remote.GetFullName(), g.org+"/"+name) || remote.GetHomepage() != sourceURL {
		return Repository{}, fmt.Errorf("refusing to adopt %s/%s: source provenance does not match %s", g.org, name, sourceURL)
	}
	if remote.GetPrivate() || remote.GetArchived() || remote.GetFork() {
		return Repository{}, fmt.Errorf("refusing %s: mirror repository must be public, writable, and not a fork", remote.GetFullName())
	}
	// Clone URLs returned by the API are never followed.
	return Repository{Name: name, GitURL: "https://github.com/" + g.org + "/" + name + ".git"}, nil
}

func (g *Client) SetDefaultBranch(ctx context.Context, repoName, branch string) error {
	if !validGitHubName(g.org) || !validGitHubName(repoName) || branch == "" {
		return errors.New("invalid repository or default branch")
	}
	remote, _, err := g.api.Repositories.Get(ctx, g.org, repoName)
	if err != nil {
		return retryPolicy(err)
	}
	if remote.GetDefaultBranch() == branch {
		return nil
	}
	_, _, err = g.api.Repositories.Edit(ctx, g.org, repoName, &sdk.Repository{DefaultBranch: sdk.Ptr(branch)})
	return retryPolicy(err)
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

// The SDK parses rate limits; this adapter exposes their delay to the scheduler.
type rateLimitError struct {
	error
	after time.Duration
}

func (e *rateLimitError) Unwrap() error             { return e.error }
func (e *rateLimitError) RetryAfter() time.Duration { return e.after }

func retryPolicy(err error) error {
	var limited *rateLimitError
	if errors.As(err, &limited) {
		return err
	}
	var auth *ghinstallation.HTTPError
	if errors.As(err, &auth) && auth.Response != nil {
		// ghinstallation leaves error bodies open; CheckResponse replaces Body.
		body := auth.Response.Body
		err = errors.Join(err, sdk.CheckResponse(auth.Response))
		_ = body.Close()
	}
	var primary *sdk.RateLimitError
	var secondary *sdk.AbuseRateLimitError
	var delay time.Duration
	switch {
	case errors.As(err, &primary):
		delay = time.Until(primary.Rate.Reset.Time)
	case errors.As(err, &secondary):
		delay = time.Minute
		if secondary.RetryAfter != nil && *secondary.RetryAfter > 0 {
			delay = *secondary.RetryAfter
		}
	default:
		return err
	}
	return &rateLimitError{error: err, after: max(0, min(delay, 24*time.Hour))}
}
