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
	"lore-mirror/internal/retryafter"
)

const mirrorDescriptionPrefix = "Mirror of "

// Client provisions mirrors and shares installation-token refresh with Git pushes.
type Client struct {
	api          *sdk.Client
	org          string
	installation *ghinstallation.Transport
}

type Repository struct {
	ID            int64
	Name          string
	GitURL        string
	DefaultBranch string
}

func New(ctx context.Context, client *http.Client, org string) (*Client, error) {
	if !validOrganization(org) {
		return nil, errors.New("invalid GitHub organization")
	}
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

// EnsureRepository checks provenance before reconciling mirror metadata.
// A nil summary preserves existing metadata and creates new mirrors with just the source marker.
func (g *Client) EnsureRepository(ctx context.Context, sourcePath string, summary *string, expectedID int64) (Repository, error) {
	name, err := lore.RepositoryName(sourcePath)
	if err != nil {
		return Repository{}, err
	}
	sourceURL := lore.Origin + sourcePath
	listURL := lore.Origin + "/" + lore.InboxName(sourcePath) + "/"
	desc := mirrorDescriptionPrefix + sourceURL
	if summary != nil && *summary != "" {
		desc += ": " + *summary
	}
	if runes := []rune(desc); len(runes) > 350 {
		desc = string(runes[:350])
	}
	remote, response, err := g.api.Repositories.Get(ctx, g.org, name)
	if err != nil {
		if response == nil || response.StatusCode != http.StatusNotFound {
			return Repository{}, retryPolicy(err)
		}
		if expectedID != 0 {
			return Repository{}, fmt.Errorf("refusing to recreate %s/%s: approved repository %d is missing", g.org, name, expectedID)
		}
		remote, _, err = g.api.Repositories.Create(ctx, g.org, &sdk.Repository{
			Name: sdk.Ptr(name), Description: sdk.Ptr(desc), Homepage: sdk.Ptr(listURL),
			Private: sdk.Ptr(false), AutoInit: sdk.Ptr(false),
			HasIssues: sdk.Ptr(false), HasWiki: sdk.Ptr(false), HasProjects: sdk.Ptr(false),
		})
		if err != nil {
			createErr := retryPolicy(err)
			var limited *retryafter.Error
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
	}
	repo, err := g.validateRepository(remote, name, sourceURL, listURL, expectedID)
	if err != nil {
		return Repository{}, err
	}
	if summary != nil && (remote.GetDescription() != desc || remote.GetHomepage() != listURL) {
		remote, _, err = g.api.Repositories.Edit(ctx, g.org, name, &sdk.Repository{
			Description: sdk.Ptr(desc), Homepage: sdk.Ptr(listURL),
		})
		if err != nil {
			return Repository{}, retryPolicy(err)
		}
		return g.validateRepository(remote, name, sourceURL, listURL, repo.ID)
	}
	return repo, nil
}

func (g *Client) validateRepository(remote *sdk.Repository, name, sourceURL, listURL string, expectedID int64) (Repository, error) {
	if expectedID != 0 && remote.GetID() != expectedID {
		return Repository{}, fmt.Errorf("refusing %s/%s: repository ID changed from %d to %d", g.org, name, expectedID, remote.GetID())
	}
	if remote.GetID() <= 0 || !strings.EqualFold(remote.GetName(), name) ||
		!strings.EqualFold(remote.GetFullName(), g.org+"/"+name) ||
		(remote.GetHomepage() != sourceURL && remote.GetHomepage() != listURL) {
		return Repository{}, fmt.Errorf("refusing to adopt %s/%s: source provenance does not match %s", g.org, name, sourceURL)
	}
	// All epochs share the list homepage. On first adoption, the description must identify the archive.
	if expectedID == 0 && remote.GetHomepage() == listURL {
		marker, _, _ := strings.Cut(remote.GetDescription(), ": ")
		if marker != mirrorDescriptionPrefix+sourceURL {
			return Repository{}, fmt.Errorf("refusing to adopt %s/%s: description does not identify %s", g.org, name, sourceURL)
		}
	}
	if remote.GetPrivate() || remote.GetArchived() || remote.GetFork() {
		return Repository{}, fmt.Errorf("refusing %s: mirror repository must be public, writable, and not a fork", remote.GetFullName())
	}
	// Clone URLs returned by the API are never followed.
	return Repository{
		ID: remote.GetID(), Name: name, GitURL: "https://github.com/" + g.org + "/" + name + ".git",
		DefaultBranch: remote.GetDefaultBranch(),
	}, nil
}

func (g *Client) SetDefaultBranch(ctx context.Context, repoName, branch string) error {
	_, _, err := g.api.Repositories.Edit(ctx, g.org, repoName, &sdk.Repository{DefaultBranch: sdk.Ptr(branch)})
	return retryPolicy(err)
}

func validOrganization(name string) bool {
	if name == "" {
		return false
	}
	for _, c := range name {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' {
			continue
		}
		return false
	}
	return true
}

func retryPolicy(err error) error {
	var limited *retryafter.Error
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
	var apiErr *sdk.ErrorResponse
	switch {
	case errors.As(err, &primary):
		delay := retryafter.Delay(primary.Response, time.Until(primary.Rate.Reset.Time))
		return retryafter.Wrap(err, delay)
	case errors.As(err, &secondary):
		var delay time.Duration
		if secondary.RetryAfter != nil {
			delay = *secondary.RetryAfter
		}
		delay = retryafter.Delay(secondary.Response, delay)
		if delay <= 0 {
			delay = time.Minute
		}
		return retryafter.Wrap(err, delay)
	case errors.As(err, &apiErr):
		if delay := retryafter.Delay(apiErr.Response, 0); delay > 0 {
			return retryafter.Wrap(err, delay)
		}
	}
	return err
}
