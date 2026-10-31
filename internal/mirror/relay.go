package mirror

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/sideband"
	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"lore-mirror/internal/observe"
)

const DefaultMaxPushBytes int64 = 7 << 28 // 1.75 GiB, including update commands.

var ErrPushTooLarge = errors.New("push exceeds configured byte limit")

// Relay streams packs without opening a local Git repository. The caller must
// serialize operations for each destination and provide a positive push limit,
// a deadline, and an HTTP client that rejects redirects.
type Relay struct {
	Client       *http.Client
	MaxPushBytes int64
	metrics      *observe.Telemetry
	health       *observe.Health
}

// A relaySession keeps the verified destination state between pushes in one sync.
type relaySession struct {
	relay           *Relay
	upload          transport.UploadPackSession
	receive         transport.ReceivePackSession
	source          *packp.AdvRefs
	destination     *packp.AdvRefs
	destinationRefs map[string]plumbing.Hash
	destinationURL  string
	token           string
}

type SyncResult struct {
	Changed  bool
	RefCount int
	Bytes    int64
}

// Sync mirrors one source advertisement, leaving GitHub's pull-request refs alone.
// currentDefault comes from GitHub. Sync calls setDefault once the replacement
// branch exists and before deleting the old default.
func (r *Relay) Sync(ctx context.Context, sourceURL, destinationURL, token, currentDefault string, setDefault func(string) error) (result SyncResult, err error) {
	// go-git can include the authenticated receiver's response body in errors.
	defer func() { err = redactGitError(err, token) }()
	source, err := relayEndpoint(sourceURL)
	if err != nil {
		return result, fmt.Errorf("source: %w", err)
	}
	destination, err := relayEndpoint(destinationURL)
	if err != nil {
		return result, fmt.Errorf("destination: %w", err)
	}
	gitClient := githttp.NewClientWithOptions(r.Client, &githttp.ClientOptions{RedirectPolicy: githttp.NoFollowRedirects})
	up, err := gitClient.NewUploadPackSession(source, nil)
	if err != nil {
		return result, err
	}
	defer func() { _ = up.Close() }()
	auth := &githttp.BasicAuth{Username: "x-access-token", Password: token}
	down, err := gitClient.NewReceivePackSession(destination, auth)
	if err != nil {
		return result, err
	}
	defer func() { _ = down.Close() }()
	src, sourceRefs, err := r.advertisedRefs(ctx, up, true)
	if err != nil {
		return result, fmt.Errorf("advertise source: %w", err)
	}
	dst, destinationRefs, err := r.advertisedRefs(ctx, down, false)
	if err != nil {
		return result, fmt.Errorf("advertise destination: %w", err)
	}
	result.RefCount = len(sourceRefs)
	defaultBranch := relayDefaultBranch(src, sourceRefs)
	session := relaySession{
		relay: r, upload: up, receive: down,
		source: src, destination: dst, destinationRefs: destinationRefs,
		destinationURL: destination.String(), token: token,
	}
	changingDefault := defaultBranch != "" && defaultBranch != currentDefault
	oldRef := "refs/heads/" + currentDefault
	deletingCurrentDefault := destinationRefs[oldRef] != plumbing.ZeroHash && sourceRefs[oldRef] == plumbing.ZeroHash
	firstTarget := sourceRefs
	staged := changingDefault && deletingCurrentDefault
	if staged {
		// GitHub rejects deletion of its current default. Publish only the replacement first.
		firstTarget = maps.Clone(destinationRefs)
		newRef := "refs/heads/" + defaultBranch
		firstTarget[newRef] = sourceRefs[newRef]
	}
	if err := session.reconcile(ctx, firstTarget, &result); err != nil {
		return result, err
	}
	if changingDefault {
		if err := setDefault(defaultBranch); err != nil {
			return result, fmt.Errorf("set default branch to %q: %w", defaultBranch, err)
		}
		r.health.SyncProgress()
	}
	if staged {
		err = session.reconcile(ctx, sourceRefs, &result)
	}
	return result, err
}

func (s *relaySession) reconcile(ctx context.Context, want map[string]plumbing.Hash, result *SyncResult) error {
	if maps.Equal(want, s.destinationRefs) {
		return nil
	}
	written, err := s.push(ctx, relayCommands(want, s.destinationRefs))
	result.Bytes += written
	if err != nil {
		return err
	}
	s.relay.health.SyncProgress()
	dst, refs, err := s.relay.advertisedRefs(ctx, s.receive, false)
	if err != nil {
		return fmt.Errorf("verify destination: %w", err)
	}
	if !maps.Equal(want, refs) {
		return errors.New("destination refs differ after push; retry from fresh advertisements")
	}
	s.destination, s.destinationRefs = dst, refs
	result.Changed = true
	return nil
}

func (s *relaySession) push(ctx context.Context, commands []*packp.Command) (int64, error) {
	if !s.destination.Capabilities.Supports(capability.ReportStatus) {
		return 0, errors.New("destination does not support report-status")
	}
	update := packp.NewReferenceUpdateRequest()
	update.Commands = commands
	_ = update.Capabilities.Set(capability.ReportStatus)
	if s.destination.Capabilities.Supports(capability.Sideband64k) {
		_ = update.Capabilities.Set(capability.Sideband64k)
	}
	if s.destination.Capabilities.Supports(capability.Atomic) {
		_ = update.Capabilities.Set(capability.Atomic)
	}

	fetch := packp.NewUploadPackRequest() // Full history; no thin packs or multi-ACK.
	seen := make(map[plumbing.Hash]bool)
	for name, hash := range s.destination.References {
		seen[hash] = true
		if strings.HasPrefix(name, "refs/heads/") && hash != plumbing.ZeroHash {
			fetch.Haves = append(fetch.Haves, hash)
		}
	}
	needsPack := false
	hasDeletes := false
	for _, command := range commands {
		if command.New == plumbing.ZeroHash {
			hasDeletes = true
			continue
		}
		needsPack = true
		if !seen[command.New] {
			fetch.Wants = append(fetch.Wants, command.New)
			seen[command.New] = true
		}
	}
	if hasDeletes {
		if !s.destination.Capabilities.Supports(capability.DeleteRefs) {
			return 0, errors.New("destination does not support delete-refs")
		}
		_ = update.Capabilities.Set(capability.DeleteRefs)
	}
	if len(fetch.Wants) > 0 {
		if s.source.Capabilities.Supports(capability.Sideband64k) {
			_ = fetch.Capabilities.Set(capability.Sideband64k)
		}
		if s.source.Capabilities.Supports(capability.OFSDelta) && s.destination.Capabilities.Supports(capability.OFSDelta) {
			_ = fetch.Capabilities.Set(capability.OFSDelta)
			_ = update.Capabilities.Set(capability.OFSDelta)
		}
		var err error
		update.Packfile, err = s.fetchPack(ctx, fetch)
		if err != nil {
			return 0, err
		}
	} else if needsPack {
		// Receive-pack requires a valid pack even when all objects already exist.
		update.Packfile = io.NopCloser(bytes.NewReader(relayEmptyPack()))
	}
	return s.relay.receivePack(ctx, s.destinationURL, s.token, update)
}

func (s *relaySession) fetchPack(ctx context.Context, fetch *packp.UploadPackRequest) (io.ReadCloser, error) {
	response, err := s.upload.UploadPack(ctx, fetch)
	if err != nil {
		// go-git's UnexpectedError does not implement Unwrap.
		if unexpected, ok := err.(*plumbing.UnexpectedError); ok {
			err = unexpected.Err
		}
		return nil, fmt.Errorf("upload-pack: %w", err)
	}
	s.relay.health.SyncProgress()
	var pack io.Reader = response
	if fetch.Capabilities.Supports(capability.Sideband64k) {
		demux := sideband.NewDemuxer(sideband.Sideband64k, response)
		demux.Progress = relayProgress{s.relay.health}
		pack = demux
	}
	return &relayCountingReader{Reader: pack, Closer: response, metrics: s.relay.metrics, health: s.relay.health}, nil
}

func relayEndpoint(raw string) (*transport.Endpoint, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Host == "" || u.Scheme != "https" {
		return nil, errors.New("git endpoint must be an absolute HTTPS URL")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("git endpoint must not contain credentials, a query, or a fragment")
	}
	return transport.NewEndpoint(strings.TrimRight(raw, "/"))
}

func (r *Relay) advertisedRefs(ctx context.Context, session transport.Session, source bool) (*packp.AdvRefs, map[string]plumbing.Hash, error) {
	adv, err := session.AdvertisedReferencesContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	r.health.SyncProgress()
	for _, format := range adv.Capabilities.Get(capability.ObjectFormat) {
		if format != "sha1" {
			return nil, nil, fmt.Errorf("unsupported Git object format %q", format)
		}
	}
	if len(adv.Shallows) != 0 {
		return nil, nil, errors.New("shallow repositories cannot provide a complete mirror")
	}
	refs := make(map[string]plumbing.Hash)
	for name, hash := range adv.References {
		if !strings.HasPrefix(name, "refs/") || strings.HasSuffix(name, "^{}") {
			continue
		}
		if strings.HasPrefix(name, "refs/pull/") {
			if source {
				return nil, nil, fmt.Errorf("source ref %q occupies GitHub's reserved namespace", name)
			}
			continue
		}
		if err := plumbing.ReferenceName(name).Validate(); err != nil || hash == plumbing.ZeroHash {
			return nil, nil, fmt.Errorf("invalid advertised ref %q", name)
		}
		refs[name] = hash
	}
	return adv, refs, nil
}

func relayCommands(source, destination map[string]plumbing.Hash) []*packp.Command {
	var commands []*packp.Command
	for name, tip := range source {
		if old := destination[name]; old != tip {
			commands = append(commands, &packp.Command{Name: plumbing.ReferenceName(name), Old: old, New: tip})
		}
	}
	for name, old := range destination {
		if _, exists := source[name]; !exists {
			commands = append(commands, &packp.Command{Name: plumbing.ReferenceName(name), Old: old})
		}
	}
	slices.SortFunc(commands, func(a, b *packp.Command) int { return cmp.Compare(a.Name, b.Name) })
	return commands
}

func relayDefaultBranch(adv *packp.AdvRefs, refs map[string]plumbing.Hash) string {
	for _, symref := range adv.Capabilities.Get(capability.SymRef) {
		if branch, ok := strings.CutPrefix(symref, "HEAD:refs/heads/"); ok && refs["refs/heads/"+branch] != plumbing.ZeroHash {
			return branch
		}
	}
	return ""
}

type relayCountingReader struct {
	io.Reader
	io.Closer
	metrics *observe.Telemetry
	health  *observe.Health
}

type relayProgress struct{ health *observe.Health }

func (p relayProgress) Write(b []byte) (int, error) {
	p.health.SyncProgress()
	return len(b), nil
}

func (r *relayCountingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.metrics.BytesReceived(int64(n))
	if n > 0 {
		r.health.SyncProgress()
	}
	return n, err
}

func relayEmptyPack() []byte {
	pack := []byte{'P', 'A', 'C', 'K', 0, 0, 0, 2, 0, 0, 0, 0}
	sum := sha1.Sum(pack)
	return append(pack, sum[:]...)
}
