package mirror

import (
	"bytes"
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

type SyncResult struct {
	Changed       bool
	SourceRefs    map[string]plumbing.Hash
	DefaultBranch string
	Bytes         int64
}

// Sync makes all user-managed destination refs match one source advertisement.
// Source corrections and ref deletions are authoritative; GitHub's pull-request
// refs are reserved and are never updated or deleted.
func (r *Relay) Sync(ctx context.Context, sourceURL, destinationURL, token string) (result SyncResult, err error) {
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
	src, err := up.AdvertisedReferencesContext(ctx)
	if err != nil {
		return result, fmt.Errorf("advertise source: %w", err)
	}
	r.health.Progress("sync")
	dst, err := down.AdvertisedReferencesContext(ctx)
	if err != nil {
		return result, fmt.Errorf("advertise destination: %w", err)
	}
	r.health.Progress("sync")
	if err := relayCheckFormat(src); err != nil {
		return result, fmt.Errorf("source: %w", err)
	}
	if err := relayCheckFormat(dst); err != nil {
		return result, fmt.Errorf("destination: %w", err)
	}
	result.SourceRefs, err = relayRefs(src, true)
	if err != nil {
		return result, err
	}
	destinationRefs, err := relayRefs(dst, false)
	if err != nil {
		return result, err
	}
	result.DefaultBranch = relayDefaultBranch(src, result.SourceRefs)
	if maps.Equal(result.SourceRefs, destinationRefs) {
		return result, nil
	}
	if !dst.Capabilities.Supports(capability.ReportStatus) {
		return result, errors.New("destination does not support report-status")
	}
	commands := relayCommands(result.SourceRefs, destinationRefs)
	update := packp.NewReferenceUpdateRequest()
	update.Commands = commands
	_ = update.Capabilities.Set(capability.ReportStatus)
	if dst.Capabilities.Supports(capability.Sideband64k) {
		_ = update.Capabilities.Set(capability.Sideband64k)
	}
	if dst.Capabilities.Supports(capability.Atomic) {
		_ = update.Capabilities.Set(capability.Atomic)
	}
	for _, command := range commands {
		if command.Action() == packp.Delete {
			if !dst.Capabilities.Supports(capability.DeleteRefs) {
				return result, errors.New("destination does not support delete-refs")
			}
			_ = update.Capabilities.Set(capability.DeleteRefs)
		}
	}

	fetch := packp.NewUploadPackRequest() // Full history; no thin packs or multi-ACK.
	known := make(map[plumbing.Hash]bool)
	for name, hash := range dst.References {
		known[hash] = true
		if strings.HasPrefix(name, "refs/heads/") && hash != plumbing.ZeroHash {
			fetch.Haves = append(fetch.Haves, hash)
		}
	}
	needsPack := false
	wanted := make(map[plumbing.Hash]bool)
	for _, command := range commands {
		if command.New == plumbing.ZeroHash {
			continue
		}
		needsPack = true
		if !known[command.New] && !wanted[command.New] {
			fetch.Wants = append(fetch.Wants, command.New)
			wanted[command.New] = true
		}
	}
	if len(fetch.Wants) > 0 {
		if src.Capabilities.Supports(capability.Sideband64k) {
			_ = fetch.Capabilities.Set(capability.Sideband64k)
		}
		if src.Capabilities.Supports(capability.OFSDelta) && dst.Capabilities.Supports(capability.OFSDelta) {
			_ = fetch.Capabilities.Set(capability.OFSDelta)
			_ = update.Capabilities.Set(capability.OFSDelta)
		}
		response, fetchErr := up.UploadPack(ctx, fetch)
		if fetchErr != nil {
			// go-git's UnexpectedError does not implement Unwrap.
			if unexpected, ok := fetchErr.(*plumbing.UnexpectedError); ok {
				fetchErr = unexpected.Err
			}
			return result, fmt.Errorf("upload-pack: %w", fetchErr)
		}
		r.health.Progress("sync")
		defer func() { _ = response.Close() }()
		var pack io.Reader = response
		if fetch.Capabilities.Supports(capability.Sideband64k) {
			demux := sideband.NewDemuxer(sideband.Sideband64k, response)
			demux.Progress = relayProgress{r.health}
			pack = demux
		}
		update.Packfile = &relayCountingReader{Reader: pack, Closer: response, metrics: r.metrics, health: r.health}
	} else if needsPack {
		// Receive-pack requires a valid pack even when all objects already exist.
		update.Packfile = io.NopCloser(bytes.NewReader(relayEmptyPack()))
	}
	result.Bytes, err = r.receivePack(ctx, destination.String(), token, update)
	if err != nil {
		return result, err
	}
	r.health.Progress("sync")
	verified, err := down.AdvertisedReferencesContext(ctx)
	if err != nil {
		return result, fmt.Errorf("verify destination: %w", err)
	}
	r.health.Progress("sync")
	verifiedRefs, err := relayRefs(verified, false)
	if err != nil {
		return result, err
	}
	if !maps.Equal(result.SourceRefs, verifiedRefs) {
		return result, errors.New("destination refs differ after push; retry from fresh advertisements")
	}
	result.Changed = true
	return result, nil
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

func relayCheckFormat(adv *packp.AdvRefs) error {
	for _, format := range adv.Capabilities.Get(capability.ObjectFormat) {
		if format != "sha1" {
			return fmt.Errorf("unsupported Git object format %q", format)
		}
	}
	if len(adv.Shallows) != 0 {
		return errors.New("shallow repositories cannot provide a complete mirror")
	}
	return nil
}

func relayRefs(adv *packp.AdvRefs, source bool) (map[string]plumbing.Hash, error) {
	refs := make(map[string]plumbing.Hash)
	for name, hash := range adv.References {
		if !strings.HasPrefix(name, "refs/") || strings.HasSuffix(name, "^{}") {
			continue
		}
		if strings.HasPrefix(name, "refs/pull/") {
			if source {
				return nil, fmt.Errorf("source ref %q occupies GitHub's reserved namespace", name)
			}
			continue
		}
		if err := plumbing.ReferenceName(name).Validate(); err != nil || hash == plumbing.ZeroHash {
			return nil, fmt.Errorf("invalid advertised ref %q", name)
		}
		refs[name] = hash
	}
	return refs, nil
}

func relayCommands(source, destination map[string]plumbing.Hash) []*packp.Command {
	names := make(map[string]bool, len(source)+len(destination))
	for name := range source {
		names[name] = true
	}
	for name := range destination {
		names[name] = true
	}
	var commands []*packp.Command
	for _, name := range slices.Sorted(maps.Keys(names)) {
		if source[name] != destination[name] {
			commands = append(commands, &packp.Command{Name: plumbing.ReferenceName(name), Old: destination[name], New: source[name]})
		}
	}
	return commands
}

func relayDefaultBranch(adv *packp.AdvRefs, refs map[string]plumbing.Hash) string {
	for _, symref := range adv.Capabilities.Get(capability.SymRef) {
		if target, ok := strings.CutPrefix(symref, "HEAD:"); ok && strings.HasPrefix(target, "refs/heads/") {
			if _, exists := refs[target]; exists {
				return strings.TrimPrefix(target, "refs/heads/")
			}
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
	p.health.Progress("sync")
	return len(b), nil
}

func (r *relayCountingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.metrics.AddBytes("receive", int64(n))
	if n > 0 {
		r.health.Progress("sync")
	}
	return n, err
}

func relayEmptyPack() []byte {
	pack := []byte{'P', 'A', 'C', 'K', 0, 0, 0, 2, 0, 0, 0, 0}
	sum := sha1.Sum(pack)
	return append(pack, sum[:]...)
}
