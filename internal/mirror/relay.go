package mirror

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
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
// serialize operations for each destination and provide an HTTP client and deadline.
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
	client := r.httpClient()
	gitClient := githttp.NewClientWithOptions(client, &githttp.ClientOptions{RedirectPolicy: githttp.NoFollowRedirects})
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
	commands := relayCommands(result.SourceRefs, destinationRefs)
	if len(commands) == 0 {
		return result, nil
	}
	if !dst.Capabilities.Supports(capability.ReportStatus) {
		return result, errors.New("destination does not support report-status")
	}
	update := packp.NewReferenceUpdateRequest()
	update.Commands = commands
	_ = update.Capabilities.Set(capability.ReportStatus)
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
			return result, fmt.Errorf("upload-pack: %w", fetchErr)
		}
		r.health.Progress("sync")
		defer func() { _ = response.Close() }()
		var pack io.Reader = response
		if fetch.Capabilities.Supports(capability.Sideband64k) {
			pack = sideband.NewDemuxer(sideband.Sideband64k, response)
		}
		update.Packfile = &relayCountingReader{Reader: pack, Closer: response, metrics: r.metrics, health: r.health}
	} else if needsPack {
		// Receive-pack requires a valid pack even when all objects already exist.
		update.Packfile = io.NopCloser(bytes.NewReader(relayEmptyPack()))
	}
	limit := r.MaxPushBytes
	if limit == 0 {
		limit = DefaultMaxPushBytes
	}
	if limit < 0 {
		return result, errors.New("max push bytes must be positive")
	}
	result.Bytes, err = relayReceivePack(ctx, client, destination.String(), token, update, limit, r.metrics, r.health)
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

func (r *Relay) httpClient() *http.Client {
	client := *r.Client
	// A streaming request cannot be replayed, and credentials must not follow
	// redirects. Reconfigure the endpoint explicitly if a repository moves.
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("git HTTP redirects are disabled")
	}
	return &client
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

type relayLimitedWriter struct {
	w       io.Writer
	limit   int64
	n       int64
	metrics *observe.Telemetry
	health  *observe.Health
}

func (w *relayLimitedWriter) Write(p []byte) (int, error) {
	remaining := w.limit - w.n
	if remaining <= 0 && len(p) > 0 {
		return 0, ErrPushTooLarge
	}
	tooLarge := int64(len(p)) > remaining
	if tooLarge {
		p = p[:remaining]
	}
	n, err := w.w.Write(p)
	w.n += int64(n)
	w.metrics.AddBytes("send", int64(n))
	if n > 0 {
		w.health.Progress("sync")
	}
	if err == nil && tooLarge {
		err = ErrPushTooLarge
	}
	return n, err
}

func relayReceivePack(ctx context.Context, client *http.Client, destination, token string, update *packp.ReferenceUpdateRequest, limit int64, metrics *observe.Telemetry, health *observe.Health) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader, writer := io.Pipe()
	// HTTP cancellation can wait for its body reader. Unblock that reader
	// before waiting for Do to return and closing the source pack below.
	stop := context.AfterFunc(ctx, func() { _ = reader.CloseWithError(ctx.Err()) })
	defer stop()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, destination+"/git-receive-pack", reader)
	if err != nil {
		_ = reader.Close()
		_ = writer.Close()
		return 0, err
	}
	request.SetBasicAuth("x-access-token", token)
	request.Header.Set("Content-Type", "application/x-git-receive-pack-request")
	request.Header.Set("Accept", "application/x-git-receive-pack-result")
	request.Header.Set("User-Agent", "lore-mirror")
	type encodedResult struct {
		n   int64
		err error
	}
	done := make(chan encodedResult, 1)
	go func() {
		limited := &relayLimitedWriter{w: writer, limit: limit, metrics: metrics, health: health}
		err := update.Encode(limited)
		writer.CloseWithError(err)
		done <- encodedResult{limited.n, err}
	}()
	response, receiveErr := client.Do(request)
	if receiveErr == nil {
		if response.StatusCode != http.StatusOK {
			message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			text := strings.TrimSpace(string(message))
			if token != "" {
				text = strings.ReplaceAll(text, token, "[redacted]")
			}
			receiveErr = fmt.Errorf("receive-pack HTTP %d: %s", response.StatusCode, text)
		} else if contentType, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type")); contentType != "application/x-git-receive-pack-result" {
			receiveErr = fmt.Errorf("unexpected receive-pack content type %q", contentType)
		} else {
			report := packp.NewReportStatus()
			if err := report.Decode(io.LimitReader(response.Body, 1<<20)); err != nil {
				receiveErr = fmt.Errorf("receive-pack status: %w", err)
			} else {
				receiveErr = relayCheckReport(report, update.Commands)
			}
		}
		_ = response.Body.Close()
	}
	// A receiver can reject a request before reading its body. Unblock both the
	// pipe writer and a source HTTP read before joining the encoder goroutine.
	cancel()
	reader.CloseWithError(io.ErrClosedPipe)
	if update.Packfile != nil {
		_ = update.Packfile.Close()
	}
	encoded := <-done
	return encoded.n, errors.Join(receiveErr, encoded.err)
}

func relayCheckReport(report *packp.ReportStatus, commands []*packp.Command) error {
	if err := report.Error(); err != nil {
		return err
	}
	seen := make(map[plumbing.ReferenceName]bool)
	for _, status := range report.CommandStatuses {
		if seen[status.ReferenceName] {
			return fmt.Errorf("duplicate status for %s", status.ReferenceName)
		}
		seen[status.ReferenceName] = true
	}
	if len(seen) != len(commands) {
		return errors.New("receive-pack did not report every requested ref")
	}
	for _, command := range commands {
		if !seen[command.Name] {
			return fmt.Errorf("receive-pack omitted status for %s", command.Name)
		}
	}
	return nil
}
