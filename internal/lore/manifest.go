package lore

import (
	"compress/gzip"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	fetchTimeout          = 45 * time.Second
	Origin                = "https://lore.kernel.org"
	maxManifestCompressed = 8 << 20
	maxManifestExpanded   = 32 << 20
)

type ManifestEntry struct {
	Fingerprint string `json:"fingerprint"`
	Modified    int64  `json:"modified"`
	Description string `json:"description"`
	Epochs      int    `json:"epochs"`
}

type Manifest map[string]ManifestEntry

// Summary adds the posting address and the epoch's one-based position.
func (e ManifestEntry) Summary(sourcePath, address string) string {
	epoch := strings.TrimSuffix(path.Base(sourcePath), ".git")
	description := strings.TrimSpace(e.Description)
	description = strings.TrimSpace(strings.TrimSuffix(description, "[epoch "+epoch+"]"))
	if address != "" {
		description += " <" + address + ">"
	}
	number, _ := strconv.ParseUint(epoch, 10, 64) // Validated by RepositoryName.
	description += fmt.Sprintf(" (%d out of %d)", number+1, e.Epochs)
	return strings.TrimSpace(description)
}

// FetchManifest skips unsharded archives and rejects malformed inventories.
func FetchManifest(ctx context.Context, client *http.Client, log *slog.Logger, lists map[string]bool, requireAll bool) (Manifest, error) {
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, Origin+"/manifest.js.gz", nil)
	if err != nil {
		return nil, err
	}
	// The resource itself is gzip. Disable transparent HTTP decompression so
	// both the compressed and expanded byte limits have their intended meaning.
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("User-Agent", "lore-mirror")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch manifest: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch manifest: HTTP %s", resp.Status)
	}
	compressed := &io.LimitedReader{R: resp.Body, N: maxManifestCompressed + 1}
	zr, err := gzip.NewReader(compressed)
	if err != nil {
		return nil, fmt.Errorf("open gzip manifest: %w", err)
	}
	defer func() { _ = zr.Close() }()
	expanded := &io.LimitedReader{R: zr, N: maxManifestExpanded + 1}
	var raw map[string]jsontext.Value
	err = json.UnmarshalRead(expanded, &raw)
	if compressed.N == 0 {
		return nil, fmt.Errorf("compressed manifest exceeds %d-byte limit", maxManifestCompressed)
	}
	if expanded.N == 0 {
		return nil, fmt.Errorf("expanded manifest exceeds %d-byte limit", maxManifestExpanded)
	}
	if err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	return manifestFromEntries(raw, log, lists, requireAll)
}

func manifestFromEntries(raw map[string]jsontext.Value, log *slog.Logger, lists map[string]bool, requireAll bool) (Manifest, error) {
	if len(raw) == 0 {
		return nil, errors.New("manifest is empty")
	}
	manifest := make(Manifest, len(raw))
	names := make(map[string]string, len(raw))
	epochs := make(map[string]int)
	for path, data := range raw {
		if unshardedPathPattern.MatchString(path) {
			log.Warn("unsharded_archive_skipped", "source", path)
			continue
		}
		name, err := RepositoryName(path)
		if err != nil {
			return nil, fmt.Errorf("manifest repository: %w", err)
		}
		if previous, exists := names[name]; exists {
			return nil, fmt.Errorf("repository name collision: %q and %q map to %q", previous, path, name)
		}
		names[name] = path
		var entry struct {
			Fingerprint string `json:"fingerprint"`
			Modified    *int64 `json:"modified"`
			Description string `json:"description"`
		}
		if err := json.Unmarshal(data, &entry); err != nil {
			return nil, fmt.Errorf("decode manifest entry %q: %w", path, err)
		}
		if !fingerprintPattern.MatchString(entry.Fingerprint) {
			return nil, fmt.Errorf("manifest entry %q has no valid fingerprint", path)
		}
		if entry.Modified == nil || *entry.Modified < 0 {
			return nil, fmt.Errorf("manifest entry %q has no valid modified timestamp", path)
		}
		inbox := InboxName(path)
		epochs[inbox]++
		// lkml.1 is maintained manually because GitHub rejects the NUL in
		// commit 299dbd50b6995c6debe2275f0df984ce697fb4cc.
		if name == "lkml.1" || (len(lists) != 0 && !lists[inbox]) {
			continue
		}
		manifest[path] = ManifestEntry{Fingerprint: entry.Fingerprint, Modified: *entry.Modified, Description: entry.Description}
	}
	if requireAll {
		for name := range lists {
			if _, unsharded := raw["/"+name]; epochs[name] == 0 && !unsharded {
				return nil, fmt.Errorf("%w: %q", ErrUnknownInbox, name)
			}
		}
	}
	for path, entry := range manifest {
		entry.Epochs = epochs[InboxName(path)]
		manifest[path] = entry
	}
	return manifest, nil
}

const listNamePattern = `[A-Za-z0-9][A-Za-z0-9._-]*`

var (
	fingerprintPattern   = regexp.MustCompile(`^[[:xdigit:]]{40}$`)
	unshardedPathPattern = regexp.MustCompile(`^/` + listNamePattern + `$`)
	sourcePathPattern    = regexp.MustCompile(`^/(` + listNamePattern + `)/git/(0|[1-9][0-9]*)\.git$`)
)

// InboxName returns the case-preserving inbox component of a Lore manifest path.
func InboxName(path string) string {
	name, _, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	return name
}

// RepositoryName reserves a deterministic, case-insensitive GitHub namespace.
func RepositoryName(sourcePath string) (string, error) {
	parts := sourcePathPattern.FindStringSubmatch(sourcePath)
	if parts == nil {
		return "", fmt.Errorf("unsupported source repository path %q", sourcePath)
	}
	epoch := parts[2]
	if number, err := strconv.ParseUint(epoch, 10, 64); err != nil || number == math.MaxUint64 {
		return "", fmt.Errorf("unsupported epoch in source repository path %q", sourcePath)
	}
	name := strings.ToLower(parts[1]) + "." + epoch
	if len(name) > 100 {
		return "", fmt.Errorf("source repository path %q produces a GitHub name longer than 100 bytes", sourcePath)
	}
	return name, nil
}
