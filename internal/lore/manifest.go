package lore

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
)

const (
	Origin                = "https://lore.kernel.org"
	maxManifestCompressed = 8 << 20
	maxManifestExpanded   = 32 << 20
)

type ManifestEntry struct {
	Fingerprint string `json:"fingerprint"`
	Modified    int64  `json:"modified"`
	Description string `json:"description"`
	Epochs      int    `json:"epochs,omitempty"`
}

type Manifest map[string]ManifestEntry

// Summary adds the posting address and the zero-based epoch's inventory count.
func (e ManifestEntry) Summary(sourcePath, address string) string {
	_, epoch, numeric := strings.Cut(sourcePath[1:], "/git/")
	description := strings.TrimSpace(e.Description)
	if numeric {
		epoch = strings.TrimSuffix(epoch, ".git")
		description = strings.TrimSpace(strings.TrimSuffix(description, "[epoch "+epoch+"]"))
	}
	if address != "" {
		description += " <" + address + ">"
	}
	if numeric {
		description += fmt.Sprintf(" (%s out of %d)", epoch, e.Epochs)
	}
	return strings.TrimSpace(description)
}

// FetchManifest retrieves one complete inventory. A bad entry invalidates the
// entire response, so a partial inventory never replaces the last good one.
func FetchManifest(ctx context.Context, client *http.Client) (Manifest, error) {
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
	compressed, err := readManifestBytes(resp.Body, maxManifestCompressed)
	if err != nil {
		return nil, fmt.Errorf("read compressed manifest: %w", err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("open gzip manifest: %w", err)
	}
	defer func() { _ = zr.Close() }()
	expanded, err := readManifestBytes(zr, maxManifestExpanded)
	if err != nil {
		return nil, fmt.Errorf("expand manifest: %w", err)
	}
	return decodeManifest(expanded)
}

func readManifestBytes(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("exceeds %d-byte limit", limit)
	}
	return b, nil
}

func decodeManifest(b []byte) (Manifest, error) {
	var raw map[string]struct {
		Fingerprint *string `json:"fingerprint"`
		Modified    *int64  `json:"modified"`
		Description string  `json:"description"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("manifest is empty")
	}
	manifest := make(Manifest, len(raw))
	names := make(map[string]string, len(raw))
	epochs := make(map[string]int)
	for path, entry := range raw {
		name, err := RepositoryName(path)
		if err != nil {
			return nil, fmt.Errorf("manifest repository: %w", err)
		}
		if previous, exists := names[name]; exists {
			return nil, fmt.Errorf("repository name collision: %q and %q map to %q", previous, path, name)
		}
		names[name] = path
		if entry.Fingerprint == nil || len(*entry.Fingerprint) != 40 {
			return nil, fmt.Errorf("manifest entry %q has no valid fingerprint", path)
		}
		if _, err := hex.DecodeString(*entry.Fingerprint); err != nil {
			return nil, fmt.Errorf("manifest entry %q has no valid fingerprint", path)
		}
		if entry.Modified == nil || *entry.Modified < 0 {
			return nil, fmt.Errorf("manifest entry %q has no valid modified timestamp", path)
		}
		manifest[path] = ManifestEntry{Fingerprint: *entry.Fingerprint, Modified: *entry.Modified, Description: entry.Description}
		if list, _, numeric := strings.Cut(path[1:], "/git/"); numeric {
			epochs[list]++
		}
	}
	for path, entry := range manifest {
		if list, _, numeric := strings.Cut(path[1:], "/git/"); numeric {
			entry.Epochs = epochs[list]
			manifest[path] = entry
		}
	}
	return manifest, nil
}

var sourcePathPattern = regexp.MustCompile(`^/([A-Za-z0-9][A-Za-z0-9._-]*)(?:/git/(0|[1-9][0-9]*)\.git)?$`)

// RepositoryName reserves a deterministic, case-insensitive GitHub namespace.
func RepositoryName(sourcePath string) (string, error) {
	parts := sourcePathPattern.FindStringSubmatch(sourcePath)
	if parts == nil {
		return "", fmt.Errorf("unsupported source repository path %q", sourcePath)
	}
	epoch := parts[2]
	if epoch == "" {
		epoch = "v1"
	}
	name := strings.ToLower(parts[1]) + "." + epoch
	if len(name) > 100 {
		return "", fmt.Errorf("source repository path %q produces a GitHub name longer than 100 bytes", sourcePath)
	}
	return name, nil
}
