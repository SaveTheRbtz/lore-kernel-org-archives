package lore

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
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
}

type Manifest map[string]ManifestEntry

// FetchManifest retrieves one complete inventory. A bad entry invalidates the
// entire response, so a partial inventory never replaces the last good one.
func FetchManifest(ctx context.Context, client *http.Client, sourceURL string) (Manifest, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(sourceURL, "/")+"/manifest.js.gz", nil)
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
	d := json.NewDecoder(bytes.NewReader(b))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return nil, fmt.Errorf("manifest must be a JSON object")
	}
	manifest := make(Manifest)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return nil, fmt.Errorf("read manifest key: %w", err)
		}
		path, ok := key.(string)
		if !ok {
			return nil, fmt.Errorf("manifest repository key is not a string")
		}
		if _, err := RepositoryName(path); err != nil {
			return nil, fmt.Errorf("manifest repository: %w", err)
		}
		if _, exists := manifest[path]; exists {
			return nil, fmt.Errorf("duplicate manifest repository %q", path)
		}
		var entry struct {
			Fingerprint *string `json:"fingerprint"`
			Modified    *int64  `json:"modified"`
			Description string  `json:"description"`
		}
		if err := d.Decode(&entry); err != nil {
			return nil, fmt.Errorf("decode manifest entry %q: %w", path, err)
		}
		if entry.Fingerprint == nil || len(*entry.Fingerprint) != 40 {
			return nil, fmt.Errorf("manifest entry %q has no valid fingerprint", path)
		}
		if _, err := hex.DecodeString(*entry.Fingerprint); err != nil {
			return nil, fmt.Errorf("manifest entry %q has no valid fingerprint", path)
		}
		if entry.Modified == nil || *entry.Modified < 0 {
			return nil, fmt.Errorf("manifest entry %q has no valid modified timestamp", path)
		}
		manifest[path] = ManifestEntry{*entry.Fingerprint, *entry.Modified, entry.Description}
	}
	if _, err := d.Token(); err != nil {
		return nil, fmt.Errorf("close manifest object: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("manifest contains trailing JSON or invalid data")
	}
	if len(manifest) == 0 {
		return nil, fmt.Errorf("manifest is empty")
	}
	return manifest, nil
}

var sourcePathPattern = regexp.MustCompile(`^/([A-Za-z0-9][A-Za-z0-9._-]*)(?:/git/(0|[1-9][0-9]*)\.git)?$`)

// RepositoryName reserves a deterministic GitHub namespace. GitHub names are
// case-insensitive, so callers must reject collisions across the inventory.
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

func SourceRepositoryURL(sourcePath string) (string, error) {
	if _, err := RepositoryName(sourcePath); err != nil {
		return "", err
	}
	return Origin + sourcePath, nil
}
