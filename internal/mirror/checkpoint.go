package mirror

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
)

const (
	checkpointCommits = 10000
	checkpointDelay   = 5 * time.Second
)

func checkpointWait(ctx context.Context) error {
	timer := time.NewTimer(checkpointDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Only negotiate depth boundaries here. UploadPack appends "done", which would
// ask the source to generate a pack; UploadRequest ends at the first flush.
func (r *Relay) checkpoints(ctx context.Context, sourceURL string, tip plumbing.Hash) ([]plumbing.Hash, error) {
	if tip == plumbing.ZeroHash {
		return nil, errors.New("checkpoint tip is zero")
	}
	plan := []plumbing.Hash{tip}
	seen := map[plumbing.Hash]bool{tip: true}
	for {
		if err := checkpointWait(ctx); err != nil {
			return nil, err
		}
		boundary, err := r.checkpointBoundary(ctx, sourceURL, plan[len(plan)-1])
		if err != nil {
			return nil, err
		}
		if boundary == plumbing.ZeroHash {
			slices.Reverse(plan)
			return plan, nil
		}
		if seen[boundary] {
			return nil, fmt.Errorf("checkpoint boundary repeated: %s", boundary)
		}
		seen[boundary] = true
		plan = append(plan, boundary)
	}
}

func (r *Relay) checkpointBoundary(ctx context.Context, sourceURL string, tip plumbing.Hash) (plumbing.Hash, error) {
	upload := packp.NewUploadRequest()
	upload.Wants = []plumbing.Hash{tip}
	upload.Depth = packp.DepthCommits(checkpointCommits + 1)
	_ = upload.Capabilities.Set(capability.Shallow)
	var body bytes.Buffer
	if err := upload.Encode(&body); err != nil {
		return plumbing.ZeroHash, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, sourceURL+"/git-upload-pack", &body)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	request.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	request.Header.Set("Accept", "application/x-git-upload-pack-result")
	request.Header.Set("User-Agent", "lore-mirror")
	response, err := r.Client.Do(request)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return plumbing.ZeroHash, fmt.Errorf("checkpoint discovery HTTP %d", response.StatusCode)
	}
	if contentType, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type")); contentType != "application/x-git-upload-pack-result" {
		return plumbing.ZeroHash, fmt.Errorf("unexpected checkpoint content type %q", contentType)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil {
		return plumbing.ZeroHash, err
	}
	if len(data) > 64<<10 {
		return plumbing.ZeroHash, errors.New("checkpoint response exceeds 64 KiB")
	}
	reader := bytes.NewReader(data)
	scanner := pktline.NewScanner(reader)
	var boundary plumbing.Hash
	for scanner.Scan() {
		line := string(scanner.Bytes())
		if line == "" {
			if reader.Len() != 0 {
				return plumbing.ZeroHash, errors.New("trailing checkpoint response data")
			}
			r.health.Progress("sync")
			return boundary, nil
		}
		hash, ok := strings.CutPrefix(strings.TrimSuffix(line, "\n"), "shallow ")
		if !ok || !plumbing.IsHash(hash) || plumbing.NewHash(hash) == plumbing.ZeroHash {
			return plumbing.ZeroHash, errors.New("invalid checkpoint boundary")
		}
		if boundary != plumbing.ZeroHash {
			return plumbing.ZeroHash, errors.New("multiple checkpoint boundaries")
		}
		boundary = plumbing.NewHash(hash)
	}
	if err := scanner.Err(); err != nil {
		return plumbing.ZeroHash, fmt.Errorf("checkpoint response: %w", err)
	}
	return plumbing.ZeroHash, errors.New("checkpoint response missing flush")
}
