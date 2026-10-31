package lore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/mail"

	"github.com/go-git/go-git/v5/plumbing/format/config"

	"lore-mirror/internal/retryafter"
)

// FetchAddress reads the primary posting address from an inbox's public config.
// An archive without a configured address has no posting address to display.
func FetchAddress(ctx context.Context, client *http.Client, sourcePath string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, Origin+"/"+InboxName(sourcePath)+"/_/text/config/raw", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "lore-mirror")
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch inbox config: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("fetch inbox config: HTTP %s", resp.Status)
		if delay := retryafter.Delay(resp, 0); delay > 0 {
			err = retryafter.Wrap(err, delay)
		}
		return "", err
	}
	reader := &io.LimitedReader{R: resp.Body, N: (64 << 10) + 1}
	parsed := config.New()
	err = config.NewDecoder(reader).Decode(parsed)
	if reader.N == 0 {
		return "", errors.New("inbox config exceeds 64 KiB")
	}
	if err != nil {
		return "", fmt.Errorf("decode inbox config: %w", err)
	}
	inboxes := parsed.Section("publicinbox").Subsections
	if len(inboxes) == 0 {
		return "", nil
	}
	if len(inboxes) != 1 {
		return "", errors.New("inbox config contains multiple inboxes")
	}
	for _, option := range inboxes[0].Options {
		if !option.IsKey("address") {
			continue
		}
		address, err := mail.ParseAddress(option.Value)
		if err != nil {
			return "", fmt.Errorf("invalid primary inbox address: %w", err)
		}
		return address.Address, nil
	}
	return "", nil
}
