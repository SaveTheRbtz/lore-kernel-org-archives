package lore

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"github.com/go-git/go-git/v5/plumbing/format/config"
)

// FetchAddress reads the primary posting address from an inbox's public config.
// An archive without a configured address has no posting address to display.
func FetchAddress(ctx context.Context, client *http.Client, sourcePath string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	inbox, _, _ := strings.Cut(strings.TrimPrefix(sourcePath, "/"), "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, Origin+"/"+inbox+"/_/text/config/raw", nil)
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
		return "", fmt.Errorf("fetch inbox config: HTTP %s", resp.Status)
	}
	reader := &io.LimitedReader{R: resp.Body, N: (64 << 10) + 1}
	parsed := config.New()
	err = config.NewDecoder(reader).Decode(parsed)
	if reader.N == 0 {
		return "", fmt.Errorf("inbox config exceeds 64 KiB")
	}
	if err != nil {
		return "", fmt.Errorf("decode inbox config: %w", err)
	}
	inboxes := parsed.Section("publicinbox").Subsections
	if len(inboxes) == 0 {
		return "", nil
	}
	if len(inboxes) != 1 {
		return "", fmt.Errorf("inbox config contains multiple inboxes")
	}
	addresses := inboxes[0].OptionAll("address")
	if len(addresses) == 0 {
		return "", nil
	}
	address, err := mail.ParseAddress(addresses[0])
	if err != nil {
		return "", fmt.Errorf("invalid primary inbox address: %w", err)
	}
	return address.Address, nil
}
