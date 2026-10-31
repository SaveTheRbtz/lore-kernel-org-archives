package mirror

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"

	gh "lore-mirror/internal/github"
	"lore-mirror/internal/observe"
)

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

func (r *Relay) receivePack(ctx context.Context, destination, token string, update *packp.ReferenceUpdateRequest) (int64, error) {
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
		limited := &relayLimitedWriter{w: writer, limit: r.MaxPushBytes, metrics: r.metrics, health: r.health}
		err := update.Encode(limited)
		writer.CloseWithError(err)
		done <- encodedResult{limited.n, err}
	}()
	response, receiveErr := r.Client.Do(request)
	if receiveErr == nil {
		if response.StatusCode != http.StatusOK {
			message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			text := strings.TrimSpace(string(message))
			if token != "" {
				text = strings.ReplaceAll(text, token, "[redacted]")
			}
			receiveErr = fmt.Errorf("receive-pack HTTP %d: %s", response.StatusCode, text)
			if delay := receiveRetryAfter(response); delay > 0 {
				receiveErr = gh.WithRetryAfter(receiveErr, delay)
			}
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

func receiveRetryAfter(response *http.Response) time.Duration {
	value := response.Header.Get("Retry-After")
	var delay time.Duration
	if seconds, err := strconv.ParseUint(value, 10, 32); err == nil {
		delay = time.Duration(seconds) * time.Second
	} else if until, err := http.ParseTime(value); err == nil {
		delay = time.Until(until)
	}
	if response.StatusCode == http.StatusTooManyRequests && delay <= 0 {
		delay = time.Minute
	}
	return delay
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
