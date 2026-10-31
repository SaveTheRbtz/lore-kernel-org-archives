package mirror

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/sideband"

	"lore-mirror/internal/observe"
)

type relayLimitedWriter struct {
	w       io.Writer
	limit   int64
	n       int64
	metrics *observe.Telemetry
	health  *observe.Health
}

// redactedGitError keeps the original cause for retry and cancellation checks.
type redactedGitError struct {
	message string
	cause   error
}

func (e *redactedGitError) Error() string { return e.message }
func (e *redactedGitError) Unwrap() error { return e.cause }

func redactGitCredentials(message, token string) string {
	if token == "" || message == "" {
		return message
	}
	basic := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + token))
	return strings.NewReplacer(basic, "[redacted]", token, "[redacted]").Replace(message)
}

func redactGitError(err error, token string) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	if safe := redactGitCredentials(message, token); safe != message {
		return &redactedGitError{message: safe, cause: err}
	}
	return err
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
	w.metrics.BytesSent(int64(n))
	if n > 0 {
		w.health.SyncProgress()
	}
	if err == nil && tooLarge {
		err = ErrPushTooLarge
	}
	return n, err
}

func (r *Relay) receivePack(ctx context.Context, destination, token string, update *packp.ReferenceUpdateRequest) (int64, error) {
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader, writer := io.Pipe()
	// HTTP cancellation can wait for its body reader. Unblock that reader
	// before waiting for Do to return and closing the source pack below.
	stop := context.AfterFunc(ctx, func() { _ = reader.CloseWithError(ctx.Err()) })
	defer stop()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, destination+"/git-receive-pack", reader)
	if err != nil {
		_ = reader.Close()
		_ = writer.Close()
		return 0, err
	}
	request.SetBasicAuth("x-access-token", token)
	request.Header.Set("Content-Type", "application/x-git-receive-pack-request")
	request.Header.Set("Accept", "application/x-git-receive-pack-result")
	request.Header.Set("User-Agent", "lore-mirror")
	limited := &relayLimitedWriter{w: writer, limit: r.MaxPushBytes, metrics: r.metrics, health: r.health}
	done := make(chan error, 1)
	go func() {
		err := update.Encode(limited)
		writer.CloseWithError(err)
		done <- err
	}()
	var remote strings.Builder
	response, receiveErr := r.Client.Do(request)
	if receiveErr == nil {
		receiveErr = r.readReceivePackResponse(response, update, &remote)
	}
	// A receiver can reject a request before reading its body. Unblock both the
	// pipe writer and a source HTTP read before joining the encoder goroutine.
	_ = reader.Close()
	cancel()
	if update.Packfile != nil {
		_ = update.Packfile.Close()
	}
	encodeErr := <-done
	cause := errors.Join(receiveErr, encodeErr, ctx.Err())
	if cause == nil {
		return limited.n, nil
	}
	// Redact before truncating, including credentials split across sidebands.
	message := redactGitCredentials(cause.Error(), token)
	detail := redactGitCredentials(strings.TrimSpace(remote.String()), token)
	if len(message) > 4096 {
		message = message[:4096]
	}
	if len(detail) > 4096 {
		detail = detail[len(detail)-4096:]
	}
	if detail != "" {
		message += "\nremote: " + detail
	}
	return limited.n, &redactedGitError{message: message, cause: cause}
}

func (r *Relay) readReceivePackResponse(response *http.Response, update *packp.ReferenceUpdateRequest, remote io.Writer) error {
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		message, readErr := io.ReadAll(io.LimitReader(response.Body, 4097))
		if readErr != nil || len(message) > 4096 {
			// A partial credential cannot be matched by the redactor.
			message = []byte("[body omitted]")
		}
		return fmt.Errorf("receive-pack HTTP %d (protocol=%s, request_id=%s): %s",
			response.StatusCode, response.Proto, response.Header.Get("X-GitHub-Request-Id"), strings.TrimSpace(string(message)))
	}
	contentType, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if contentType != "application/x-git-receive-pack-result" {
		return fmt.Errorf("unexpected receive-pack content type %q", contentType)
	}
	var body io.Reader = io.LimitReader(response.Body, 1<<20)
	if update.Capabilities.Supports(capability.Sideband64k) {
		demux := sideband.NewDemuxer(sideband.Sideband64k, body)
		demux.Progress = io.MultiWriter(relayProgress{r.health}, remote)
		body = demux
	}
	report := packp.NewReportStatus()
	if err := report.Decode(body); err != nil {
		return fmt.Errorf("receive-pack status: %w", err)
	}
	return relayCheckReport(report, update.Commands)
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
