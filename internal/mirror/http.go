package mirror

import (
	"cmp"
	"fmt"
	"net/http"
	"time"

	"lore-mirror/internal/retryafter"
)

func newHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 4
	transport.ResponseHeaderTimeout = time.Minute
	return &http.Client{
		Transport: transport,
		// No request, especially an authenticated push, may change origin.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// Keep Git throttling separate from the GitHub SDK's own retry handling.
func newGitHTTPClient(client *http.Client) *http.Client {
	git := *client
	git.Transport = gitTransport{cmp.Or(client.Transport, http.DefaultTransport)}
	return &git
}

type gitTransport struct{ http.RoundTripper }

func (t gitTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.RoundTripper.RoundTrip(request)
	if err == nil && response.StatusCode >= http.StatusMultipleChoices {
		if delay := retryafter.Delay(response, 0); delay > 0 {
			// go-git drops response headers on some errors, including HTTP 403.
			// Do not retain or log an error body that might echo credentials.
			_ = response.Body.Close()
			return nil, retryafter.Wrap(fmt.Errorf("git HTTP %d", response.StatusCode), delay)
		}
	}
	return response, err
}
