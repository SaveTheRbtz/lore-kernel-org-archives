package mirror

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	gh "lore-mirror/internal/github"
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
	git.Transport = gitTransport{client.Transport}
	return &git
}

type gitTransport struct{ http.RoundTripper }

func (t gitTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.RoundTripper.RoundTrip(request)
	if err == nil && response.StatusCode >= http.StatusMultipleChoices {
		if delay := gitRetryAfter(response); delay > 0 {
			// go-git drops response headers on some errors, including HTTP 403.
			// Do not retain or log an error body that might echo credentials.
			_ = response.Body.Close()
			return nil, gh.WithRetryAfter(fmt.Errorf("git HTTP %d", response.StatusCode), delay)
		}
	}
	return response, err
}

func gitRetryAfter(response *http.Response) time.Duration {
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
