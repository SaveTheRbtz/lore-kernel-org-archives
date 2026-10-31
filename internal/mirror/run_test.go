package mirror

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"lore-mirror/internal/lore"
)

func TestRepositorySummary(t *testing.T) {
	const section = "[publicinbox \"list\"]\n"
	entry := lore.ManifestEntry{Description: "Mail", Epochs: 1}
	for _, tc := range []struct {
		name   string
		status int
		config string
		want   *string
	}{
		{"missing config", http.StatusNotFound, "", nil},
		{"malformed address", http.StatusOK, section + "address = invalid\n", nil},
		{"no address", http.StatusOK, section + "listid = list.example.org\n", new("Mail (1 out of 1)")},
		{"known address", http.StatusOK, section + "address = list@example.org\n", new("Mail <list@example.org> (1 out of 1)")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.config)
			}))
			got, err := repositorySummary(t.Context(), server.Client(), testLogger(), "/list/git/0.git", entry)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("summary (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRepositorySummaryPropagatesThrottleAndCancellation(t *testing.T) {
	server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	_, err := repositorySummary(t.Context(), server.Client(), testLogger(), "/list/git/0.git", lore.ManifestEntry{})
	if delay := serverRetryAfter(err); delay != 2*time.Minute {
		t.Fatalf("scheduler delay = %s, want 2m; error = %v", delay, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = repositorySummary(ctx, server.Client(), testLogger(), "/list/git/0.git", lore.ManifestEntry{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled lookup = %v, want context.Canceled", err)
	}
}
