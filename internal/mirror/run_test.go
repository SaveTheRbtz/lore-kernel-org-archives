package mirror

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
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

func TestWriteInventoryOrder(t *testing.T) {
	var b bytes.Buffer
	if err := writeInventory(&b, nil); err != nil || b.String() != "[]\n" {
		t.Fatalf("empty inventory %q: %v", b.String(), err)
	}
	b.Reset()
	if err := writeInventory(&b, lore.Manifest{
		"/z/git/0.git": {Modified: 100}, "/a/git/0.git": {Modified: 100}, "/b/git/0.git": {Modified: 3},
	}); err != nil {
		t.Fatal(err)
	}
	var rows []struct{ Source, Repository string }
	if err := json.Unmarshal(b.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, row := range rows {
		names = append(names, row.Repository)
	}
	if !slices.Equal(names, []string{"a.0", "z.0", "b.0"}) {
		t.Fatalf("repository order = %v", names)
	}
}
