package mirror

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/google/go-cmp/cmp"
)

func TestCheckpointsWalkBackWithoutRequestingPacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tip := plumbing.NewHash(strings.Repeat("1", 40))
		middle := plumbing.NewHash(strings.Repeat("2", 40))
		oldest := plumbing.NewHash(strings.Repeat("3", 40))
		requests := []struct {
			want     plumbing.Hash
			response string
			at       time.Duration
		}{
			{tip, "0035shallow " + middle.String() + "\n0000", 5 * time.Second},
			{middle, "0035shallow " + oldest.String() + "\n0000", 10 * time.Second},
			{oldest, "0000", 15 * time.Second},
		}
		started, calls := time.Now(), 0
		server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls >= len(requests) {
				t.Error("unexpected extra negotiation")
				http.Error(w, "unexpected request", http.StatusBadRequest)
				return
			}
			request := requests[calls]
			calls++
			body, err := io.ReadAll(r.Body)
			want := fmt.Sprintf("003awant %s shallow\n0011deepen 10001\n0000", request.want)
			if err != nil || string(body) != want {
				t.Errorf("negotiation body = %q, error %v; want %q", body, err, want)
			}
			if r.Method != http.MethodPost || r.URL.Path != "/source.git/git-upload-pack" ||
				r.Header.Get("Content-Type") != "application/x-git-upload-pack-request" || r.Header.Get("Authorization") != "" {
				t.Error("unexpected source request method, path, or headers")
			}
			if elapsed := time.Since(started); elapsed != request.at {
				t.Errorf("negotiation at %s, want %s", elapsed, request.at)
			}
			w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
			if _, err := io.WriteString(w, request.response); err != nil {
				t.Error(err)
			}
		}))
		relay := Relay{Client: relayTestClient(server)}
		got, err := relay.checkpoints(t.Context(), "https://example.com/source.git", tip)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff([]plumbing.Hash{oldest, middle, tip}, got); diff != "" {
			t.Fatalf("checkpoint plan (-want +got):\n%s", diff)
		}
	})
}

func TestCheckpointsRefuseAmbiguousOrIncompleteResponses(t *testing.T) {
	tip := plumbing.NewHash(strings.Repeat("1", 40))
	boundary := "0035shallow " + strings.Repeat("2", 40) + "\n"
	for _, tc := range []struct {
		name, body, wantError string
	}{
		{"multiple", boundary + "0035shallow " + strings.Repeat("3", 40) + "\n0000", "multiple checkpoint boundaries"},
		{"cycle", "0035shallow " + tip.String() + "\n0000", "checkpoint boundary repeated"},
		{"truncated", boundary, "missing flush"},
		{"invalid hash", "0035shallow " + strings.Repeat("x", 40) + "\n0000", "invalid checkpoint boundary"},
		{"extra data", "0000PACK", "trailing checkpoint response data"},
		{"oversized", strings.Repeat("x", 65537), "exceeds 64 KiB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
					if _, err := io.WriteString(w, tc.body); err != nil {
						t.Error(err)
					}
				}))
				relay := Relay{Client: relayTestClient(server)}
				if plan, err := relay.checkpoints(t.Context(), "https://example.com/source.git", tip); err == nil || !strings.Contains(err.Error(), tc.wantError) || plan != nil {
					t.Fatalf("plan = %v, error %v; want %q", plan, err, tc.wantError)
				}
			})
		})
	}
}

func TestCheckpointsCancellationStopsPacingAndLiveRequests(t *testing.T) {
	for _, duringRequest := range []bool{false, true} {
		t.Run(fmt.Sprintf("during_request_%t", duringRequest), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := make(chan struct{})
				server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if !duringRequest {
						t.Error("canceled pacing issued a request")
					}
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						t.Error(err)
					}
					close(started)
					<-r.Context().Done()
				}))
				relay := Relay{Client: relayTestClient(server)}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var err error
				go func() {
					_, err = relay.checkpoints(ctx, "https://example.com/source.git", plumbing.NewHash(strings.Repeat("1", 40)))
				}()
				if duringRequest {
					<-started
				} else {
					synctest.Wait()
				}
				cancel()
				synctest.Wait()
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation error = %v", err)
				}
			})
		})
	}
}
