package lore

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"lore-mirror/internal/retryafter"
)

func TestFetchAddress(t *testing.T) {
	for _, test := range []struct {
		name, sourcePath, configPath, config, want string
		status                                     int
		wantErr                                    bool
	}{
		{
			name: "URL alias uses the first configured address", sourcePath: "/lkml/git/0.git", configPath: "/lkml/_/text/config/raw",
			config: "[publicinbox \"linux-kernel\"]\naddress = linux-kernel@vger.kernel.org\naddress = old-list@example.org\n",
			status: http.StatusOK, want: "linux-kernel@vger.kernel.org",
		},
		{
			name: "archive on another mail host", sourcePath: "/linux-arm-kernel/git/0.git", configPath: "/linux-arm-kernel/_/text/config/raw",
			config: "[publicinbox \"linux-arm-kernel\"]\naddress = linux-arm-kernel@lists.infradead.org\n",
			status: http.StatusOK, want: "linux-arm-kernel@lists.infradead.org",
		},
		{
			name: "archive without a posting address", sourcePath: "/archive/git/0.git", configPath: "/archive/_/text/config/raw",
			config: "[publicinbox \"archive\"]\nlistid = archive.example.org\n", status: http.StatusOK,
		},
		{
			name: "invalid primary does not select a later alias", sourcePath: "/list/git/0.git", configPath: "/list/_/text/config/raw",
			config: "[publicinbox \"list\"]\naddress = invalid\naddress = list@example.org\n", status: http.StatusOK, wantErr: true,
		},
		{
			name: "failed config response", sourcePath: "/list/git/0.git", configPath: "/list/_/text/config/raw",
			config: "[publicinbox \"list\"]\naddress = list@example.org\n", status: http.StatusServiceUnavailable, wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("GET lore.kernel.org"+test.configPath, func(w http.ResponseWriter, r *http.Request) {
				if r.UserAgent() != "lore-mirror" {
					t.Errorf("User-Agent = %q", r.UserAgent())
				}
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.config))
			})
			server := httptest.NewTestServer(t, mux)
			got, err := FetchAddress(t.Context(), server.Client(), test.sourcePath)
			if got != test.want || (err != nil) != test.wantErr {
				t.Fatalf("FetchAddress = %q, %v; want %q, error=%v", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestFetchAddressPreservesRetryAfter(t *testing.T) {
	server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	_, err := FetchAddress(t.Context(), server.Client(), "/lkml/git/0.git")
	if retryafter.FromError(err) != 2*time.Minute {
		t.Fatalf("FetchAddress error = %v; want 2-minute retry delay", err)
	}
}
