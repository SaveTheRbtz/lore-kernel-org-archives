package github

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testSecretVersion = "projects/kernelorg-mirror/secrets/github-app/versions/latest"

func secretPayload(data []byte) []byte {
	b, _ := json.Marshal(map[string]any{"payload": map[string]any{
		"data":       data,
		"dataCrc32c": fmt.Sprint(crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))),
	}})
	return b
}

func TestSecretReaderUsesVMIdentity(t *testing.T) {
	app := githubAppSecret{AppID: "123", InstallationID: 456, PrivateKey: "PEM key"}
	data, _ := json.Marshal(app)
	for _, version := range []string{testSecretVersion, "projects/508414901212/secrets/github-app/versions/1"} {
		t.Run(version, func(t *testing.T) {
			reader := newTestSecretReader(t, func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/metadata":
					if req.Header.Get("Metadata-Flavor") != "Google" {
						t.Error("missing metadata header")
					}
					if req.Header.Get("Authorization") != "" {
						t.Error("credentials sent to metadata endpoint")
					}
					w.Header().Set("Metadata-Flavor", "Google")
					_, _ = fmt.Fprint(w, `{"access_token":"vm-access-token"}`)
				case "/v1/" + version + ":access":
					if req.Header.Get("Authorization") != "Bearer vm-access-token" {
						t.Error("missing VM access token")
					}
					_, _ = w.Write(secretPayload(data))
				default:
					t.Errorf("unexpected path %s", req.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			})
			got, err := reader.read(t.Context(), version)
			if err != nil {
				t.Fatal(err)
			}
			if got != app {
				t.Fatal("read secret differs from the stored GitHub App credentials")
			}
		})
	}
}

func TestSecretReaderRejectsInvalidResourcesBeforeRequest(t *testing.T) {
	for _, name := range []string{
		"", "https://example.com/secret", "projects/kernelorg-mirror/secrets/key/versions/0",
		"projects/../secrets/key/versions/latest", "projects/kernelorg-mirror/secrets/key/versions/latest?other",
		"projects/kernelorg-mirror/secrets/key%2fother/versions/latest",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := (secretReader{}).read(context.Background(), name); err == nil {
				t.Fatal("invalid resource accepted")
			}
		})
	}
}

func TestSecretReaderFailuresDoNotExposeSecrets(t *testing.T) {
	valid := []byte(`{"app_id":"123","installation_id":456,"private_key":"very-secret-key"}`)
	token := []byte(`{"access_token":"secret-token"}`)
	payload := secretPayload(valid)
	badChecksum := []byte(fmt.Sprintf(`{"payload":{"data":%q,"dataCrc32c":"0"}}`, base64.StdEncoding.EncodeToString(valid)))
	tests := []struct {
		name     string
		metadata []byte
		flavor   string
		status   int
		payload  []byte
	}{
		{"metadata missing flavor", token, "", 200, payload},
		{"metadata empty token", []byte(`{"access_token":""}`), "Google", 200, payload},
		{"metadata too large", paddedJSON(token, 16<<10), "Google", 200, payload},
		{"secret denied", token, "Google", 403, []byte("very-secret-key secret-token")},
		{"secret too large", token, "Google", 200, paddedJSON(payload, 128<<10)},
		{"checksum mismatch", token, "Google", 200, badChecksum},
		{"incomplete app", token, "Google", 200, secretPayload([]byte(`{"private_key":"very-secret-key"}`))},
		{"decoded too large", token, "Google", 200, secretPayload(paddedJSON(valid, 64<<10))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newTestSecretReader(t, func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/metadata" {
					w.Header().Set("Metadata-Flavor", tt.flavor)
					_, _ = w.Write(tt.metadata)
					return
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write(tt.payload)
			})
			_, err := reader.read(context.Background(), testSecretVersion)
			if err == nil {
				t.Fatal("invalid credentials accepted")
			}
			if strings.Contains(err.Error(), "very-secret-key") || strings.Contains(err.Error(), "secret-token") {
				t.Fatal("error exposed credentials")
			}
		})
	}
}

func TestSecretReaderCancelsBlockedRequest(t *testing.T) {
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	reader := newTestSecretReader(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/metadata" {
			w.Header().Set("Metadata-Flavor", "Google")
			_, _ = fmt.Fprint(w, `{"access_token":"vm-access-token"}`)
			return
		}
		close(started)
		select {
		case <-req.Context().Done():
			close(canceled)
		case <-release:
		}
	})
	// Release a broken client's request before the server cleanup waits for it.
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := reader.read(ctx, testSecretVersion); done <- err }()
	timeout := time.After(5 * time.Second)
	select {
	case <-started:
	case <-timeout:
		t.Fatal("secret request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked request returned %v, want context.Canceled", err)
		}
	case <-timeout:
		t.Fatal("reader did not return after cancellation")
	}
	select {
	case <-canceled:
	case <-timeout:
		t.Fatal("server request remained active after cancellation")
	}
}

func newTestSecretReader(t *testing.T, handler http.HandlerFunc) secretReader {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return secretReader{server.Client(), server.URL + "/metadata", server.URL + "/v1/"}
}

func paddedJSON(data []byte, padding int) []byte {
	// Truncating this padding leaves valid JSON, so only the size check rejects it.
	return []byte(string(data) + strings.Repeat(" ", padding))
}
