package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls = append(calls, req.URL.Path)
		switch req.URL.Path {
		case "/metadata":
			if req.Header.Get("Metadata-Flavor") != "Google" {
				t.Error("missing metadata header")
			}
			if req.Header.Get("Authorization") != "" {
				t.Error("credentials sent to metadata endpoint")
			}
			w.Header().Set("Metadata-Flavor", "Google")
			fmt.Fprint(w, `{"access_token":"vm-access-token"}`)
		case "/v1/" + testSecretVersion + ":access":
			if req.Header.Get("Authorization") != "Bearer vm-access-token" {
				t.Error("missing VM access token")
			}
			w.Write(secretPayload(data))
		default:
			t.Errorf("unexpected path %s", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	reader := secretReader{server.Client(), server.URL + "/metadata", server.URL + "/v1/"}
	got, err := reader.read(context.Background(), testSecretVersion)
	if err != nil {
		t.Fatal(err)
	}
	if got != app || len(calls) != 2 {
		t.Fatalf("unexpected result or calls: %d requests", len(calls))
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
	for _, name := range []string{testSecretVersion, "projects/508414901212/secrets/github-app/versions/1"} {
		if !secretVersionName.MatchString(name) {
			t.Fatalf("valid resource rejected: %s", name)
		}
	}
}

func TestSecretReaderFailuresDoNotExposeSecrets(t *testing.T) {
	valid := []byte(`{"app_id":"123","installation_id":456,"private_key":"very-secret-key"}`)
	tests := []struct {
		name, metadata string
		flavor         string
		status         int
		payload        []byte
	}{
		{"metadata missing flavor", `{"access_token":"secret-token"}`, "", 200, secretPayload(valid)},
		{"metadata malformed", `{"access_token":`, "Google", 200, secretPayload(valid)},
		{"metadata empty token", `{"access_token":""}`, "Google", 200, secretPayload(valid)},
		{"metadata too large", strings.Repeat("x", 16<<10+1), "Google", 200, secretPayload(valid)},
		{"secret denied", `{"access_token":"secret-token"}`, "Google", 403, []byte("very-secret-key secret-token")},
		{"secret missing", `{"access_token":"secret-token"}`, "Google", 404, []byte("very-secret-key")},
		{"secret too large", `{"access_token":"secret-token"}`, "Google", 200, []byte(strings.Repeat("x", 128<<10+1))},
		{"invalid base64", `{"access_token":"secret-token"}`, "Google", 200, []byte(`{"payload":{"data":"not-base64!","dataCrc32c":"0"}}`)},
		{"checksum mismatch", `{"access_token":"secret-token"}`, "Google", 200, []byte(`{"payload":{"data":"e30=","dataCrc32c":"0"}}`)},
		{"invalid JSON", `{"access_token":"secret-token"}`, "Google", 200, secretPayload([]byte("very-secret-key"))},
		{"incomplete app", `{"access_token":"secret-token"}`, "Google", 200, secretPayload([]byte(`{"private_key":"very-secret-key"}`))},
		{"numeric app ID", `{"access_token":"secret-token"}`, "Google", 200, secretPayload([]byte(`{"app_id":123,"installation_id":456,"private_key":"very-secret-key"}`))},
		{"decoded too large", `{"access_token":"secret-token"}`, "Google", 200, secretPayload([]byte(strings.Repeat("x", 64<<10+1)))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/metadata" {
					w.Header().Set("Metadata-Flavor", tt.flavor)
					fmt.Fprint(w, tt.metadata)
					return
				}
				w.WriteHeader(tt.status)
				w.Write(tt.payload)
			}))
			defer server.Close()
			reader := secretReader{server.Client(), server.URL + "/metadata", server.URL + "/v1/"}
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

func TestSecretReaderCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := secretReader{http.DefaultClient, "http://127.0.0.1:1/metadata", "http://127.0.0.1:1/v1/"}
	if _, err := reader.read(ctx, testSecretVersion); err == nil {
		t.Fatal("canceled request succeeded")
	}
}

func TestTokenSourceRequiresSecretManager(t *testing.T) {
	t.Setenv("GITHUB_APP_SECRET", "")
	if _, err := tokenSource(context.Background(), http.DefaultClient); err == nil {
		t.Fatal("missing secret resource accepted")
	}
}
