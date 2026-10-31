package observe

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/oauth2"
	"google.golang.org/grpc/credentials"
)

func TestTelemetryCredentialsOutliveSetup(t *testing.T) {
	const tokenURL = "https://issuer.example/token"
	server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/token" {
			t.Errorf("unexpected token request: %s %s", r.Method, r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		// Force a refresh on every RPC.
		_, _ = fmt.Fprint(w, `{"access_token":"test-token","token_type":"Bearer","expires_in":1}`)
	}))
	file := filepath.Join(t.TempDir(), "credentials.json")
	data := fmt.Sprintf(`{"type":"authorized_user","client_id":"test","client_secret":"test","refresh_token":"test","token_uri":%q}`, tokenURL)
	if err := os.WriteFile(file, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", file)
	setup, cancelSetup := context.WithCancel(context.WithValue(t.Context(), oauth2.HTTPClient, server.Client()))
	defer cancelSetup()
	creds, stop, err := telemetryCredentials(setup)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	cancelSetup()

	rpc := credentials.NewContextWithRequestInfo(t.Context(), credentials.RequestInfo{
		AuthInfo: credentials.TLSInfo{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity}},
	})
	md, err := creds.GetRequestMetadata(rpc)
	if err != nil || md["authorization"] != "Bearer test-token" {
		t.Fatalf("refresh after setup = %v, %v", md, err)
	}
	if _, _, err := telemetryCredentials(setup); !errors.Is(err, context.Canceled) {
		t.Fatalf("new credentials with canceled setup = %v, want context.Canceled", err)
	}
	stop()
	if _, err := creds.GetRequestMetadata(rpc); !errors.Is(err, context.Canceled) {
		t.Fatalf("refresh after shutdown = %v, want context.Canceled", err)
	}
}
