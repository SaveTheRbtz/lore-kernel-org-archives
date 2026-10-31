package github

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAppAuthJWTAndRefresh(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var clock atomic.Int64
	clock.Store(time.Date(2026, 10, 30, 22, 0, 0, 0, time.UTC).Unix())
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestNumber := requests.Add(1)
		now := time.Unix(clock.Load(), 0)
		if r.Method != http.MethodPost || r.URL.Path != "/app/installations/42/access_tokens" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		jwt := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		parts := strings.Split(jwt, ".")
		if len(parts) != 3 {
			t.Error("invalid JWT structure")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
		signature, err := base64.RawURLEncoding.DecodeString(parts[2])
		if err != nil || rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], signature) != nil {
			t.Error("invalid JWT signature")
		}
		claimsJSON, _ := base64.RawURLEncoding.DecodeString(parts[1])
		var claims struct {
			Issuer string `json:"iss"`
			Issued int64  `json:"iat"`
			Expiry int64  `json:"exp"`
		}
		if err := json.Unmarshal(claimsJSON, &claims); err != nil {
			t.Error(err)
		}
		if claims.Issuer != "123" || claims.Issued != now.Add(-time.Minute).Unix() || claims.Expiry != now.Add(9*time.Minute).Unix() {
			t.Errorf("unexpected claims: %+v", claims)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"token": fmt.Sprintf("token-%d", requestNumber), "expires_at": now.Add(time.Hour)})
	}))
	defer server.Close()
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	auth, err := NewAppAuth("123", 42, pemKey, server.Client(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	auth.now = func() time.Time { return time.Unix(clock.Load(), 0) }
	for range 2 {
		token, err := auth.Token(context.Background())
		if err != nil || token != "token-1" {
			t.Fatalf("token=%q err=%v", token, err)
		}
	}
	if requests.Load() != 1 {
		t.Fatalf("expected cached token, made %d requests", requests.Load())
	}
	clock.Add(int64(56 * time.Minute / time.Second))
	token, err := auth.Token(context.Background())
	if err != nil || token != "token-2" || requests.Load() != 2 {
		t.Fatalf("refresh token=%q requests=%d err=%v", token, requests.Load(), err)
	}
}

func TestAppAuthPKCS8AndInvalidKeys(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewAppAuth("123", 42, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}), nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAppAuth("123", 42, []byte("not a key"), nil, ""); err == nil {
		t.Fatal("invalid key accepted")
	}
}
