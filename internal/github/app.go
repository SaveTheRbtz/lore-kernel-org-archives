package github

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// AppAuth holds only an in-memory installation token. Its key is loaded by the
// caller from Secret Manager; no long-lived credential is sent to GitHub.
type AppAuth struct {
	client         *http.Client
	apiBase        string
	appID          string
	installationID int64
	key            *rsa.PrivateKey
	now            func() time.Time
	mu             sync.Mutex
	token          string
	expires        time.Time
}

func NewAppAuth(appID string, installationID int64, pemKey []byte, client *http.Client, apiBase string) (*AppAuth, error) {
	if appID == "" || installationID <= 0 {
		return nil, errors.New("GitHub App ID and positive installation ID are required")
	}
	block, rest := pem.Decode(pemKey)
	if block == nil || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("expected one PEM-encoded GitHub App private key")
	}
	var key *rsa.PrivateKey
	switch block.Type {
	case "RSA PRIVATE KEY":
		var err error
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse GitHub App RSA key: %w", err)
		}
	case "PRIVATE KEY":
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse GitHub App key: %w", err)
		}
		var ok bool
		key, ok = parsed.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("GitHub App key must be RSA")
		}
	default:
		return nil, errors.New("GitHub App key must be an unencrypted RSA private key")
	}
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("invalid GitHub App key: %w", err)
	}
	return &AppAuth{client: client, apiBase: apiBase, appID: appID, installationID: installationID, key: key, now: time.Now}, nil
}

func (a *AppAuth) Token(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	if a.token != "" && now.Add(5*time.Minute).Before(a.expires) {
		return a.token, nil
	}
	jwt, err := a.jwt(now)
	if err != nil {
		return "", err
	}
	var response struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	path := "/app/installations/" + strconv.FormatInt(a.installationID, 10) + "/access_tokens"
	if err := githubRequest(ctx, a.client, a.apiBase, false, http.MethodPost, path, jwt, struct{}{}, &response); err != nil {
		return "", err
	}
	if response.Token == "" || !response.ExpiresAt.After(now) {
		return "", errors.New("GitHub returned an empty or expired installation token")
	}
	a.token, a.expires = response.Token, response.ExpiresAt
	return a.token, nil
}

func (a *AppAuth) jwt(now time.Time) (string, error) {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, err := json.Marshal(struct {
		IssuedAt int64  `json:"iat"`
		Expires  int64  `json:"exp"`
		Issuer   string `json:"iss"`
	}{IssuedAt: now.Add(-time.Minute).Unix(), Expires: now.Add(9 * time.Minute).Unix(), Issuer: a.appID})
	if err != nil {
		return "", err
	}
	message := header + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(message))
	signature, err := rsa.SignPKCS1v15(rand.Reader, a.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign GitHub App JWT: %w", err)
	}
	return message + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}
