package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"
)

type githubAppSecret struct {
	AppID          string `json:"app_id"`
	InstallationID int64  `json:"installation_id"`
	PrivateKey     string `json:"private_key"`
}

// These endpoints are fixed in production; tests substitute local servers.
type secretReader struct {
	client                 *http.Client
	metadataURL, secretURL string
}

// TokenSource loads GitHub App credentials from Secret Manager using VM identity.
func TokenSource(ctx context.Context, githubClient *http.Client) (func(context.Context) (string, error), error) {
	name := os.Getenv("GITHUB_APP_SECRET")
	if name == "" {
		return nil, errors.New("set GITHUB_APP_SECRET to a Secret Manager version resource; -discover needs no credentials")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // Metadata credentials must never pass through a proxy.
	defer transport.CloseIdleConnections()
	reader := secretReader{
		client: &http.Client{
			Transport:     transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		metadataURL: "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
		secretURL:   "https://secretmanager.googleapis.com/v1/",
	}
	secret, err := reader.read(ctx, name)
	if err != nil {
		return nil, err
	}
	auth, err := NewAppAuth(secret.AppID, secret.InstallationID, []byte(secret.PrivateKey), githubClient, "")
	if err != nil {
		return nil, fmt.Errorf("GitHub App secret: %w", err)
	}
	return auth.Token, nil
}

var secretVersionName = regexp.MustCompile(`^projects/(?:[a-z][a-z0-9-]{4,28}[a-z0-9]|[0-9]+)/secrets/[A-Za-z0-9_-]+/versions/(?:[1-9][0-9]*|latest)$`)

func (r secretReader) read(ctx context.Context, name string) (githubAppSecret, error) {
	var secret githubAppSecret
	if !secretVersionName.MatchString(name) {
		return secret, errors.New("GITHUB_APP_SECRET must be projects/PROJECT/secrets/SECRET/versions/VERSION (number or latest)")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	data, err := r.get(ctx, r.metadataURL, "Metadata-Flavor", "Google", 16<<10)
	if err != nil {
		return secret, fmt.Errorf("VM service-account token: %w", err)
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if json.Unmarshal(data, &token) != nil || token.AccessToken == "" {
		return secret, errors.New("metadata server returned an invalid access token")
	}
	data, err = r.get(ctx, r.secretURL+name+":access", "Authorization", "Bearer "+token.AccessToken, 128<<10)
	if err != nil {
		return secret, fmt.Errorf("Secret Manager access: %w", err)
	}
	var response struct {
		Payload struct {
			Data   []byte `json:"data"` // encoding/json decodes the base64 payload.
			CRC32C string `json:"dataCrc32c"`
		} `json:"payload"`
	}
	if json.Unmarshal(data, &response) != nil || len(response.Payload.Data) == 0 || len(response.Payload.Data) > 64<<10 {
		return secret, errors.New("Secret Manager returned an invalid payload")
	}
	checksum, err := strconv.ParseUint(response.Payload.CRC32C, 10, 32)
	if err != nil || uint32(checksum) != crc32.Checksum(response.Payload.Data, crc32.MakeTable(crc32.Castagnoli)) {
		return secret, errors.New("Secret Manager payload checksum mismatch")
	}
	if json.Unmarshal(response.Payload.Data, &secret) != nil || secret.AppID == "" || secret.InstallationID <= 0 || secret.PrivateKey == "" {
		return githubAppSecret{}, errors.New("GitHub App secret requires app_id (string), installation_id (positive integer), and private_key (PEM string)")
	}
	return secret, nil
}

func (r secretReader) get(ctx context.Context, endpoint, header, value string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, errors.New("invalid credential endpoint")
	}
	req.Header.Set(header, value)
	res, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		// Error bodies may contain credentials. Status alone is sufficient.
		return nil, fmt.Errorf("HTTP %d", res.StatusCode)
	}
	if header == "Metadata-Flavor" && res.Header.Get("Metadata-Flavor") != "Google" {
		return nil, errors.New("response is not from the metadata server")
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if int64(len(data)) > limit {
		return nil, errors.New("credential response exceeds size limit")
	}
	return data, nil
}
