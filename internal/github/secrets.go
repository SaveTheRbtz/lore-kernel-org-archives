package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
)

type githubAppSecret struct {
	AppID          int64  `json:"app_id,string"`
	InstallationID int64  `json:"installation_id"`
	PrivateKey     string `json:"private_key"`
}

func readAppSecret(ctx context.Context) (githubAppSecret, error) {
	name := os.Getenv("GITHUB_APP_SECRET")
	if name == "" {
		return githubAppSecret{}, errors.New("set GITHUB_APP_SECRET to a Secret Manager version resource; -discover needs no credentials")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return githubAppSecret{}, fmt.Errorf("create Secret Manager client: %w", err)
	}
	defer func() { _ = client.Close() }()
	response, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: name})
	if err != nil {
		return githubAppSecret{}, fmt.Errorf("access GitHub App secret: %w", err)
	}
	return decodeAppSecret(response.GetPayload())
}

func decodeAppSecret(payload *secretmanagerpb.SecretPayload) (githubAppSecret, error) {
	if payload == nil || len(payload.Data) == 0 || len(payload.Data) > 64<<10 {
		return githubAppSecret{}, errors.New("invalid payload from Secret Manager")
	}
	checksum := int64(crc32.Checksum(payload.Data, crc32.MakeTable(crc32.Castagnoli)))
	if payload.DataCrc32C == nil || *payload.DataCrc32C != checksum {
		return githubAppSecret{}, errors.New("checksum mismatch in Secret Manager payload")
	}
	var secret githubAppSecret
	if json.Unmarshal(payload.Data, &secret) != nil || secret.AppID <= 0 || secret.InstallationID <= 0 || secret.PrivateKey == "" {
		return githubAppSecret{}, errors.New("GitHub App secret requires positive app_id (string), positive installation_id, and private_key (PEM string)")
	}
	return secret, nil
}
