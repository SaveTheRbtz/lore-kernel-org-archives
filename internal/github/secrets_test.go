package github

import (
	"hash/crc32"
	"strings"
	"testing"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
)

const validAppJSON = `{"app_id":"123","installation_id":456,"private_key":"very-secret-key"}`

func TestDecodeAppSecret(t *testing.T) {
	checksum := int64(1094507487) // CRC32C of validAppJSON, independent of the decoder.
	got, err := decodeAppSecret(&secretmanagerpb.SecretPayload{Data: []byte(validAppJSON), DataCrc32C: &checksum})
	if err != nil {
		t.Fatal(err)
	}
	want := appSecret{AppID: 123, InstallationID: 456, PrivateKey: "very-secret-key"}
	if got != want {
		t.Fatal("decoded credentials differ from the stored GitHub App credentials")
	}
}

func TestDecodeAppSecretRejectsInvalidPayload(t *testing.T) {
	badChecksum := secretPayload(validAppJSON)
	*badChecksum.DataCrc32C ^= 1
	for _, tt := range []struct {
		name    string
		payload *secretmanagerpb.SecretPayload
	}{
		{"missing payload", nil},
		{"empty payload", secretPayload("")},
		{"missing checksum", &secretmanagerpb.SecretPayload{Data: []byte(validAppJSON)}},
		{"checksum mismatch", badChecksum},
		{"too large", secretPayload(validAppJSON + strings.Repeat(" ", 64<<10))},
		{"missing app ID", secretPayload(`{"installation_id":456,"private_key":"very-secret-key"}`)},
		{"negative app ID", secretPayload(`{"app_id":"-1","installation_id":456,"private_key":"very-secret-key"}`)},
		{"unparseable ID", secretPayload(`{"app_id":"very-secret-key","installation_id":456,"private_key":"very-secret-key"}`)},
		{"missing installation ID", secretPayload(`{"app_id":"123","private_key":"very-secret-key"}`)},
		{"negative installation ID", secretPayload(`{"app_id":"123","installation_id":-1,"private_key":"very-secret-key"}`)},
		{"missing key", secretPayload(`{"app_id":"123","installation_id":456}`)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeAppSecret(tt.payload)
			if err == nil || got != (appSecret{}) {
				t.Fatal("invalid credentials accepted or partially returned")
			}
			if strings.Contains(err.Error(), "very-secret-key") {
				t.Fatal("error exposed credentials")
			}
		})
	}
}

func secretPayload(data string) *secretmanagerpb.SecretPayload {
	payload := []byte(data)
	checksum := int64(crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)))
	return &secretmanagerpb.SecretPayload{Data: payload, DataCrc32C: &checksum}
}
