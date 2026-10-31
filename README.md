# lore-mirror

Mirror [lore.kernel.org](https://lore.kernel.org) mailing-list archives to [lore-kernel-org-archives](https://github.com/lore-kernel-org-archives). Each archive epoch gets a repository: `/lkml/git/20.git` becomes `lkml.20`. Unsharded v1 archives use `<list>.v1`.

The service discovers archives automatically and streams Git packs directly to GitHub. It keeps no local archive or database. GitHub repositories are mirrors: upstream ref changes and removals are propagated, so do not edit them directly. Removing an archive from Lore's manifest does not delete its GitHub repository.

## Deployment

The deployment uses project `kernelorg-mirror`, region `us-central1`, and VM `lore-mirror` in `us-central1-a`. The `e2-medium` VM has 4 GiB RAM and a 20 GiB boot disk, costing approximately $29/month before network traffic and other usage.

Terraform in `infra/` is managed by Infrastructure Manager. Cloud Build builds the image and Artifact Registry stores it. Use an authenticated `gcloud` CLI and Python 3 to deploy:

```sh
# Build and deploy a new release.
deploy/release.sh

# Apply configuration changes without rebuilding.
deploy/apply.sh
```

Both commands run the VM startup script after applying Terraform. Infrastructure Manager stores Terraform state; a local Terraform installation is unnecessary.

Edit `infra/runtime.auto.tfvars.json` to configure the service:

| Setting | Meaning |
| --- | --- |
| `enabled` | Start or stop mirroring |
| `lists` | Comma-separated inbox names, such as `cgroups`; empty selects all inboxes |
| `image` | Image digest, updated by `deploy/release.sh` |

Start with one inbox, check the resulting repository and logs, then expand the selection.

## GitHub credentials

Authentication uses the `lore-kernel-org-mirror` GitHub App, installed for all repositories in `lore-kernel-org-archives`. It needs Administration, Contents, and Repository creation write permissions, plus Metadata read permission.

To replace its private key in Secret Manager:

```sh
python3 - 5551934 177121167 /absolute/path/to/key.pem <<'PY' | \
  gcloud secrets versions add github-app --project=kernelorg-mirror --data-file=-
import json, pathlib, sys
print(json.dumps({
    "app_id": sys.argv[1],
    "installation_id": int(sys.argv[2]),
    "private_key": pathlib.Path(sys.argv[3]).read_text(),
}))
PY
```

The VM reads `github-app/versions/latest` using its service account. After rotating the key, run `deploy/apply.sh` to restart with the new version.

## Operation

Check the service over IAP:

```sh
gcloud compute ssh lore-mirror --project=kernelorg-mirror --zone=us-central1-a \
  --tunnel-through-iap --command='sudo systemctl status lore-mirror --no-pager'
```

Application logs are in [Cloud Logging](https://console.cloud.google.com/logs/query?project=kernelorg-mirror). OpenTelemetry metrics are in [Cloud Monitoring](https://console.cloud.google.com/monitoring?project=kernelorg-mirror), under `prometheus.googleapis.com/lore_mirror_*`. Use them to track sync latency, throughput, errors, pending work, worker utilization, and Go memory. Metrics are exported every minute and have no per-repository labels.

On the VM, `curl -f http://127.0.0.1:8080/healthz` checks health. An active discovery or sync with no progress for five minutes returns HTTP 503 and sets `lore_mirror_stuck` to 1. Idle time and retry backoff stay healthy. Docker uses the same endpoint for its healthcheck.

Discovery runs every minute. One worker processes archives, retries failures with backoff, and rechecks unchanged archives daily. Restarting is safe: each sync compares against GitHub's current refs and verifies the result after pushing.

## Limits

Pushes are limited to 1.75 GiB; oversized epochs remain incomplete and retry daily. Packs are not split. GitHub also rejects blobs larger than 100 MiB. If an upstream default-branch rename requires deleting GitHub's current default branch, change the repository's default branch before retrying.

## Development

Requires Go 1.26 or newer. The application uses go-git and builds with CGO disabled.

```sh
CGO_ENABLED=0 go test ./...
CGO_ENABLED=0 go vet ./...
go test -race ./...
CGO_ENABLED=0 go build -buildvcs=false -o /tmp/lore-mirror ./cmd/lore-mirror
/tmp/lore-mirror -discover -lists cgroups
```

`-discover` is read-only and needs no credentials. A sync requires the VM identity and `GITHUB_APP_SECRET=projects/kernelorg-mirror/secrets/github-app/versions/latest`. Use `-once` for one pass and `-h` for available flags.
