# lore-mirror

Mirror [lore.kernel.org](https://lore.kernel.org) mailing-list archives to [lore-kernel-org-archives](https://github.com/lore-kernel-org-archives). Each archive epoch gets a repository: `/lkml/git/20.git` becomes `lkml.20`. Unsharded v1 archives use `<list>.v1`.

The service discovers archives automatically and streams Git packs directly to GitHub. It keeps no local archive or database. GitHub repositories are mirrors: upstream ref changes and removals are propagated, so do not edit them directly. Removing an archive from Lore's manifest does not delete its GitHub repository.

## Deployment

The deployment uses project `kernelorg-mirror` and a size-one managed instance group (MIG), `lore-mirror`, in `us-central1-a`. Its `e2-medium` VM has 4 GiB RAM and a disposable 20 GiB boot disk, costing approximately $29/month before network traffic and other usage.

Terraform in `infra/` is managed by Infrastructure Manager. Cloud Build builds the image and Artifact Registry stores it. Use an authenticated `gcloud` CLI and Python 3 to deploy:

```sh
# Build and deploy a new release.
deploy/release.sh

# Apply configuration changes without rebuilding.
deploy/apply.sh
```

Image or configuration changes replace the VM, stopping the old worker before starting its replacement. Terraform waits for the group update to finish. Infrastructure Manager stores Terraform state; a local Terraform installation is unnecessary.

Edit `infra/runtime.auto.tfvars.json` to configure the service:

| Setting | Meaning |
| --- | --- |
| `enabled` | Run one VM when true; scale to zero when false |
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

The VM reads `github-app/versions/latest` using its service account. After rotating the key, replace the running VM to load it:

```sh
gcloud compute instance-groups managed rolling-action replace lore-mirror \
  --project=kernelorg-mirror --zone=us-central1-a \
  --max-surge=0 --max-unavailable=1 --replacement-method=recreate
```

## Operation

List the current VM and its health; use its generated name for SSH over IAP:

```sh
gcloud compute instance-groups managed list-instances lore-mirror \
  --project=kernelorg-mirror --zone=us-central1-a
gcloud compute ssh VM_NAME --project=kernelorg-mirror --zone=us-central1-a \
  --tunnel-through-iap --command='sudo systemctl status lore-mirror --no-pager'
```

Application logs are in [Cloud Logging](https://console.cloud.google.com/logs/query?project=kernelorg-mirror). OpenTelemetry metrics are in [Cloud Monitoring](https://console.cloud.google.com/monitoring?project=kernelorg-mirror), under `prometheus.googleapis.com/lore_mirror_*`. Use them to track sync latency, throughput, errors, pending work, worker utilization, and Go memory. Metrics are exported every minute and have no per-repository labels.

On the VM, `curl -f http://127.0.0.1:8080/healthz` checks health. An active discovery or sync with no progress for five minutes returns HTTP 503 and sets `lore_mirror_stuck` to 1. Idle time and retry backoff stay healthy. GCP probes this endpoint every 30 seconds and recreates the VM after three failed checks, with a five-minute startup grace period. The firewall permits port 8080 only from GCP health-check ranges.

Health transitions are recorded in Cloud Logging:

```sh
gcloud logging read 'resource.type="gce_instance_group_manager"
  resource.labels.instance_group_manager_name="lore-mirror"
  log_id("compute.googleapis.com/instance_group_manager_events")
  jsonPayload.instanceHealthStateChange:*' \
  --project=kernelorg-mirror --freshness=1h --limit=10
```

Discovery and successful repository rechecks use the same `-recheck` interval (default one hour), plus jitter. Each new archive gets an independent initial delay between zero and that interval. One worker chooses randomly among ready archives and retries failures with backoff; `-once` also uses a shuffled order. Discovering new mail makes an archive due immediately while preserving failure backoff. Restarting is safe: each sync compares against GitHub's current refs and verifies the result after pushing.

## Limits

Pushes are limited to 1.75 GiB. A failed initial import can retry in batches of roughly 10,000 commits, with paced requests and a verified checkpoint after each batch. This fallback supports a single checkpoint chain and resumes only from an exact source checkpoint; ambiguous histories or destination states stop the import. A batch that still exceeds the byte limit retries daily. GitHub also rejects blobs larger than 100 MiB. If an upstream default-branch rename requires deleting GitHub's current default branch, change the repository's default branch before retrying.

## Development

Requires Go 1.27 or newer. The application uses go-git and builds with CGO disabled.

```sh
CGO_ENABLED=0 go test ./...
CGO_ENABLED=0 go vet ./...
go test -race ./...
CGO_ENABLED=0 go build -buildvcs=false -o /tmp/lore-mirror ./cmd/lore-mirror
/tmp/lore-mirror -discover -lists cgroups
```

`-discover` is read-only and needs no credentials. A sync requires the VM identity and `GITHUB_APP_SECRET=projects/kernelorg-mirror/secrets/github-app/versions/latest`. Use `-once` for one pass and `-h` for available flags.
