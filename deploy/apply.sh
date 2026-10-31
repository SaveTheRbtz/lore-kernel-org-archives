#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/config.sh

gcloud infra-manager deployments apply "$deployment" \
  --service-account="projects/$project/serviceAccounts/$infra_sa" \
  --tf-version-constraint='=1.5.7' --local-source=infra

# Updating instance metadata does not run its startup script.
gcloud compute ssh "$name" --zone="$zone" --tunnel-through-iap \
  --command='sudo systemctl restart google-startup-scripts.service'
