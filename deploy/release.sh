#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/config.sh

digest=$(gcloud builds submit . --region="$region" \
  --service-account="$build_sa" \
  --gcs-source-staging-dir="gs://$project-build-source/source" --suppress-logs --format='value(results.images[0].digest)')
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { printf 'Cloud Build returned no valid image digest\n' >&2; exit 1; }
image="$region-docker.pkg.dev/$project/$name/$name@$digest"

lock_deployment
input=infra/image.auto.tfvars
staged=$(mktemp "$input.XXXXXX")
trap 'rm -f "$staged"' EXIT
printf 'image = "%s"\n' "$image" > "$staged"
chmod --reference="$input" "$staged"
mv -f "$staged" "$input"
apply_deployment
printf 'Deployed %s\n' "$image"
