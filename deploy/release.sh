#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/config.sh

digest=$(gcloud builds submit . --region="$region" \
  --service-account="projects/$project/serviceAccounts/$build_sa" \
  --gcs-source-staging-dir="$source_bucket/source" --suppress-logs --format='value(results.images[0].digest)')
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { printf 'Cloud Build returned no valid image digest\n' >&2; exit 1; }
image="$region-docker.pkg.dev/$project/$name/$name@$digest"

lock_deployment
python3 - "$image" <<'PY'
import json, pathlib, shutil, sys, tempfile
path = pathlib.Path("infra/runtime.auto.tfvars.json")
config = json.loads(path.read_text())
config["image"] = sys.argv[1]
with tempfile.TemporaryDirectory(dir=path.parent) as staging:
    updated = pathlib.Path(staging) / path.name
    updated.write_text(json.dumps(config, indent=2) + "\n")
    shutil.copymode(path, updated)
    updated.replace(path)
PY
apply_deployment
printf 'Deployed %s\n' "$image"
