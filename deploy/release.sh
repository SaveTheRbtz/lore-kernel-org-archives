#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/config.sh

digest=$(gcloud builds submit . --region="$region" \
  --service-account="projects/$project/serviceAccounts/$build_sa" \
  --gcs-source-staging-dir="$source_bucket/source" --suppress-logs --format='value(results.images[0].digest)')
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { printf 'Missing image digest\n' >&2; exit 1; }
image="$region-docker.pkg.dev/$project/$name/$name@$digest"

python3 - "$image" <<'PY'
import json, pathlib, sys
path = pathlib.Path("infra/runtime.auto.tfvars.json")
config = json.loads(path.read_text())
config["image"] = sys.argv[1]
path.write_text(json.dumps(config, indent=2) + "\n")
PY
bash deploy/apply.sh
printf 'Deployed %s\n' "$image"
