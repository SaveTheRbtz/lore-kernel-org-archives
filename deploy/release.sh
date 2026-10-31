#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/config.sh

build=$(gcloud builds submit . --config=cloudbuild.yaml --region="$region" \
  --substitutions="_REGION=$region" \
  --service-account="projects/$project/serviceAccounts/$build_sa" \
  --gcs-source-staging-dir="$source_bucket/source" --suppress-logs --format='json(results.images)')

python3 - "$build" <<'PY'
import json, pathlib, re, sys
image = json.loads(sys.argv[1])["results"]["images"][0]
if not re.fullmatch(r"sha256:[0-9a-f]{64}", image["digest"]):
    raise SystemExit("Missing image digest")
path = pathlib.Path("infra/runtime.auto.tfvars.json")
config = json.loads(path.read_text())
config["image"] = image["name"].rsplit(":", 1)[0] + "@" + image["digest"]
path.write_text(json.dumps(config, indent=2) + "\n")
PY
bash deploy/apply.sh
