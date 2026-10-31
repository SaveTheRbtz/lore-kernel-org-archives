#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/config.sh

lock_deployment
apply_deployment
