#!/bin/bash
set -euo pipefail

metadata() {
    local response status
    if ! response=$(curl --silent --show-error --retry 3 --connect-timeout 5 --max-time 20 \
        -H 'Metadata-Flavor: Google' --write-out $'\n%{http_code}' \
        "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"); then
        return 1
    fi
    status=${response##*$'\n'}
    case "$status" in
        200) printf '%s' "${response%$'\n'*}" ;;
        404) ;; # An absent optional setting is different from a failed read.
        *) printf 'Metadata %s returned HTTP %s\n' "$1" "$status" >&2; return 1 ;;
    esac
}

enabled=$(metadata lore-mirror-enabled)
if [[ "$enabled" != true ]]; then
    systemctl stop lore-mirror.service 2>/dev/null || true
fi

image=$(metadata lore-mirror-image)
if [[ -z "$image" ]]; then
    if [[ "$enabled" == true ]]; then
        echo 'Mirror is enabled but lore-mirror-image is missing.' >&2
        exit 1
    fi
    echo 'No image configured; mirror is stopped.'
    exit 0
fi
if [[ ! "$image" =~ ^[a-z0-9-]+-docker\.pkg\.dev/kernelorg-mirror/lore-mirror/lore-mirror@sha256:[0-9a-f]{64}$ ]]; then
    echo 'lore-mirror-image must be an Artifact Registry image pinned by digest.' >&2
    exit 1
fi
lists=$(metadata lore-mirror-lists)
if [[ ! "$lists" =~ ^[A-Za-z0-9._,-]*$ ]]; then
    echo 'Invalid lore-mirror-lists metadata.' >&2
    exit 1
fi

# COS supplies this credential helper; it gets tokens from the VM identity.
registry=${image%%/*}
registry_config=$(mktemp -d /run/lore-mirror-docker.XXXXXX)
trap 'rm -rf "$registry_config"' EXIT
printf '{"credHelpers":{"%s":"gcr"}}\n' "$registry" > "$registry_config/config.json"
systemctl start docker.service
docker --config "$registry_config" pull "$image"

cat > /etc/systemd/system/lore-mirror.service <<EOF
[Unit]
Description=Lore mailing-list mirror
Requires=docker.service
After=docker.service network-online.target
StartLimitIntervalSec=0

[Service]
ExecStartPre=-/usr/bin/docker rm -f lore-mirror
ExecStart=/usr/bin/docker run --rm --name=lore-mirror --read-only --cap-drop=ALL --security-opt=no-new-privileges --memory=2g --memory-swap=2g --pids-limit=128 --publish=127.0.0.1:8080:8080 --log-driver=gcplogs --env=GOOGLE_CLOUD_PROJECT=kernelorg-mirror --env=GITHUB_ORG=lore-kernel-org-archives --env=GITHUB_APP_SECRET=projects/kernelorg-mirror/secrets/github-app/versions/latest $image -lists=$lists
ExecStop=/usr/bin/docker stop -t 20 lore-mirror
ExecStopPost=-/usr/bin/docker rm -f lore-mirror
Restart=on-failure
RestartSec=60s
TimeoutStopSec=30s
EOF
systemctl daemon-reload
if [[ "$enabled" == true ]]; then
    systemctl restart lore-mirror.service
else
    echo 'Image ready; mirror remains stopped until lore-mirror-enabled=true.'
fi
