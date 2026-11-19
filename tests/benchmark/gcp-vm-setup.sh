#!/bin/bash
#
# On-VM setup script for benchmark VMs.
# Run by gcp-vm.sh during "create" — not intended to be called directly.
#
# Usage: gcp-vm-setup.sh [idle-hours]
#
# Installs: Go 1.25, Python 3 + PyYAML + venv, Docker, AWS CLI, build tools,
# and a timer that deletes the VM after idle-hours (default 168) without use.
#
# python3-venv is here for connectors whose local-binary mode needs a Python
# sidecar built from source, e.g. materialize-snowflake's Snowpipe Streaming
# v2 path.

set -o errexit
set -o pipefail
set -o nounset

GO_VERSION="1.25.0"
IDLE_HOURS="${1:-168}"

echo "==> updating apt"
sudo apt-get update -qq

echo "==> installing system packages"
sudo apt-get install -y -qq \
  build-essential git curl rsync jq \
  python3 python3-pip python3-venv \
  docker.io docker-compose-v2 docker-buildx \
  >/dev/null

echo "==> installing Go ${GO_VERSION}"
curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" \
  | sudo tar -C /usr/local -xz

echo "==> installing PyYAML"
pip3 install --break-system-packages -q pyyaml

echo "==> installing AWS CLI v2"
AWS_ARCH="$(uname -m)"
curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${AWS_ARCH}.zip" -o /tmp/awscliv2.zip
sudo apt-get install -y -qq unzip >/dev/null
unzip -q -o /tmp/awscliv2.zip -d /tmp
sudo /tmp/aws/install --update
rm -rf /tmp/aws /tmp/awscliv2.zip

echo "==> setting up local SSD for Docker"
# On c3-*-lssd instances, find the local NVMe SSD by its serial number,
# format it, mount it, and point Docker's data-root there so all
# container I/O uses the high-IOPS local disk.
NVME_NAME="$(lsblk --json --output name,model,size,serial | jq -r '
  .blockdevices[]
  | select(.serial == "local-nvme-ssd-0")
  | .name')"
if [[ -n "$NVME_NAME" ]]; then
  NVME_DEV="/dev/$NVME_NAME"
  sudo mkfs.ext4 -F "$NVME_DEV"
  sudo mkdir -p /mnt/localssd
  sudo mount "$NVME_DEV" /mnt/localssd
  sudo mkdir -p /mnt/localssd/docker
  sudo mkdir -p /etc/docker
  echo '{"data-root": "/mnt/localssd/docker"}' | sudo tee /etc/docker/daemon.json >/dev/null
  sudo systemctl restart docker
  sudo mkdir -p /mnt/localssd/tmp
  sudo chown "$USER:" /mnt/localssd/tmp
  echo "    local SSD ($NVME_DEV) mounted at /mnt/localssd, Docker data-root redirected"
else
  echo "    WARNING: no local NVMe SSD found, using boot disk for Docker"
fi

echo "==> adding $USER to docker group"
sudo usermod -aG docker "$USER"

echo "==> creating flow-test docker network"
sudo docker network create flow-test 2>/dev/null || true

echo "==> writing PATH config"
# Write to both /etc/profile.d (login shells) and ~/.bashrc (non-login
# interactive shells and scripts invoked via "bash -l").
sudo tee /etc/profile.d/bench-env.sh >/dev/null <<'PROFILE'
export PATH="/usr/local/go/bin:$HOME/go/bin:$HOME/connectors/flow-bin:$PATH"
export GOPATH="$HOME/go"
PROFILE
# preview-next writes every document it reads under TMPDIR, so a run whose
# TMPDIR is on the boot disk is capped by that disk's write throughput.
if [[ -d /mnt/localssd/tmp ]]; then
  echo 'export TMPDIR=/mnt/localssd/tmp' | sudo tee -a /etc/profile.d/bench-env.sh >/dev/null
fi
grep -qF 'bench-env' ~/.bashrc 2>/dev/null || \
  echo 'source /etc/profile.d/bench-env.sh' >> ~/.bashrc

echo "==> initializing git repo for connectors"
mkdir -p ~/connectors
git init -q ~/connectors

echo "==> installing flowctl"
source /etc/profile.d/bench-env.sh
cd ~/connectors
if [[ -f fetch-flow.sh ]]; then
  bash fetch-flow.sh
else
  echo "    (skipped — fetch-flow.sh not found; run again after sync)"
fi

echo "==> installing idle delete timer (${IDLE_HOURS}h)"
echo "IDLE_HOURS=${IDLE_HOURS}" | sudo tee /etc/bench-idle-delete.conf >/dev/null
sudo tee /usr/local/bin/bench-idle-delete.sh >/dev/null <<'IDLE'
#!/bin/bash
#
# Deletes this VM once it has gone IDLE_HOURS without use. The VM is in use
# while an SSH connection is open, an SSH login happened in the last ten
# minutes, or a benchmark is running. Falls back to powering off when the
# delete call fails, so an idle VM never keeps running.

set -o errexit
set -o pipefail
set -o nounset

IDLE_HOURS=168
# Minutes between checks. Must equal OnUnitActiveSec in bench-idle-delete.timer,
# because idle time is measured as a count of consecutive idle checks.
CHECK_MINUTES=5
# Checks are skipped this soon after boot, so that a restarted VM leaves time
# to connect.
MIN_UPTIME_MINUTES=10
# Count of consecutive idle checks. Kept on tmpfs so that it starts from zero
# on every boot and stays writable when the boot disk is full.
STATE_FILE=/run/bench-idle-delete/idle-checks
source /etc/bench-idle-delete.conf

log() { logger -t bench-idle-delete "$1"; }

in_use() {
  pgrep -f '^sshd(-session)?: [^ ]+@' >/dev/null && return 0
  journalctl -u ssh --since "-10min" -q --no-pager | grep -q 'Accepted ' && return 0
  pgrep -f 'tests/benchmark/|flowctl' >/dev/null && return 0
  return 1
}

delete_self() {
  local md="http://metadata.google.internal/computeMetadata/v1/instance"
  local token zone name
  token="$(curl -fsS -H 'Metadata-Flavor: Google' "$md/service-accounts/default/token" | jq -r .access_token)"
  zone="$(curl -fsS -H 'Metadata-Flavor: Google' "$md/zone")"
  name="$(curl -fsS -H 'Metadata-Flavor: Google' "$md/name")"
  curl -fsS -X DELETE -H "Authorization: Bearer $token" \
    "https://compute.googleapis.com/compute/v1/${zone}/instances/${name}" >/dev/null
}

mkdir -p "$(dirname "$STATE_FILE")"
UPTIME_MINUTES="$(awk '{print int($1/60)}' /proc/uptime)"

if in_use || (( UPTIME_MINUTES < MIN_UPTIME_MINUTES )); then
  echo 0 > "$STATE_FILE"
  exit 0
fi

IDLE_CHECKS="$(cat "$STATE_FILE" 2>/dev/null || true)"
[[ "$IDLE_CHECKS" =~ ^[0-9]+$ ]] || IDLE_CHECKS=0
IDLE_CHECKS=$(( IDLE_CHECKS + 1 ))
echo "$IDLE_CHECKS" > "$STATE_FILE"

if (( IDLE_CHECKS < IDLE_HOURS * 60 / CHECK_MINUTES )); then
  exit 0
fi

log "idle for $(( IDLE_CHECKS * CHECK_MINUTES ))min, limit is ${IDLE_HOURS}h; deleting this VM"
if ! delete_self; then
  log "delete failed; powering off instead"
  systemctl poweroff
fi
IDLE
sudo chmod 755 /usr/local/bin/bench-idle-delete.sh
sudo tee /etc/systemd/system/bench-idle-delete.service >/dev/null <<'UNIT'
[Unit]
Description=Delete this benchmark VM once it has been idle too long

[Service]
Type=oneshot
ExecStart=/usr/local/bin/bench-idle-delete.sh
UNIT
sudo tee /etc/systemd/system/bench-idle-delete.timer >/dev/null <<'UNIT'
[Unit]
Description=Periodic idle check for benchmark VM deletion

[Timer]
OnBootSec=5min
OnUnitActiveSec=5min
Unit=bench-idle-delete.service

[Install]
WantedBy=timers.target
UNIT
sudo systemctl daemon-reload
sudo systemctl enable --now bench-idle-delete.timer

echo "==> setup complete"
