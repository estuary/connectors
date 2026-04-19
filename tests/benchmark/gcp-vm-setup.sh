#!/bin/bash
#
# On-VM setup script for benchmark VMs.
# Run by gcp-vm.sh during "create" — not intended to be called directly.
#
# Installs: Go 1.25, Python 3 + PyYAML, Docker, build tools.

set -o errexit
set -o pipefail
set -o nounset

GO_VERSION="1.25.0"

echo "==> updating apt"
sudo apt-get update -qq

echo "==> installing system packages"
sudo apt-get install -y -qq \
  build-essential git curl rsync jq \
  python3 python3-pip \
  docker.io docker-compose-v2 docker-buildx \
  >/dev/null

echo "==> installing Go ${GO_VERSION}"
curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" \
  | sudo tar -C /usr/local -xz

echo "==> installing PyYAML"
pip3 install --break-system-packages -q pyyaml

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

echo "==> setup complete"
