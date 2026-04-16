#!/bin/bash
#
# Manage GCP VMs for running materialization benchmarks against cloud
# connectors. Handles creation (with full dependency setup), code syncing,
# SSH access, and cleanup.
#
# Usage:
#   tests/benchmark/gcp-vm.sh <command> [flags]
#
# Commands:
#   create     Create a new benchmark VM and provision it
#   sync       Rsync the local codebase to the VM
#   pull-runs  Pull benchmark runs from the VM to the local machine
#   ssh        SSH into the VM (lands in ~/connectors)
#   destroy    Delete the VM
#   list       List all benchmark VMs
#
# Global flags (apply to most commands):
#   --name NAME           VM name (default: bench-$USER)
#   --zone ZONE           GCP zone (default: us-central1-a)
#   --project PROJECT     GCP project (default: gcloud config)
#
# Create flags:
#   --machine-type TYPE   Machine type (default: e2-standard-2)
#
# Destroy flags:
#   --yes                 Skip confirmation prompt

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"

# Defaults.
USER_LABEL="$(echo "${USER:-unknown}" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9-]/-/g')"
VM_NAME="bench-${USER_LABEL}"
ZONE="us-central1-a"
MACHINE_TYPE="e2-standard-2"
PROJECT=""
YES=0
COMMAND=""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

die()  { echo "ERROR: $*" >&2; exit 1; }
info() { echo "==> $*"; }

usage() {
  sed -n '/^#/!q;s/^# \{0,1\}//;p' "$0" | tail -n +2
  exit "${1:-0}"
}

# Check for benchmark VMs running longer than 7 days. Runs on every
# invocation so that stale VMs are never silently forgotten.
warn_stale_vms() {
  local project_flag=""
  [[ -n "$PROJECT" ]] && project_flag="--project=$PROJECT"

  local cutoff
  if date --version >/dev/null 2>&1; then
    # GNU date
    cutoff="$(date -u -d '7 days ago' '+%Y-%m-%dT%H:%M:%S')"
  else
    # BSD date (macOS)
    cutoff="$(date -u -v-7d '+%Y-%m-%dT%H:%M:%S')"
  fi

  local stale
  stale="$(gcloud compute instances list \
    ${project_flag} \
    --filter="labels.purpose=bench AND creationTimestamp<${cutoff}" \
    --format='table[no-heading](name, zone.basename(), creationTimestamp.date())' \
    2>/dev/null)" || return 0

  if [[ -n "$stale" ]]; then
    echo ""
    echo "WARNING: The following benchmark VMs have been running for more than a week:"
    while IFS= read -r line; do
      echo "  $line"
    done <<< "$stale"
    echo "Consider destroying them with: $0 destroy --name <name>"
    echo ""
  fi
}

# Resolve the GCP project.
resolve_project() {
  if [[ -z "$PROJECT" ]]; then
    PROJECT="$(gcloud config get-value project 2>/dev/null)" || true
  fi
  [[ -n "$PROJECT" ]] || die "no GCP project set; pass --project or run: gcloud config set project <id>"
}

# Common gcloud SSH flags. All SSH access goes through IAP tunnel.
gcloud_ssh() {
  gcloud compute ssh "$VM_NAME" --zone="$ZONE" --project="$PROJECT" \
    --tunnel-through-iap \
    --ssh-flag="-o" --ssh-flag="StrictHostKeyChecking=no" \
    "$@"
}

# Wait for SSH to become available on a newly-created VM.
wait_for_ssh() {
  info "waiting for SSH to become available"
  local attempts=0
  while (( attempts < 40 )); do
    if gcloud_ssh --command="true" --quiet 2>/dev/null; then
      return 0
    fi
    sleep 3
    (( attempts++ ))
  done
  die "SSH not available after 120s — check VM status with: gcloud compute instances describe $VM_NAME --zone=$ZONE"
}

# Build rsync-over-IAP transport strings. Sets RSYNC_TRANSPORT and
# RSYNC_REMOTE_DEST in the caller's scope.
_rsync_transport() {
  info "provisioning SSH keys"
  gcloud_ssh --command="true" --quiet 2>/dev/null || true

  # Build the ssh command that gcloud would use for IAP tunneling, so rsync
  # can reuse it as its transport. The --dry-run flag prints the command
  # without executing it.
  local ssh_cmd
  ssh_cmd="$(gcloud compute ssh "$VM_NAME" --zone="$ZONE" --project="$PROJECT" \
    --tunnel-through-iap \
    --ssh-flag="-o" --ssh-flag="StrictHostKeyChecking=no" \
    --ssh-flag="-o" --ssh-flag="UserKnownHostsFile=/dev/null" \
    --ssh-flag="-o" --ssh-flag="LogLevel=ERROR" \
    --dry-run 2>/dev/null)"

  # ssh_cmd is the full "ssh <flags> <user@host>" string. rsync -e needs
  # everything except the final destination argument (user@host), which
  # rsync supplies itself. Split off the last token.
  RSYNC_REMOTE_DEST="${ssh_cmd##* }"
  RSYNC_TRANSPORT="${ssh_cmd% *}"
}

# Rsync the codebase to the VM via IAP tunnel.
# gcloud compute scp lacks --delete and --exclude, so we use rsync with
# gcloud's IAP wrapper as the transport.
do_sync() {
  _rsync_transport

  info "syncing codebase to ${VM_NAME}"
  rsync -az --delete \
    --exclude='.git' \
    --filter=':- .gitignore' \
    -e "$RSYNC_TRANSPORT" \
    "${ROOT_DIR}/" \
    "${RSYNC_REMOTE_DEST}:~/connectors/"

  info "sync complete"
}

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

cmd_create() {
  resolve_project

  # Check if VM already exists.
  if gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --project="$PROJECT" \
       >/dev/null 2>&1; then
    die "VM '$VM_NAME' already exists in $ZONE. Use 'sync' to update code, or 'destroy' first."
  fi

  info "creating VM: $VM_NAME (zone=$ZONE, type=$MACHINE_TYPE)"
  gcloud compute instances create "$VM_NAME" \
    --zone="$ZONE" \
    --project="$PROJECT" \
    --machine-type="$MACHINE_TYPE" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=100GB \
    --boot-disk-type=pd-ssd \
    --labels="purpose=bench,owner=${USER_LABEL}" \
    --tags=bench \
    --scopes=cloud-platform \
    --quiet

  wait_for_ssh

  info "running setup script on VM"
  gcloud compute scp "$SCRIPT_DIR/gcp-vm-setup.sh" \
    "$VM_NAME:~/gcp-vm-setup.sh" \
    --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap --quiet
  gcloud_ssh --command="bash ~/gcp-vm-setup.sh"

  do_sync

  info "installing flowctl and Go dependencies on VM"
  gcloud_ssh --command="cd ~/connectors && source /etc/profile.d/bench-env.sh && bash fetch-flow.sh && go mod vendor"

  echo ""
  echo "Benchmark VM ready!"
  echo "  name: $VM_NAME"
  echo "  zone: $ZONE"
  echo ""
  echo "Quick start:"
  echo "  $0 ssh                  # shell into the VM"
  echo "  $0 sync                 # push local changes"
  echo "  $0 destroy              # tear down when done"
  echo ""
  echo "On the VM, run benchmarks with:"
  echo "  ./tests/benchmark/materialize/run.sh \\"
  echo "    --connector materialize-bigquery \\"
  echo "    --scenario tests/benchmark/materialize/scenarios/large-docs.yaml"
}

cmd_sync() {
  resolve_project
  gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --project="$PROJECT" \
    >/dev/null 2>&1 || die "VM '$VM_NAME' not found in $ZONE. Run 'create' first."
  do_sync
}

cmd_ssh() {
  resolve_project
  info "connecting to $VM_NAME"
  # shellcheck disable=SC2029
  gcloud_ssh -- -t "cd ~/connectors && exec bash -l" "$@"
}

cmd_destroy() {
  resolve_project

  gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --project="$PROJECT" \
    >/dev/null 2>&1 || die "VM '$VM_NAME' not found in $ZONE."

  if (( ! YES )); then
    echo "This will permanently delete VM '$VM_NAME' in $ZONE."
    read -r -p "Continue? [y/N] " confirm
    [[ "$confirm" =~ ^[Yy] ]] || { echo "Aborted."; exit 1; }
  fi

  info "deleting VM: $VM_NAME"
  gcloud compute instances delete "$VM_NAME" \
    --zone="$ZONE" --project="$PROJECT" --quiet
  info "destroyed $VM_NAME"
}

cmd_pull_runs() {
  resolve_project
  gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --project="$PROJECT" \
    >/dev/null 2>&1 || die "VM '$VM_NAME' not found in $ZONE. Run 'create' first."

  _rsync_transport

  local local_runs="$ROOT_DIR/tests/benchmark/materialize/runs/"
  local remote_runs="${RSYNC_REMOTE_DEST}:~/connectors/tests/benchmark/materialize/runs/"

  info "pulling benchmark runs from ${VM_NAME}"
  rsync -avz \
    -e "$RSYNC_TRANSPORT" \
    "$remote_runs" \
    "$local_runs"

  info "pull complete — runs saved to tests/benchmark/materialize/runs/"
}

cmd_list() {
  resolve_project
  gcloud compute instances list \
    --project="$PROJECT" \
    --filter='labels.purpose=bench' \
    --format='table(name, zone.basename(), status, creationTimestamp.date():label=CREATED, labels.owner:label=OWNER)'
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

# Collect extra args to forward to SSH.
SSH_EXTRA_ARGS=()

while (( $# )); do
  case "$1" in
    create|sync|pull-runs|ssh|destroy|list)
      COMMAND="$1"; shift ;;
    --name)    VM_NAME="$2";      shift 2 ;;
    --zone)    ZONE="$2";         shift 2 ;;
    --project) PROJECT="$2";      shift 2 ;;
    --machine-type) MACHINE_TYPE="$2"; shift 2 ;;
    --yes)     YES=1;             shift   ;;
    -h|--help) usage 0 ;;
    --)        shift; SSH_EXTRA_ARGS=("$@"); break ;;
    *)         die "unknown argument: $1 (try --help)" ;;
  esac
done

[[ -n "$COMMAND" ]] || { echo "No command specified."; usage 1; }

command -v gcloud >/dev/null || die "gcloud CLI not found; install from https://cloud.google.com/sdk/docs/install"

# Always check for stale VMs.
warn_stale_vms

case "$COMMAND" in
  create)    cmd_create ;;
  sync)      cmd_sync ;;
  pull-runs) cmd_pull_runs ;;
  ssh)       cmd_ssh ${SSH_EXTRA_ARGS[@]+"${SSH_EXTRA_ARGS[@]}"} ;;
  destroy)   cmd_destroy ;;
  list)      cmd_list ;;
  *)       die "unknown command: $COMMAND" ;;
esac
