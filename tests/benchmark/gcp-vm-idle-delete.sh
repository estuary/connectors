#!/bin/bash
#
# On-VM idle watchdog for benchmark VMs: deletes the VM it runs on once it has
# seen no activity for IDLE_DELETE_AFTER_SECONDS (default 3 days).
# Installed by gcp-vm-setup.sh during "create".
#
# Activity is any of:
#   - an SSH session open right now, or an SSH login since the previous check
#     (covers gcp-vm.sh ssh, sync and pull-runs)
#   - a 15-minute load average of IDLE_BUSY_LOAD or more (default 1.0), which
#     covers detached benchmark runs
#
# Time spent powered off does not count as idle.
#
# Usage (as root):
#   gcp-vm-idle-delete.sh install   Install and start the systemd timer
#   gcp-vm-idle-delete.sh check     Run one check (what the timer does)
#
# Each check logs its verdict: journalctl -u bench-idle-delete
# Override the defaults in /etc/default/bench-idle-delete.
# To keep a VM indefinitely: sudo systemctl disable --now bench-idle-delete.timer

set -o errexit
set -o pipefail
set -o nounset
# A failing probe must abort the check rather than read as "idle".
shopt -s inherit_errexit

UNIT="bench-idle-delete"
INSTALL_PATH="/usr/local/sbin/${UNIT}"
STAMP="/var/lib/${UNIT}/last-active"
CHECK_INTERVAL_MIN=5

IDLE_DELETE_AFTER_SECONDS="${IDLE_DELETE_AFTER_SECONDS:-259200}"
IDLE_BUSY_LOAD="${IDLE_BUSY_LOAD:-1.0}"

die() { echo "ERROR: $*" >&2; exit 1; }

# Prints why the VM counts as active, or nothing if it is idle.
activity() {
  local reasons=() login load busy

  # Authenticated sessions are titled "sshd: user@pts/0" or "sshd: user@notty".
  # Connections that never log in carry no "@", so port scanners do not count.
  if pgrep -f '^sshd(-session)?: [^ ]+@' >/dev/null; then
    reasons+=("open SSH session")
  fi

  # Catches logins shorter than the check interval. The window overlaps the
  # previous check so that none falls between two checks.
  login="$(journalctl --quiet --identifier=sshd --identifier=sshd-session \
    --since="-$(( CHECK_INTERVAL_MIN * 2 ))min" --grep='^Accepted ' \
    --lines=1 --output=cat 2>/dev/null || true)"
  if [[ -n "$login" ]]; then
    reasons+=("recent SSH login")
  fi

  load="$(cut -d' ' -f3 /proc/loadavg)"
  busy="$(awk -v have="$load" -v want="$IDLE_BUSY_LOAD" 'BEGIN { print (have >= want) }')"
  if [[ "$busy" == 1 ]]; then
    reasons+=("load ${load}")
  fi

  local IFS=,
  echo "${reasons[*]-}"
}

metadata() {
  curl --fail --silent --show-error --max-time 10 \
    -H 'Metadata-Flavor: Google' \
    "http://metadata.google.internal/computeMetadata/v1/$1"
}

delete_self() {
  local zone name token
  zone="$(metadata instance/zone)" # projects/<number>/zones/<zone>
  name="$(metadata instance/name)"
  token="$(metadata instance/service-accounts/default/token | jq -er .access_token)"

  curl --fail --silent --show-error --max-time 30 -X DELETE \
    -H "Authorization: Bearer ${token}" \
    "https://compute.googleapis.com/compute/v1/${zone}/instances/${name}" \
    >/dev/null
}

cmd_check() {
  # Without a baseline there is no telling how long the VM has been idle.
  if [[ ! -e "$STAMP" ]]; then
    mkdir -p "$(dirname "$STAMP")"
    touch "$STAMP"
    echo "no baseline, starting the idle clock"
    return
  fi

  local reasons
  reasons="$(activity)"
  if [[ -n "$reasons" ]]; then
    touch "$STAMP"
    echo "active: ${reasons}"
    return
  fi

  # Capping at uptime keeps a VM that was stopped for days from being deleted
  # the moment it boots.
  local idle uptime
  idle=$(( $(date +%s) - $(stat -c %Y "$STAMP") ))
  uptime="$(cut -d. -f1 /proc/uptime)"
  if (( idle > uptime )); then
    idle="$uptime"
  fi

  if (( idle < IDLE_DELETE_AFTER_SECONDS )); then
    echo "idle for ${idle}s, deleting at ${IDLE_DELETE_AFTER_SECONDS}s"
    return
  fi

  echo "idle for ${idle}s, deleting this VM"
  delete_self
}

cmd_install() {
  local bin
  for bin in awk curl journalctl jq pgrep; do
    command -v "$bin" >/dev/null || die "$bin not found"
  done

  install -m 0755 "${BASH_SOURCE[0]}" "$INSTALL_PATH"
  mkdir -p "$(dirname "$STAMP")"
  touch "$STAMP"

  cat >"/etc/systemd/system/${UNIT}.service" <<EOF
[Unit]
Description=Delete this benchmark VM once it has been idle for too long

[Service]
Type=oneshot
EnvironmentFile=-/etc/default/${UNIT}
ExecStart=${INSTALL_PATH} check
EOF

  cat >"/etc/systemd/system/${UNIT}.timer" <<EOF
[Unit]
Description=Check this benchmark VM for inactivity

[Timer]
OnBootSec=${CHECK_INTERVAL_MIN}min
OnUnitActiveSec=${CHECK_INTERVAL_MIN}min

[Install]
WantedBy=timers.target
EOF

  systemctl daemon-reload
  systemctl enable --now "${UNIT}.timer"
}

(( EUID == 0 )) || die "must run as root"

case "${1:-}" in
  install) cmd_install ;;
  check)   cmd_check ;;
  *)       die "usage: $0 install|check" ;;
esac
