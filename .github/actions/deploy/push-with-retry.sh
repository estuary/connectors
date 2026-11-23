#!/usr/bin/env bash
# Run a command that pushes one image, retrying transient registry failures.
#
# Usage: push-with-retry.sh IMAGE COMMAND [ARG...]
#
# IMAGE only names the image in messages; COMMAND does the push.
#
# GHCR occasionally rejects a push partway through (e.g. `unknown blob`),
# most often when several jobs push the same base layers at once. Layers the
# registry already has are skipped on a retry, so a retry mostly re-sends
# what failed.
#
# Many concurrent pushes can also exceed GHCR's request rate limit
# (`toomanyrequests`). Each delay gets up to as much again at random, so
# jobs that failed together don't all retry together.
set -uo pipefail

IMAGE="$1"
shift
DELAYS=(15 30 60 120)
ATTEMPTS=$((${#DELAYS[@]} + 1))

for attempt in $(seq "$ATTEMPTS"); do
    if "$@"; then
        exit 0
    fi
    if [ "$attempt" -eq "$ATTEMPTS" ]; then
        echo "::error::pushing ${IMAGE} failed after ${attempt} attempts"
        exit 1
    fi
    base=${DELAYS[$((attempt - 1))]}
    delay=$((base + RANDOM % (base + 1)))
    echo "::warning::pushing ${IMAGE} failed (attempt ${attempt}); retrying in ${delay}s"
    sleep "$delay"
done
