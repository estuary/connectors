#!/usr/bin/env bash
# Push one image, retrying transient registry failures.
#
# GHCR occasionally rejects a push partway through (e.g. `unknown blob`),
# most often when several jobs push the same base layers at once. Layers the
# registry already has are skipped on a retry, so a retry mostly re-sends
# what failed.
set -uo pipefail

IMAGE="$1"
DELAYS=(10 30 60)

for attempt in 1 2 3 4; do
    if docker image push "$IMAGE"; then
        exit 0
    fi
    if [ "$attempt" -eq 4 ]; then
        echo "::error::pushing ${IMAGE} failed after ${attempt} attempts"
        exit 1
    fi
    delay=${DELAYS[$((attempt - 1))]}
    echo "::warning::pushing ${IMAGE} failed (attempt ${attempt}); retrying in ${delay}s"
    sleep "$delay"
done
