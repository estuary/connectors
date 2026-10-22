#!/usr/bin/env bash
#
# Benchmark setup for materialize-spanner against the local emulator: create
# the instance and database named by $BENCH_CONFIG, as main_test.go does,
# through the emulator's REST port.

set -o errexit
set -o pipefail
set -o nounset

[[ -n "${BENCH_CONFIG:-}" && -f "$BENCH_CONFIG" ]] || {
  echo "BENCH_CONFIG must point to the connector config YAML" >&2
  exit 1
}

python3 - "$BENCH_CONFIG" <<'PY'
import json, sys, time, urllib.error, urllib.request, yaml

cfg = yaml.safe_load(open(sys.argv[1]))
project, instance, database = cfg["project_id"], cfg["instance_id"], cfg["database"]
rest = "http://localhost:9020/v1"

def post(path, body):
    req = urllib.request.Request(rest + path, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status
    except urllib.error.HTTPError as err:
        return err.code

for _ in range(60):
    try:
        status = post(f"/projects/{project}/instances", {
            "instanceId": instance,
            "instance": {
                "config": f"projects/{project}/instanceConfigs/emulator-config",
                "displayName": instance,
                "nodeCount": 1,
            },
        })
        break
    except urllib.error.URLError:
        time.sleep(1)
else:
    sys.exit("Spanner emulator did not come up")
if status not in (200, 409):
    sys.exit(f"creating instance: HTTP {status}")

status = post(f"/projects/{project}/instances/{instance}/databases",
              {"createStatement": f"CREATE DATABASE `{database}`"})
if status not in (200, 409):
    sys.exit(f"creating database: HTTP {status}")
PY
