#!/usr/bin/env bash
#
# Benchmark setup for materialize-s3-parquet: create the bucket in the local
# rustfs container before the connector runs. The connector itself does not
# create buckets; integration tests (driver_test.go) handle this in Go.
#
# Reads bucket / endpoint / credentials from the connector config YAML passed
# in as $BENCH_CONFIG (set by run.sh).

set -o errexit
set -o pipefail
set -o nounset

[[ -n "${BENCH_CONFIG:-}" && -f "$BENCH_CONFIG" ]] || {
  echo "BENCH_CONFIG must point to the connector config YAML" >&2
  exit 1
}

command -v aws >/dev/null || {
  echo "aws CLI required for materialize-s3-parquet benchmark setup" >&2
  exit 1
}

eval "$(python3 - "$BENCH_CONFIG" <<'PY'
import sys, shlex, yaml
with open(sys.argv[1]) as f:
    cfg = yaml.safe_load(f)
creds = cfg.get("credentials") or {}
fields = {
    "BUCKET":   cfg["bucket"],
    "REGION":   cfg["region"],
    "ENDPOINT": cfg.get("endpoint", ""),
    "KEY":      creds.get("awsAccessKeyId")     or cfg.get("awsAccessKeyId", ""),
    "SECRET":   creds.get("awsSecretAccessKey") or cfg.get("awsSecretAccessKey", ""),
}
for k, v in fields.items():
    print(f"{k}={shlex.quote(str(v))}")
PY
)"

export AWS_ACCESS_KEY_ID="$KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET"
export AWS_DEFAULT_REGION="$REGION"

aws_args=()
if [[ -n "$ENDPOINT" ]]; then
  aws_args+=(--endpoint-url "$ENDPOINT")
fi

if aws "${aws_args[@]}" s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "bucket $BUCKET already exists${ENDPOINT:+ at $ENDPOINT}"
else
  echo "creating bucket $BUCKET${ENDPOINT:+ at $ENDPOINT}"
  aws "${aws_args[@]}" s3 mb "s3://$BUCKET"
fi
