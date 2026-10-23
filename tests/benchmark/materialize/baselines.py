"""Helpers for baselines.sh.

  baselines.py prepare-config CONNECTOR SRC DST
      Copy CONNECTOR's test config SRC to DST, ready for a benchmark run.

  baselines.py record RESULTS DST
      Write a run's RESULTS (results.json) to DST as a baseline, adding the
      environment the run measured.

A sops-encrypted config stays encrypted in DST, and its decrypted values never
appear on a command line, which every local user can read through
/proc/<pid>/cmdline.
"""

import json
import os
import shutil
import subprocess
import sys

import yaml

# Test-config feature flags which make the runtime Load every key, even against
# a table the run has just created, so a run would measure the runtime.
LOAD_EVERY_KEY_FLAGS = {
    "allow_existing_tables_for_new_bindings",
    "retain_existing_data_on_backfill",
}

# Fabric Warehouse detects snapshot conflicts per table, so commits by other
# tasks to a shared schema's flow_checkpoints_v1 abort the run's own commits.
DEDICATED_SCHEMAS = {
    "materialize-azure-fabric-warehouse": "flow_bench_tpcds",
}


def benchmark_edits(connector, config):
    """Top-level config keys to change for benchmarking, with their new values."""
    edits = {}

    advanced = config.get("advanced") or {}
    if "feature_flags" in advanced:
        flags = [
            flag for flag in (advanced["feature_flags"] or "").split(",")
            if flag and flag not in LOAD_EVERY_KEY_FLAGS
        ]
        edits["advanced"] = {**advanced, "feature_flags": ",".join(flags)}

    if connector in DEDICATED_SCHEMAS:
        edits["schema"] = DEDICATED_SCHEMAS[connector]

    return edits


def prepare_config(connector, src, dst):
    with open(src) as f:
        encrypted = "sops" in (yaml.safe_load(f) or {})

    if not encrypted:
        with open(src) as f:
            config = yaml.safe_load(f)
        config.update(benchmark_edits(connector, config))
        with open(dst, "w") as f:
            yaml.safe_dump(config, f, sort_keys=False)
        return

    plain = subprocess.run(["sops", "-d", src], check=True, capture_output=True).stdout
    shutil.copyfile(src, dst)
    for key, value in benchmark_edits(connector, yaml.safe_load(plain)).items():
        subprocess.run(
            ["sops", "set", "--value-stdin", dst, f'["{key}"]'],
            input=json.dumps(value).encode(),
            check=True,
        )


def record(results, dst):
    with open(results) as f:
        baseline = json.load(f)
    flowctl = subprocess.run(["flowctl", "--version"], capture_output=True, text=True)
    baseline["environment"] = {
        "machine": os.environ.get("MACHINE_TYPE") or None,
        "flowctl": flowctl.stdout.strip() or None,
        "flow_commit": os.environ.get("FLOW_COMMIT") or None,
        "connectors_commit": os.environ.get("CONNECTORS_COMMIT") or None,
    }
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(dst, "w") as f:
        json.dump(baseline, f, indent=2)


def main(argv):
    command, args = argv[1] if len(argv) > 1 else None, argv[2:]
    if command == "prepare-config" and len(args) == 3:
        prepare_config(*args)
    elif command == "record" and len(args) == 2:
        record(*args)
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv)
