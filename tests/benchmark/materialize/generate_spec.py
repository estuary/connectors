#!/usr/bin/env python3
"""
Emit a flow.yaml describing the benchmark materialization.

Inputs:
  --scenario  path to scenario YAML (collections come from here)
  --connector connector name (e.g. materialize-postgres)
  --config    path to the connector's endpoint config (yaml/json), e.g.
              materialize-postgres/testdata/config.local.yaml
  --task      materialization task name (default: bench/<connector>)
  --output    output path (default: stdout)

By default the connector runs as a local binary (matching the integration
test convention), so config.local.yaml addresses like localhost:5435 work
out of the box. Pass --docker to run the connector as a Docker image instead
(useful for measuring container overhead or testing published images).

  --docker    use a Docker image instead of a local binary
  --image     full image ref (overrides default ghcr.io/estuary/<connector>:<tag>)
  --tag       image tag (default: $VERSION env or "local")

The scenario's `collections[].resource` (if present) is passed through to the
binding; otherwise we derive `{table: <basename>}` from the collection name.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Any

import yaml


def _table_name(collection_name: str) -> str:
    base = collection_name.rsplit("/", 1)[-1]
    return re.sub(r"[^A-Za-z0-9_]", "_", base)


def _load_yaml(path: str) -> Any:
    with open(path) as f:
        return yaml.safe_load(f)


def _rewrite_localhost_for_docker(config: dict, compose_path: str) -> dict:
    """In Docker mode the connector runs inside a container on the flow-test
    network. ``localhost:HOST_PORT`` from config.local.yaml won't reach the
    host — rewrite it to the docker-compose container's internal address.

    The container name follows the compose v2 convention:
    ``<project>-<service>-<replica>`` where project = parent directory name.
    """
    if "address" not in config:
        return config

    addr = config["address"]
    m = re.match(r"(localhost|127\.0\.0\.1):(\d+)(.*)", addr)
    if not m:
        return config

    host_port = m.group(1 + 1)  # the port group
    trailing = m.group(3)

    compose = _load_yaml(compose_path)
    project = os.path.basename(os.path.dirname(os.path.abspath(compose_path)))

    for svc_name, svc in (compose.get("services") or {}).items():
        for port_spec in svc.get("ports", []):
            pm = re.match(r"(\d+):(\d+)", str(port_spec))
            if pm and pm.group(1) == host_port:
                container = f"{project}-{svc_name}-1"
                internal_port = pm.group(2)
                config = dict(config)
                config["address"] = f"{container}:{internal_port}{trailing}"
                return config

    return config


def build_spec(
    scenario_path: str,
    connector: str,
    config_path: str,
    docker: bool = False,
    image: str | None = None,
    tag: str | None = None,
    task: str | None = None,
    compose_file: str | None = None,
) -> dict:
    scenario = _load_yaml(scenario_path)

    if task is None:
        task = f"bench/{connector}"

    collections_out: dict[str, dict] = {}
    bindings_out: list[dict] = []

    for c in scenario.get("collections", []):
        name = c["name"]
        collections_out[name] = {
            "schema": c["schema"],
            "key": c["key"],
        }
        resource = c.get("resource") or {"table": _table_name(name)}
        bindings_out.append({"source": name, "resource": resource})

    config = _load_yaml(config_path)

    if docker:
        # Rewrite localhost addresses to docker-internal hostnames so
        # the existing config.local.yaml works in --docker mode too.
        if compose_file and os.path.isfile(compose_file):
            config = _rewrite_localhost_for_docker(config, compose_file)
        if image is None:
            tag = tag or os.environ.get("VERSION", "local")
            image = f"ghcr.io/estuary/{connector}:{tag}"
        endpoint = {"connector": {"image": image, "config": config}}
    else:
        endpoint = {
            "local": {
                "command": [f"./{connector}/connector"],
                "protobuf": True,
                "config": config,
            }
        }

    return {
        "collections": collections_out,
        "materializations": {
            task: {
                "endpoint": endpoint,
                "bindings": bindings_out,
            }
        },
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--scenario", required=True)
    p.add_argument("--connector", required=True)
    p.add_argument("--config", required=True)
    p.add_argument("--docker", action="store_true",
                   help="Run connector as Docker image instead of local binary")
    p.add_argument("--image", default=None)
    p.add_argument("--tag", default=None)
    p.add_argument("--task", default=None)
    p.add_argument("--output", default="-")
    args = p.parse_args(argv)

    # Auto-discover the compose file for localhost rewriting in --docker mode.
    compose_file = None
    if args.docker:
        repo_root = os.environ.get("ROOT_DIR", os.getcwd())
        for cand in [
            os.path.join(repo_root, "tests", "materialize", args.connector, "docker-compose.yaml"),
            os.path.join(repo_root, args.connector, "docker-compose.yaml"),
        ]:
            if os.path.isfile(cand):
                compose_file = cand
                break

    spec = build_spec(
        scenario_path=args.scenario,
        connector=args.connector,
        config_path=args.config,
        docker=args.docker,
        image=args.image,
        tag=args.tag,
        task=args.task,
        compose_file=compose_file,
    )

    text = yaml.safe_dump(spec, sort_keys=False, default_flow_style=False)
    if args.output == "-":
        sys.stdout.write(text)
    else:
        with open(args.output, "w") as f:
            f.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
