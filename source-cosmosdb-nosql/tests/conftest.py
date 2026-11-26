import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest


@pytest.fixture(scope="session")
def cosmosdb(tmp_path_factory: pytest.TempPathFactory):
    connector = Path(__file__).resolve().parent.parent
    compose = [
        "docker", "compose", "--project-name", f"source-cosmosdb-nosql-tests-{uuid4().hex}",
        "-f", str(connector / "docker-compose.yaml"),
    ]
    # Docker assigns a free port so the fixture can coexist with other emulators.
    env = {**os.environ, "COSMOSDB_PORT": "0"}
    network = subprocess.run(
        ["docker", "network", "inspect", "flow-test"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    if network.returncode:
        network = subprocess.run(
            ["docker", "network", "create", "flow-test"], stderr=subprocess.PIPE, text=True,
        )
        if network.returncode and "network with name flow-test already exists" not in network.stderr:
            network.check_returncode()
    try:
        subprocess.run([*compose, "up", "--wait", "--wait-timeout", "180"], env=env, check=True)
        address = subprocess.run(
            [*compose, "port", "db", "8081"], env=env, check=True,
            stdout=subprocess.PIPE, text=True,
        ).stdout.strip()
        endpoint = f"http://localhost:{address.rsplit(':', 1)[1]}/"
        subprocess.run(
            [sys.executable, str(connector / "tests" / "seed.py"), "--endpoint", endpoint],
            check=True,
        )
        catalog = tmp_path_factory.mktemp("cosmosdb") / "test.flow.yaml"
        catalog.write_text((connector / "test.flow.yaml").read_text().replace("http://localhost:8091/", endpoint))
        yield catalog
    finally:
        subprocess.run([*compose, "down", "-v"], env=env, check=False)
