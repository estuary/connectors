import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from .conftest import cosmosdb


@pytest.mark.parametrize("network", ["exists", "created", "raced"])
def test_snapshot_fixtures_use_separate_projects_and_ports(monkeypatch, tmp_path_factory, network):
    ports = iter([49152, 49153])

    def run(command, **kwargs):
        if command == ["docker", "network", "inspect", "flow-test"]:
            return subprocess.CompletedProcess(command, 0 if network == "exists" else 1)
        if command == ["docker", "network", "create", "flow-test"]:
            return subprocess.CompletedProcess(
                command, 1 if network == "raced" else 0,
                stderr="Error response from daemon: network with name flow-test already exists",
            )
        if command[-3:] == ["port", "db", "8081"]:
            return subprocess.CompletedProcess(command, 0, stdout=f"0.0.0.0:{next(ports)}\n")
        return subprocess.CompletedProcess(command, 0)

    runner = Mock(side_effect=run)
    monkeypatch.setattr(subprocess, "run", runner)
    monkeypatch.setenv("COSMOSDB_PORT", "8091")
    connector = Path(__file__).resolve().parent.parent
    original = (connector / "test.flow.yaml").read_text()
    fixtures = [cosmosdb.__wrapped__(tmp_path_factory) for _ in range(2)]
    try:
        catalogs = [next(fixture) for fixture in fixtures]
        assert catalogs[0] != catalogs[1]
        for catalog, port in zip(catalogs, [49152, 49153]):
            assert catalog.read_text() == original.replace("http://localhost:8091/", f"http://localhost:{port}/")
        assert not any("down" in call.args[0] for call in runner.call_args_list)
    finally:
        for fixture in fixtures:
            fixture.close()

    assert (connector / "test.flow.yaml").read_text() == original
    assert [call.args[0] for call in runner.call_args_list].count(
        ["docker", "network", "create", "flow-test"],
    ) == (0 if network == "exists" else 2)
    calls = [call for call in runner.call_args_list if call.args[0][:2] == ["docker", "compose"]]
    assert all(call.kwargs["env"]["COSMOSDB_PORT"] == "0" for call in calls)
    starts = [call.args[0] for call in calls if "up" in call.args[0]]
    stops = [call.args[0] for call in calls if "down" in call.args[0]]
    projects = [command[command.index("--project-name") + 1] for command in starts]
    assert len(set(projects)) == 2
    assert all(command[-4:] == ["up", "--wait", "--wait-timeout", "180"] for command in starts)
    assert all(command[-6:-4] == ["-f", str(connector / "docker-compose.yaml")] for command in starts)
    assert stops == [[*start[:-4], "down", "-v"] for start in starts]
    assert [call.args[0] for call in runner.call_args_list if call.args[0][0] == sys.executable] == [
        [sys.executable, str(connector / "tests" / "seed.py"), "--endpoint", f"http://localhost:{port}/"]
        for port in [49152, 49153]
    ]


def test_snapshot_fixture_propagates_network_creation_failure(monkeypatch, tmp_path_factory):
    error = "Cannot connect to the Docker daemon"
    runner = Mock(side_effect=lambda command, **kwargs: subprocess.CompletedProcess(command, 1, stderr=error))
    monkeypatch.setattr(subprocess, "run", runner)
    fixture = cosmosdb.__wrapped__(tmp_path_factory)
    with pytest.raises(subprocess.CalledProcessError) as exc:
        next(fixture)
    assert exc.value.cmd == ["docker", "network", "create", "flow-test"]
    assert exc.value.stderr == error
    assert runner.call_count == 2


@pytest.mark.parametrize("failure", ["up", "port", "seed"])
def test_snapshot_fixture_cleans_up_after_setup_failure(monkeypatch, tmp_path_factory, failure):
    error = subprocess.CalledProcessError(1, failure)

    def run(command, **kwargs):
        if failure in command or (failure == "seed" and command[0] == sys.executable):
            raise error
        return subprocess.CompletedProcess(command, 0, stdout="0.0.0.0:49152\n")

    runner = Mock(side_effect=run)
    monkeypatch.setattr(subprocess, "run", runner)
    fixture = cosmosdb.__wrapped__(tmp_path_factory)
    with pytest.raises(subprocess.CalledProcessError) as exc:
        next(fixture)
    assert exc.value is error
    start = next(call.args[0] for call in runner.call_args_list if "up" in call.args[0])
    assert runner.call_args.args[0] == [*start[:-4], "down", "-v"]
    assert runner.call_args.kwargs["env"]["COSMOSDB_PORT"] == "0"
    assert runner.call_args.kwargs["check"] is False
