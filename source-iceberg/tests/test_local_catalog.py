import subprocess
from pathlib import Path

import pytest

from . import conftest


def test_local_catalog_isolates_sessions(monkeypatch, tmp_path_factory):
    projects = {}
    stopped = []
    seeded = []
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "existing-fixture")
    monkeypatch.setenv("ICEBERG_CATALOG_PORT", "8181")
    monkeypatch.setenv("ICEBERG_STORAGE_PORT", "9010")

    def run(command, **kwargs):
        assert command[:3] == ["docker", "compose", "--project-name"]
        project = command[3]
        assert project.startswith("source-iceberg-pytest-")
        assert kwargs["env"]["ICEBERG_CATALOG_PORT"] == "0"
        assert kwargs["env"]["ICEBERG_STORAGE_PORT"] == "0"
        assert kwargs["check"]
        stdout = ""
        match command[6:]:
            case ["up", "--wait", "--wait-timeout", "180"]:
                assert project not in projects
                port = 49152 + len(projects) * 2
                projects[project] = {"db": port, "storage": port + 1}
            case ["port", service, port]:
                assert port == {"db": "8181", "storage": "9000"}[service]
                stdout = f"0.0.0.0:{projects[project][service]}\n[::]:{projects[project][service]}\n"
            case ["down", "-v"]:
                stopped.append(project)
            case _:
                pytest.fail(f"Unexpected Compose command: {command}")
        return subprocess.CompletedProcess(command, 0, stdout=stdout)

    monkeypatch.setattr(conftest.subprocess, "run", run)
    monkeypatch.setattr(conftest, "seed_catalog", lambda **kwargs: seeded.append(kwargs))
    sessions = [conftest.local_catalog.__wrapped__(tmp_path_factory) for _ in range(2)]
    template = (Path(__file__).parents[1] / "test.flow.yaml").read_text()
    try:
        sources = [next(session) for session in sessions]
        assert len(projects) == len(seeded) == 2
        assert sources[0] != sources[1]
        for source, endpoints, ports in zip(sources, seeded, projects.values()):
            assert endpoints == {
                "catalog_uri": f"http://localhost:{ports['db']}",
                "storage_endpoint": f"http://localhost:{ports['storage']}",
            }
            assert source.read_text() == (
                template.replace("http://localhost:8181", endpoints["catalog_uri"])
                .replace("http://localhost:9010", endpoints["storage_endpoint"])
            )
        assert stopped == []
    finally:
        for session in sessions:
            session.close()
    assert stopped == list(projects)


@pytest.mark.parametrize("failure", ["up", "port", "seed"])
def test_local_catalog_cleans_up_failed_setup(monkeypatch, tmp_path_factory, failure):
    commands = []
    error = RuntimeError("fixture setup failed")

    def run(command, **kwargs):
        commands.append(command)
        if command[6] == failure:
            raise error
        return subprocess.CompletedProcess(command, 0, stdout="0.0.0.0:49152\n")

    def seed(**kwargs):
        raise error

    monkeypatch.setattr(conftest.subprocess, "run", run)
    monkeypatch.setattr(conftest, "seed_catalog", seed)
    session = conftest.local_catalog.__wrapped__(tmp_path_factory)
    with pytest.raises(RuntimeError) as exc:
        next(session)
    assert exc.value is error
    assert commands[-1] == [*commands[0][:6], "down", "-v"]
