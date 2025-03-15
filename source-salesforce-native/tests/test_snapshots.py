import json
import subprocess

from source_salesforce_native.supported_standard_objects import SUPPORTED_STANDARD_OBJECTS


def test_discover(request, snapshot):
    enabled_bindings: list[str] = []
    for name in SUPPORTED_STANDARD_OBJECTS.keys():
        if SUPPORTED_STANDARD_OBJECTS[name].get("enabled_by_default", False):
            enabled_bindings.append(name)

    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "-o",
            "json",
            "--emit-raw",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]
    enabled_lines = [l for l in lines if l["recommendedName"] in enabled_bindings]

    assert snapshot("capture.stdout.json") == enabled_lines


def test_spec(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "spec",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    assert snapshot("capture.stdout.json") == lines
