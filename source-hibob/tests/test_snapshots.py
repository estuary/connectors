import json
import re
import subprocess

import pytest

# The capture test runs against a live HiBob partner account, which can expire, be
# rate-limited or go down. Every PR touching estuary-cdk or the lockfile runs this
# suite, so an outage on HiBob's side must skip the test rather than block those PRs.
HIBOB_UNAVAILABLE = re.compile(
    # 400s and 404s are left to fail: they're more likely a connector bug.
    r"Encountered HTTP error status (401|403|429|5\d\d)"
    r"|can't see any employees"
    r"|Cannot connect to host"
    r"|ServerDisconnectedError"
    r"|TimeoutError"
)

# Fields every document of each stream must carry. Values aren't compared: the tenant's
# data changes (HiBob recomputes fields, people get edited), so an exact snapshot would
# break without any connector change.
REQUIRED_FIELDS = {
    "acmeCo/employees": ["id"],
    "acmeCo/employee_fields": ["id", "jsonPath"],
    "acmeCo/named_lists": ["name", "items"],
    "acmeCo/employment_history": ["employeeId", "id"],
    "acmeCo/lifecycle_history": ["employeeId", "id"],
    "acmeCo/work_history": ["employeeId", "id"],
}


def test_capture(request):
    try:
        result = subprocess.run(
            [
                "flowctl",
                "raw",
                "preview-next",
                "--source",
                request.fspath.dirname + "/../test.flow.yaml",
                "--sessions",
                "1",
                "--delay",
                "10s",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=600,
        )
    except subprocess.TimeoutExpired:
        pytest.skip("HiBob capture timed out")

    if result.returncode != 0 and HIBOB_UNAVAILABLE.search(result.stderr):
        pytest.skip(f"HiBob is unavailable:\n{result.stderr[-2000:]}")
    assert result.returncode == 0, result.stderr[-2000:]

    docs: dict[str, list[dict]] = {stream: [] for stream in REQUIRED_FIELDS}
    for line in result.stdout.splitlines():
        stream, doc = json.loads(line)
        docs[stream].append(doc)

    for stream, fields in REQUIRED_FIELDS.items():
        assert docs[stream], f"{stream} captured no documents"
        for doc in docs[stream]:
            missing = [f for f in fields if f not in doc]
            assert not missing, f"{stream} document missing {missing}"

    # HiBob returns every field twice; only the nested copy should be kept.
    for doc in docs["acmeCo/employees"]:
        assert not [k for k in doc if k.startswith("/")]

    # History entry ids are only unique per employee.
    for stream in [
        "acmeCo/employment_history",
        "acmeCo/lifecycle_history",
        "acmeCo/work_history",
    ]:
        keys = [(d["employeeId"], d["id"]) for d in docs[stream]]
        assert len(keys) == len(set(keys)), f"{stream} has duplicate (employeeId, id)"


def test_discover(request, snapshot):
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

    assert snapshot("capture.stdout.json") == lines


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
