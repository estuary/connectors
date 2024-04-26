import json
import subprocess

import pytest_insta.format as insta_format
insta_format.FmtJson.dump = lambda _self, path, value: path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", "utf-8")

def test_capture(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "20s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()[:50]]

    for l in lines:
        typ, rec = l[0], l[1]

        if typ == "acmeCo/lists":
            rec["contact"] = "redacted" # removing address info from tests
            rec["stats"]["last_sub_date"] = "redacted"

        elif typ == "acmeCo/segment_members":
            rec["ip_opt"] = "redacted"

    assert snapshot("capture.stdout.json") == lines

def test_discover(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            request.config.rootdir + "/source-mailchimp/test.flow.yaml",
            "-o",
            "json",
            "--emit-raw"
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
            request.config.rootdir + "/source-mailchimp/test.flow.yaml"
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    assert snapshot("capture.stdout.json") == lines
 
