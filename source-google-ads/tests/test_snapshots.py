import json
import subprocess


# Disabled for now until we get a test account, don't want to check in
# real data from our account here.
# def test_capture(request, snapshot):
#     result = subprocess.run(
#         [
#             "flowctl",
#             "preview",
#             "--source",
#             request.config.rootdir + "/source-google-ads/test.flow.yaml",
#             "--sessions",
#             "1",
#             "--delay",
#             "20s"
#         ],
#         stdout=subprocess.PIPE,
#         text=True,
#     )
#     assert result.returncode == 0
#     lines = [json.loads(l) for l in result.stdout.splitlines()]

#     for l in lines:
#         l[1]["ts"] = "redacted-timestamp"

#     assert snapshot("capture.stdout.json") == lines

def test_discover(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            request.config.rootdir + "/source-google-ads/test.flow.yaml",
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
            request.config.rootdir + "/source-google-ads/test.flow.yaml"
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    assert snapshot("capture.stdout.json") == lines
