import json
import subprocess


def test_capture(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "--sessions",
            "27",
            "--delay",
            "10s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    unique_stream_lines = []
    seen = set()

    for line in lines:
        stream = line[0]
        if stream not in seen:
            unique_stream_lines.append(line)
            seen.add(stream)

    streams_with_updated_at = [
        "acmeCo/group_memberships",
        "acmeCo/groups",
        "acmeCo/macros",
        "acmeCo/organizations",
        "acmeCo/organization_memberships",
        "acmeCo/posts",
        "acmeCo/post_comments",
        "acmeCo/post_comment_votes",
        "acmeCo/post_votes",
        "acmeCo/satisfaction_ratings",
        "acmeCo/sla_policies",
        "tacmeCo/icket_fields",
        "acmeCo/ticket_metrics",
        "acmeCo/ticket_skips",
        "acmeCo/tickets",
        "acmeCo/users",
        "acmeCo/brands",
        "acmeCo/custom_roles",
        "acmeCo/schedules",
        "acmeCo/ticket_forms",
        "acmeCo/account_attributes",
        "acmeCo/attribute_definitions"
    ]

    for l in unique_stream_lines:
        type, rec = l[0], l[1]

        rec['_meta']['row_id'] = 0
        if type in streams_with_updated_at:
            rec["updated_at"] = "redacted"
        if type == "acmeCo/users":
            rec["last_login_at"] = "redacted"


    assert snapshot("capture.stdout.json") == unique_stream_lines


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
