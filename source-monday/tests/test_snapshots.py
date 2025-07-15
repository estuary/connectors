import json
import subprocess


def test_capture(request, snapshot):
    FIELDS_TO_REDACT = [
        "utc_hours_diff",
        "updated_at",
    ]

    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "250s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    
    lines = [json.loads(l) for l in result.stdout.splitlines()]
    
    stream_data = {}
    for line in lines:
        stream = line[0]
        if stream not in stream_data:
            stream_data[stream] = []
        stream_data[stream].append(line[1])
    
    unique_stream_lines = []
    for stream in sorted(stream_data.keys()):
        records = stream_data[stream]
        if not records:
            continue
            
        # Create a stable sort key that considers multiple fields
        def sort_key(r):
            # Primary sort by id if available
            primary = r.get('id', '')
            # Secondary sort by name if available
            secondary = r.get('name', '')
            # Tertiary sort by board.id for items
            tertiary = r.get('board', {}).get('id', '') if 'board' in r else ''
            return (primary, secondary, tertiary)
        
        # Sort all records with the stable key
        records.sort(key=sort_key)
        
        # Always take the first record after sorting
        unique_stream_lines.append([stream, records[0]])
    
    for line in unique_stream_lines:
        _stream, rec = line[0], line[1]

        for field in FIELDS_TO_REDACT:
            if field in rec:
                rec[field] = 'redacted'

    assert snapshot("stdout.json") == unique_stream_lines


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

    assert snapshot("stdout.json") == lines


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

    assert snapshot("stdout.json") == lines
