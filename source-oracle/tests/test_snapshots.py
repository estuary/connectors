import os
import time
import json
import subprocess
import oracledb
from pathlib import Path


def test_capture(request, snapshot):
    user = os.environ['ORACLE_USER']
    password = os.environ['ORACLE_PASSWORD']
    dsn = os.environ['ORACLE_DSN']
    conn = oracledb.connect(
        user=user,
        password=password,
        dsn=dsn,
    )

    # seed the test database first
    try:
        with conn.cursor() as c:
            c.execute("DROP TABLE test_all_types")
        with conn.cursor() as c:
            c.execute("DROP TABLE test_changes")
    except Exception as e:
        # do nothing
        print("tables did not exist, ignoring", e)

    with conn.cursor() as c:
        for f in ['create_test_all_types.sql', 'create_test_changes.sql', 'insert_test_all_types.sql']:
            q = Path(request.fspath.dirname + "/db_seeds/" + f).read_text()
            c.execute(q)
    conn.commit()

    p = subprocess.Popen(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "10s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )

    time.sleep(10)

    with conn.cursor() as c:
        c.execute("INSERT INTO test_changes(id, str) VALUES (1, 'record 1')")
        c.execute("INSERT INTO test_changes(id, str) VALUES (2, 'record 2')")
        c.execute("INSERT INTO test_changes(id, str) VALUES (3, 'record 3')")
    conn.commit()

    time.sleep(2)

    with conn.cursor() as c:
        c.execute("DELETE FROM test_changes WHERE id=2")
        c.execute("UPDATE test_changes SET str='updated str'")
    conn.commit()

    with conn.cursor() as c:
        c.execute("UPDATE test_changes SET str='updated str 2' WHERE id=3")
    conn.commit()

    out, err = p.communicate(timeout=20)
    assert p.returncode == 0
    lines = [json.loads(l) for l in out.splitlines()[:50]]

    assert snapshot("stdout.json") == lines


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
