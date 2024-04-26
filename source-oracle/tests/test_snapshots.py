import os
import time
import yaml
import json
import subprocess
import oracledb
import tempfile
from pathlib import Path


def test_capture(request, snapshot):
    sops = subprocess.run(['sops', '--decrypt', request.fspath.dirname + "/../config.yaml"], capture_output=True, text=True)
    config = yaml.safe_load(sops.stdout)

    dsn = config['address']
    creds = config['credentials']
    user = creds['username']
    password = creds['password_sops']
    credentials = {}
    if creds['credentials_title'] == 'Wallet':
        tmpdir = tempfile.TemporaryDirectory(delete=False)
        with open(f"{tmpdir.name}/tnsnames.ora", 'w') as f:
            f.write(creds['tnsnames'])

        with open(f"{tmpdir.name}/ewallet.pem", 'w') as f:
            f.write(creds['ewallet_sops'])

        credentials = {
            'config_dir': tmpdir.name,
            'wallet_location': tmpdir.name,
            'wallet_password': creds['wallet_password_sops'],
        }
    conn = oracledb.connect(
        user=user,
        password=password,
        dsn=dsn,
        **credentials,
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
            "1,1,1",
            "--delay",
            "1s",
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

    time.sleep(1)

    with conn.cursor() as c:
        c.execute("DELETE FROM test_changes WHERE id=2")
        c.execute("UPDATE test_changes SET str='updated str'")
    conn.commit()

    time.sleep(1)

    with conn.cursor() as c:
        c.execute("UPDATE test_changes SET str='updated str 2' WHERE id=3")
    conn.commit()

    out, _ = p.communicate(timeout=60)
    assert p.returncode == 0
    lines = [json.loads(l) for l in out.splitlines()[:50]]
    lines.sort(key=lambda doc: (doc[0], doc[1]['_meta'].get('scn', 0)))

    # clean up snapshot from non-deterministic values
    for _, doc in lines:
        source = doc['_meta']['source']
        if 'row_id' in source:
            source['row_id'] = '<row_id>'
        if 'scn' in source:
            source['scn'] = '<scn>'

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
