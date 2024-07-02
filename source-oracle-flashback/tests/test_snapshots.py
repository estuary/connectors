import os
import sys
import time
import yaml
import json
import subprocess
import oracledb
import tempfile
from pathlib import Path
from logging import Logger

from source_oracle_flashback.ssh_tunnel import ssh_tunnel


def connect(request, user=None, password=None):
    sops = subprocess.run(['sops', '--decrypt', request.fspath.dirname + "/../config.yaml"], capture_output=True, text=True)
    config = yaml.safe_load(sops.stdout)

    dsn = config['address']
    creds = config['credentials']
    user = user or creds['username']
    password = password or creds['password_sops']
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

    if config['networkTunnel'] is not None:
        ssh_forwarding = config['networkTunnel']['sshForwarding']
        params = oracledb.ConnectParams()
        params.parse_connect_string(dsn)
        host = params.host
        port = params.port

        dsn = dsn.replace(host, 'localhost')

        log = Logger(name='test_snapshots')
        ssh_tunnel(
            log=log,
            endpoint=ssh_forwarding['sshEndpoint'],
            key=ssh_forwarding['privateKey_sops'],
            remote_bind_address=(host, port),
            local_bind_port=port,
        )

    return oracledb.connect(
        user=user,
        password=password,
        dsn=dsn,
        **credentials,
    )


def test_capture_all_types(request, snapshot):
    conn = connect(request)

    # seed the test database first
    try:
        with conn.cursor() as c:
            c.execute("DROP TABLE test_all_types")
    except Exception as e:
        # do nothing
        print("tabls did not exist, ignoring", e)

    with conn.cursor() as c:
        for f in ['create_test_all_types.sql', 'insert_test_all_types.sql']:
            q = Path(request.fspath.dirname + "/db_seeds/" + f).read_text()
            c.execute(q)
    conn.commit()

    env = os.environ
    env['RUST_LOG'] = 'debug'
    p = subprocess.Popen(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test-types.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "1s",
        ],
        stdout=subprocess.PIPE,
        text=True,
        env=env,
    )

    lines = []
    lines.append(json.loads(p.stdout.readline()))

    assert p.wait(timeout=30) == 0

    # clean up snapshot from non-deterministic values
    for _, doc in lines:
        source = doc['_meta']['source']
        if 'row_id' in source:
            source['row_id'] = '<row_id>'
        if 'scn' in source:
            source['scn'] = '<scn>'

    assert snapshot("stdout.json") == lines


# larger than backfill_chunk_size backfills
def test_capture_large_backfill(request, snapshot):
    conn = connect(request)

    # seed the test database first
    try:
        with conn.cursor() as c:
            c.execute("DROP TABLE test_changes")
    except Exception as e:
        # do nothing
        print("tables did not exist, ignoring", e)

    with conn.cursor() as c:
        c.execute(Path(request.fspath.dirname + "/db_seeds/create_test_changes.sql").read_text())
    conn.commit()

    backfill_n = 128
    with conn.cursor() as c:
        for i in range(backfill_n):
            c.execute(f"INSERT INTO test_changes(id, str) VALUES ({i}, 'record {i}')")
    conn.commit()

    env = os.environ
    env['RUST_LOG'] = 'debug'
    p = subprocess.Popen(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test-changes.flow.yaml",
            "--sessions",
            "2",
            "--delay",
            "1s",
        ],
        stdout=subprocess.PIPE,
        text=True,
        env=env,
    )

    inc_n = 32
    with conn.cursor() as c:
        for i in range(backfill_n, backfill_n + inc_n):
            c.execute(f"INSERT INTO test_changes(id, str) VALUES ({i}, 'record {i}')")
    conn.commit()

    lines = []
    rowids = []
    for i in range(backfill_n + inc_n):
        line = p.stdout.readline()
        try:
            lines.append(json.loads(line))
        except Exception:
            print("invalid JSON output", line, file=sys.stderr)
            raise
        rowids.append(lines[i][1]['_meta']['source']['row_id'])

    # expect all rowids to be unique
    assert len(rowids) == len(set(rowids))

    assert p.wait(timeout=30) == 0

    # clean up snapshot from non-deterministic values
    for _, doc in lines:
        source = doc['_meta']['source']
        if 'row_id' in source:
            source['row_id'] = '<row_id>'
        if 'scn' in source:
            source['scn'] = '<scn>'

    assert snapshot("stdout.json") == lines


def test_capture_changes(request, snapshot):
    conn = connect(request)

    # seed the test database first
    try:
        with conn.cursor() as c:
            c.execute("DROP TABLE test_changes")
    except Exception as e:
        # do nothing
        print("tables did not exist, ignoring", e)

    with conn.cursor() as c:
        c.execute(Path(request.fspath.dirname + "/db_seeds/create_test_changes.sql").read_text())
    conn.commit()

    env = os.environ
    env['RUST_LOG'] = 'debug'
    p = subprocess.Popen(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test-changes.flow.yaml",
            "--sessions",
            "1,1,1",
            "--delay",
            "1s",
        ],
        stdout=subprocess.PIPE,
        text=True,
        env=env,
    )

    with conn.cursor() as c:
        c.execute("INSERT INTO test_changes(id, str) VALUES (1, 'record 1')")
        c.execute("INSERT INTO test_changes(id, str) VALUES (2, 'record 2')")
        c.execute("INSERT INTO test_changes(id, str) VALUES (3, 'record 3')")
    conn.commit()

    lines = []
    # expect to have three lines in the output
    lines.append(json.loads(p.stdout.readline()))
    lines.append(json.loads(p.stdout.readline()))
    lines.append(json.loads(p.stdout.readline()))

    with conn.cursor() as c:
        c.execute("DELETE FROM test_changes WHERE id=2")
        c.execute("UPDATE test_changes SET str='updated str'")
    conn.commit()

    # expect to have three lines in the output
    lines.append(json.loads(p.stdout.readline()))
    lines.append(json.loads(p.stdout.readline()))
    lines.append(json.loads(p.stdout.readline()))

    with conn.cursor() as c:
        c.execute("UPDATE test_changes SET str='updated str 2' WHERE id=3")
    conn.commit()

    # expect to have one new line in the output
    lines.append(json.loads(p.stdout.readline()))

    assert p.wait(timeout=30) == 0

    # clean up snapshot from non-deterministic values
    for _, doc in lines:
        source = doc['_meta']['source']
        if 'row_id' in source:
            source['row_id'] = '<row_id>'
        if 'scn' in source:
            source['scn'] = '<scn>'

    assert snapshot("stdout.json") == lines


def test_capture_incremental_multiple_transactions(request, snapshot):
    conn = connect(request)

    # seed the test database first
    try:
        with conn.cursor() as c:
            c.execute("DROP TABLE test_changes")
    except Exception as e:
        # do nothing
        print("tables did not exist, ignoring", e)

    with conn.cursor() as c:
        c.execute(Path(request.fspath.dirname + "/db_seeds/create_test_changes.sql").read_text())
    conn.commit()

    env = os.environ
    env['RUST_LOG'] = 'debug'
    p = subprocess.Popen(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test-changes.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "1s",
        ],
        stdout=subprocess.PIPE,
        text=True,
        env=env,
    )

    # how many transactions
    t = 3
    # how many documents per transaction
    n = 10
    with conn.cursor() as c:
        for i in range(t):
            for j in range(i*n, (i+1)*n):
                c.execute(f"INSERT INTO test_changes(id, str) VALUES ({j}, 'record {j}')")
            conn.commit()

    lines = []
    for i in range(t*n):
        lines.append(json.loads(p.stdout.readline()))

    assert p.wait(timeout=30) == 0

    # clean up snapshot from non-deterministic values
    for _, doc in lines:
        source = doc['_meta']['source']
        if 'row_id' in source:
            source['row_id'] = '<row_id>'
        if 'scn' in source:
            source['scn'] = '<scn>'

    assert snapshot("stdout.json") == lines


def test_capture_empty(request, snapshot):
    conn = connect(request)

    # seed the test database first
    try:
        with conn.cursor() as c:
            c.execute("DROP TABLE test_empty")
    except Exception as e:
        # do nothing
        print("tables did not exist, ignoring", e)

    with conn.cursor() as c:
        c.execute("CREATE TABLE test_empty(id INTEGER)")
    conn.commit()

    env = os.environ
    env['RUST_LOG'] = 'debug'
    p = subprocess.Popen(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test-changes.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "1s",
        ],
        stdout=subprocess.PIPE,
        text=True,
        env=env,
    )

    assert p.wait(timeout=30) == 0

    assert snapshot("stdout.json") == []


def test_discover(request, snapshot):
    conn = connect(request)

    # seed the test database first
    for query in ["DROP TABLE test_changes", "DROP TABLE test_all_types", "DROP TABLE test_empty", "DROP TABLE flow_capture.test", "DROP USER flow_capture"]:
        try:
            with conn.cursor() as c:
                c.execute(query)
        except Exception as e:
            # do nothing
            print("tables did not exist, ignoring", e)

    with conn.cursor() as c:
        for f in ['create_test_all_types.sql', 'create_test_changes.sql']:
            q = Path(request.fspath.dirname + "/db_seeds/" + f).read_text()
            c.execute(q)
        qs = Path(request.fspath.dirname + "/db_seeds/create_flow_capture_user.sql").read_text().split('\n')
        for q in qs:
            c.execute(q)
        c.execute("CREATE TABLE test_empty(id INTEGER)")
    conn.commit()

    conn_flow_capture = connect(request, user='flow_capture', password='Secret1234ABCDEFG')
    with conn_flow_capture.cursor() as c:
        q = Path(request.fspath.dirname + "/db_seeds/create_flow_capture_table.sql").read_text()
        c.execute(q)
    conn_flow_capture.commit()

    env = os.environ
    env['RUST_LOG'] = 'debug'
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            request.fspath.dirname + "/../test-types.flow.yaml",
            "-o",
            "json",
            "--emit-raw",
        ],
        stdout=subprocess.PIPE,
        text=True,
        env=env,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    assert snapshot("stdout.json") == lines


def test_spec(request, snapshot):
    env = os.environ
    env['RUST_LOG'] = 'debug'
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "spec",
            "--source",
            request.fspath.dirname + "/../test-types.flow.yaml",
        ],
        stdout=subprocess.PIPE,
        text=True,
        env=env,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    assert snapshot("stdout.json") == lines
