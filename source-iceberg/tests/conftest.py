import os
import subprocess
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import pytest
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType

from .seed import main as seed_catalog


CATALOG_URI = "http://catalog.test"


@pytest.fixture(scope="session")
def local_catalog(tmp_path_factory):
    connector_dir = Path(__file__).parents[1]
    compose = [
        "docker", "compose", "--project-name", f"source-iceberg-pytest-{uuid4().hex}",
        "-f", str(connector_dir / "docker-compose.yaml"),
    ]
    # Docker assigns free host ports so other fixtures can stay running.
    env = {**os.environ, "ICEBERG_CATALOG_PORT": "0", "ICEBERG_STORAGE_PORT": "0"}

    def endpoint(service: str, port: int) -> str:
        result = subprocess.run(
            [*compose, "port", service, str(port)],
            env=env, check=True, capture_output=True, text=True,
        )
        host_port = result.stdout.splitlines()[0].rsplit(":", 1)[1]
        return f"http://localhost:{host_port}"

    try:
        subprocess.run([*compose, "up", "--wait", "--wait-timeout", "180"], env=env, check=True)
        catalog_uri = endpoint("db", 8181)
        storage_endpoint = endpoint("storage", 9000)
        seed_catalog(catalog_uri=catalog_uri, storage_endpoint=storage_endpoint)
        source = tmp_path_factory.mktemp("source-iceberg") / "test.flow.yaml"
        source.write_text(
            (connector_dir / "test.flow.yaml").read_text()
            .replace("http://localhost:8181", catalog_uri)
            .replace("http://localhost:9010", storage_endpoint)
        )
        yield source
    finally:
        subprocess.run([*compose, "down", "-v"], env=env, check=True)


@pytest.fixture
def catalog_api(requests_mock):
    requests_mock.get(f"{CATALOG_URI}/v1/config", json={"defaults": {}, "overrides": {}})
    return requests_mock


@pytest.fixture
def register_table(catalog_api):
    def register(namespace: tuple[str, ...], name: str, schema: Schema, config: dict | None = None):
        location = f"s3://warehouse/{'/'.join(namespace)}/{name}"
        metadata = new_table_metadata(
            schema,
            UNPARTITIONED_PARTITION_SPEC,
            UNSORTED_SORT_ORDER,
            location,
        )
        namespace_path = quote("\x1f".join(namespace), safe="")
        catalog_api.get(
            f"{CATALOG_URI}/v1/namespaces/{namespace_path}/tables/{quote(name, safe='')}",
            json={
                "metadata-location": f"{location}/metadata/00000.metadata.json",
                "metadata": metadata.model_dump(mode="json", by_alias=True),
                "config": config or {},
            },
        )

    return register


@pytest.fixture
def shop_catalog(catalog_api, register_table):
    catalog_api.get(
        f"{CATALOG_URI}/v1/namespaces/shop/tables",
        json={"identifiers": [
            {"namespace": ["shop"], "name": "orders"},
            {"namespace": ["shop"], "name": "customers"},
        ]},
    )
    register_table(
        ("shop",),
        "orders",
        Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "canary", StringType()),
            NestedField(3, "at", TimestamptzType()),
            identifier_field_ids=[1],
        ),
    )
    register_table(
        ("shop",),
        "customers",
        Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "canary", StringType()),
        ),
    )
    return catalog_api
