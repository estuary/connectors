from urllib.parse import quote

import pytest
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType


CATALOG_URI = "http://catalog.test"


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
