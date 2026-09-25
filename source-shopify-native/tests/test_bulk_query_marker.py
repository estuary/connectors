"""Checks against the live test store that Shopify keeps the marker comment BulkJobManager adds to
each bulk query, since that's how the connector recognises its own jobs.

The unit tests in test_bulk_job_manager.py fake Shopify, so they can't catch Shopify changing how
it stores submitted queries.
"""

import json
import subprocess
from logging import Logger
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from source_shopify_native import Connector
from source_shopify_native.graphql.bulk_job_manager import BulkJobManager
from source_shopify_native.graphql.client import ShopifyGraphQLClient
from source_shopify_native.models import BulkOperationStatuses, EndpointConfig
from source_shopify_native.resources import StoreHTTP, _build_token_source

# Pinned as a literal rather than imported, like in test_bulk_job_manager.py.
MARKER = "# Estuary Flow Managed Bulk Query"
CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"
# The test store has only a few locations, so this job finishes in seconds.
SMALL_QUERY = "{ locations { edges { node { id } } } }"
SOPS_SUFFIX = "_sops"


def _strip_sops_suffix(value: Any) -> Any:
    """Drop the `_sops` suffix from decrypted keys, as Flow does before handing config to a connector."""
    if isinstance(value, dict):
        return {k.removesuffix(SOPS_SUFFIX): _strip_sops_suffix(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_strip_sops_suffix(v) for v in value]
    return value


def _load_config() -> EndpointConfig:
    decrypted = subprocess.run(
        ["sops", "--decrypt", "--output-type", "json", str(CONFIG_PATH)],
        stdout=subprocess.PIPE,
        check=True,
        text=True,
    ).stdout
    return EndpointConfig.model_validate(_strip_sops_suffix(json.loads(decrypted)))


@pytest.mark.asyncio
async def test_shopify_keeps_bulk_query_marker():
    log = MagicMock(spec=Logger)
    store_config = _load_config().stores[0]

    http = Connector()
    for base in http.__class__.__bases__:
        if enter := getattr(base, "_mixin_enter", None):
            await enter(http, log)

    try:
        client = ShopifyGraphQLClient(StoreHTTP(http, _build_token_source(store_config)), store_config.store)
        manager = BulkJobManager(client, log)

        job_id = await manager._submit(SMALL_QUERY)
        try:
            details = await manager._get_job(job_id)
            assert MARKER in details.query, details.query
        finally:
            # Don't leave the job holding one of the store's bulk query slots during test_capture.
            status = (await manager._get_job(job_id)).status
            if status in (BulkOperationStatuses.CREATED, BulkOperationStatuses.RUNNING):
                await manager._cancel(job_id)
    finally:
        for base in http.__class__.__bases__:
            if exit := getattr(base, "_mixin_exit", None):
                await exit(http, log)
