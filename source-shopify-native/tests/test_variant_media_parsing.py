"""Tests for `ProductVariants.process_result`, which reassembles the bulk JSONL result.

`product_variants` is the connector's only bulk stream nested two connections deep
(`products` -> `variants` -> `media`), so a variant's media arrives on its own line
carrying the variant's id in `__parentId`. Shopify guarantees only that a child appears
somewhere after its parent, so these tests pin the reassembly to `__parentId` lookup
rather than to line adjacency.
"""

import json
from contextlib import aclosing
from logging import Logger
from typing import Any, AsyncGenerator
from unittest.mock import MagicMock

import pytest

from source_shopify_native.graphql.products.variants import ProductVariants

PRODUCT_1 = "gid://shopify/Product/1"
PRODUCT_2 = "gid://shopify/Product/2"
VARIANT_1 = "gid://shopify/ProductVariant/11"
VARIANT_2 = "gid://shopify/ProductVariant/12"
VARIANT_3 = "gid://shopify/ProductVariant/21"
IMAGE_1 = "gid://shopify/MediaImage/101"
VIDEO_1 = "gid://shopify/Video/102"
IMAGE_2 = "gid://shopify/MediaImage/103"


async def _lines(records: list[dict[str, Any]]) -> AsyncGenerator[bytes, None]:
    for record in records:
        yield json.dumps(record).encode()


async def _process(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    log: Logger = MagicMock()
    # Closed explicitly so the tests that assert a RuntimeError don't leave the generators
    # suspended mid-iteration.
    async with aclosing(_lines(records)) as lines:
        async with aclosing(ProductVariants.process_result(log, lines)) as documents:
            return [document async for document in documents]


@pytest.mark.asyncio
async def test_media_attaches_to_its_variant_across_non_adjacent_lines():
    # VARIANT_1's media is emitted after VARIANT_2's line, which Shopify permits: children
    # are ordered after their parent, but not necessarily immediately after it.
    documents = await _process(
        [
            {"id": PRODUCT_1},
            {"id": VARIANT_1, "__parentId": PRODUCT_1},
            {"id": VARIANT_2, "__parentId": PRODUCT_1},
            {"id": IMAGE_1, "mediaContentType": "IMAGE", "__parentId": VARIANT_1},
            {"id": VIDEO_1, "mediaContentType": "VIDEO", "__parentId": VARIANT_1},
            {"id": IMAGE_2, "mediaContentType": "IMAGE", "__parentId": VARIANT_2},
        ]
    )

    assert len(documents) == 1
    variants = documents[0]["variants"]
    assert [variant["id"] for variant in variants] == [VARIANT_1, VARIANT_2]
    # Non-image media has no `image`, so it is identified by mediaContentType alone.
    assert [media["id"] for media in variants[0]["media"]] == [IMAGE_1, VIDEO_1]
    assert [media["id"] for media in variants[1]["media"]] == [IMAGE_2]


@pytest.mark.asyncio
async def test_variant_without_media_still_carries_an_empty_list():
    documents = await _process(
        [
            {"id": PRODUCT_1},
            {"id": VARIANT_1, "__parentId": PRODUCT_1},
        ]
    )

    assert documents[0]["variants"][0]["media"] == []


@pytest.mark.asyncio
async def test_media_is_scoped_to_the_product_that_owns_the_variant():
    documents = await _process(
        [
            {"id": PRODUCT_1},
            {"id": VARIANT_1, "__parentId": PRODUCT_1},
            {"id": IMAGE_1, "__parentId": VARIANT_1},
            {"id": PRODUCT_2},
            {"id": VARIANT_3, "__parentId": PRODUCT_2},
            {"id": IMAGE_2, "__parentId": VARIANT_3},
        ]
    )

    assert len(documents) == 2
    assert [media["id"] for media in documents[0]["variants"][0]["media"]] == [IMAGE_1]
    assert [media["id"] for media in documents[1]["variants"][0]["media"]] == [IMAGE_2]


@pytest.mark.asyncio
async def test_media_for_a_previous_product_variant_is_rejected():
    # Media parented to a variant of an already-yielded product means the JSONL arrived out of
    # order. Silently dropping it would lose media, so the stream fails loudly instead.
    with pytest.raises(RuntimeError):
        await _process(
            [
                {"id": PRODUCT_1},
                {"id": VARIANT_1, "__parentId": PRODUCT_1},
                {"id": PRODUCT_2},
                {"id": IMAGE_1, "__parentId": VARIANT_1},
            ]
        )


@pytest.mark.asyncio
async def test_unidentified_line_is_still_rejected():
    with pytest.raises(RuntimeError):
        await _process(
            [
                {"id": PRODUCT_1},
                {"id": "gid://shopify/Customer/9", "__parentId": "gid://shopify/Order/9"},
            ]
        )
