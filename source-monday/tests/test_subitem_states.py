import logging
import re
from typing import Any, AsyncGenerator
from unittest.mock import MagicMock

import pytest

from source_monday.graphql.items import (
    SUBITEM_STATE_BATCH_SIZE,
    SubitemState,
    _ITEM_FIELDS,
    _enrich_subitem_states,
    _with_subitem_states,
)
from source_monday.models import Item

LOG = logging.getLogger("test-subitem-states")


def make_item(item_id: str, subitem_ids: list[str] | None = None, **extra: Any) -> Item:
    """Build an Item as `execute_query` would: subitems are undeclared extra fields
    (list of dicts) that carry no `state` (it's excluded from the subitem fragment)."""
    data: dict[str, Any] = {
        "id": item_id,
        "updated_at": "2024-01-01T00:00:00Z",
        "state": "active",
    }
    if subitem_ids is not None:
        data["subitems"] = [{"id": sid, "name": f"sub-{sid}"} for sid in subitem_ids]
    data.update(extra)
    return Item.model_validate(data)


def subitem_states(item: Item) -> dict[str, Any]:
    """Map of subitem id -> injected state (missing key => not set)."""
    return {
        s["id"]: s.get("state")
        for s in (getattr(item, "subitems", None) or [])
        if isinstance(s, dict)
    }


async def as_async_gen(items: list[Item]) -> AsyncGenerator[Item, None]:
    for item in items:
        yield item


async def collect(gen: AsyncGenerator[Item, None]) -> list[Item]:
    return [item async for item in gen]


def patch_execute_query(
    monkeypatch: pytest.MonkeyPatch,
    state_by_id: dict[str, str],
    calls: list[list[str]],
) -> None:
    """Replace the real `execute_query` with a fake that records the ids it was
    asked for and yields `SubitemState`s only for ids present in `state_by_id`
    (mirroring Monday returning nothing for unknown/inaccessible ids)."""

    async def fake_execute_query(
        cls, http, log, json_path, query, variables=None, **kwargs
    ):
        ids = list(variables["ids"])
        calls.append(ids)
        for sid in ids:
            if sid in state_by_id:
                yield cls(id=sid, state=state_by_id[sid])

    monkeypatch.setattr(
        "source_monday.graphql.items.execute_query", fake_execute_query
    )


# --- _enrich_subitem_states -------------------------------------------------


@pytest.mark.asyncio
async def test_enrich_injects_state(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []
    patch_execute_query(monkeypatch, {"s1": "active", "s2": "deleted"}, calls)

    items = [make_item("i1", ["s1", "s2"])]
    await _enrich_subitem_states(MagicMock(), LOG, items)

    assert subitem_states(items[0]) == {"s1": "active", "s2": "deleted"}
    assert calls == [["s1", "s2"]]


@pytest.mark.asyncio
async def test_enrich_no_subitems_makes_no_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    patch_execute_query(monkeypatch, {}, calls)

    # One item with no subitems key, one with an empty subitems list.
    items = [make_item("i1"), make_item("i2", [])]
    await _enrich_subitem_states(MagicMock(), LOG, items)

    assert calls == []


@pytest.mark.asyncio
async def test_enrich_dedupes_ids_across_items(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    patch_execute_query(monkeypatch, {"s1": "active", "s2": "archived"}, calls)

    items = [make_item("i1", ["s1"]), make_item("i2", ["s1", "s2"])]
    await _enrich_subitem_states(MagicMock(), LOG, items)

    # s1 is shared: queried once, order preserved.
    assert calls == [["s1", "s2"]]
    assert subitem_states(items[0]) == {"s1": "active"}
    assert subitem_states(items[1]) == {"s1": "active", "s2": "archived"}


@pytest.mark.asyncio
async def test_enrich_batches_at_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []
    total = SUBITEM_STATE_BATCH_SIZE * 2 + 25
    ids = [f"s{i}" for i in range(total)]
    patch_execute_query(monkeypatch, {sid: "active" for sid in ids}, calls)

    items = [make_item("i1", ids)]
    await _enrich_subitem_states(MagicMock(), LOG, items)

    # Monday caps items(ids:) at 100 ids/request, so ids are chunked accordingly.
    assert [len(c) for c in calls] == [
        SUBITEM_STATE_BATCH_SIZE,
        SUBITEM_STATE_BATCH_SIZE,
        25,
    ]
    states = subitem_states(items[0])
    assert len(states) == total
    assert all(v == "active" for v in states.values())


# --- _with_subitem_states ---------------------------------------------------


@pytest.mark.asyncio
async def test_with_subitem_states_yields_all_enriched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    patch_execute_query(monkeypatch, {"s1": "active", "s2": "deleted"}, calls)

    items = [make_item("i1", ["s1"]), make_item("i2", ["s2"]), make_item("i3")]
    out = await collect(_with_subitem_states(MagicMock(), LOG, as_async_gen(items)))

    assert [i.id for i in out] == ["i1", "i2", "i3"]  # order preserved
    assert subitem_states(out[0]) == {"s1": "active"}
    assert subitem_states(out[1]) == {"s2": "deleted"}
    assert subitem_states(out[2]) == {}


@pytest.mark.asyncio
async def test_with_subitem_states_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []
    patch_execute_query(monkeypatch, {}, calls)

    out = await collect(_with_subitem_states(MagicMock(), LOG, as_async_gen([])))

    assert out == []
    assert calls == []


@pytest.mark.asyncio
async def test_with_subitem_states_flushes_at_batch_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    flush_sizes: list[int] = []

    async def spy(http, log, items: list[Item]) -> None:
        flush_sizes.append(len(items))

    monkeypatch.setattr("source_monday.graphql.items._enrich_subitem_states", spy)

    items = [make_item(f"i{n}", ["s"]) for n in range(SUBITEM_STATE_BATCH_SIZE + 50)]
    out = await collect(_with_subitem_states(MagicMock(), LOG, as_async_gen(items)))

    assert len(out) == SUBITEM_STATE_BATCH_SIZE + 50
    # Buffer flushes once it reaches the batch size, then the remainder flushes.
    assert flush_sizes == [SUBITEM_STATE_BATCH_SIZE, 50]


# --- fragment regression guard ----------------------------------------------


def test_fragment_omits_state_from_subitems() -> None:
    """`state` must stay out of the shared `_ItemFields` (reused by subitems) and
    remain only on the top-level `ItemFields`. Re-adding it to `_ItemFields`
    reintroduces the subitem INTERNAL_SERVER_ERROR."""
    shared, sep, parent = _ITEM_FIELDS.partition("fragment ItemFields on Item")
    assert sep, "ItemFields fragment not found"
    assert not re.search(r"\bstate\b", shared), (
        "subitems reuse _ItemFields, which must not request `state`"
    )
    assert re.search(r"\bstate\b", parent), "top-level items must still request `state`"
