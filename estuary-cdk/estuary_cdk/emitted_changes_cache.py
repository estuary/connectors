"""
This is a singleton module containing the cache of changes that have been
emitted for each object name. It used as a filter to reduce the amount of
duplicate data the connector may need to process.
"""

from datetime import datetime
from typing import Dict, Union

KeyType = Union[int, str]

emitted_cache: Dict[str, Dict[KeyType, datetime]] = {}


def add_recent(object_name: str, key: KeyType, dt: datetime) -> None:
    """
    Add or update a recent change in the emitted cache. If `dt` is more recent
    than any previous record for `id`, it will be updated in (or added to) the
    cache.
    """
    ids = emitted_cache.setdefault(object_name, {})
    if key not in ids or dt > ids[key]:
        ids[key] = dt


def has_as_recent_as(object_name: str, key: KeyType, dt: datetime) -> bool:
    """
    Check if there's a change that is at least as recent as `dt` for `id`.
    """
    ids = emitted_cache.get(object_name, {})
    return key in ids and ids[key] >= dt


def cleanup(object_name: str, cutoff: datetime) -> int:
    """
    Remove outdated entries from the cache for `object_name`, up to `cutoff`
    (exclusive).
    """
    ids = emitted_cache.get(object_name, {})
    initial_length = len(ids)

    emitted_cache[object_name] = {id: dt for id, dt in ids.items() if dt > cutoff}
    final_length = len(emitted_cache[object_name])

    return initial_length - final_length


def count_for(object_name: str) -> int:
    """
    Return the number of entries in the cache for `object_name`.
    """

    return len(emitted_cache.get(object_name, {}))


def should_yield(object_name: str, key: KeyType, dt: datetime) -> bool:
    """
    Check if the change for `object_name` and `key` should be yielded. This is
    true if the change is not in the cache or if it is more recent than the
    cached entry.

    If the change should be yielded, it is automatically added to the cache.
    Callers should immediately yield the document if this returns `True`.
    """

    if has_as_recent_as(object_name, key, dt):
        return False

    add_recent(object_name, key, dt)
    return True
