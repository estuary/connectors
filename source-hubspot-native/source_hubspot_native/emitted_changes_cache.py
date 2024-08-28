"""
This is a singleton module containing the cache of changes that have been
emitted for each object name. It used as a filter to reduce the amount of
duplicate data the connector may need to process.
"""

from datetime import datetime
from typing import Dict

emitted_cache: Dict[str, Dict[str, datetime]] = {}


def add_recent(object_name: str, id: str, dt: datetime) -> None:
    """
    Add or update a recent change in the emitted cache. If `dt` is more recent
    than any previous record for `id`, it will be updated in (or added to) the
    cache.
    """
    ids = emitted_cache.setdefault(object_name, {})
    if id not in ids or dt > ids[id]:
        ids[id] = dt


def has_as_recent_as(object_name: str, id: str, dt: datetime) -> bool:
    """
    Check if there's a change that is at least as recent as `dt` for `id`.
    """
    ids = emitted_cache.get(object_name, {})
    return id in ids and ids[id] >= dt


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
