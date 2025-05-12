

from datetime import datetime, timedelta
import estuary_cdk.emitted_changes_cache as cache


def test_emitted_changes_cache():
    base_dt = datetime.fromisoformat("2024-08-29T12:00:00.000Z")

    test_entries = [
        ("first", "id1", base_dt),
        ("second", "id1", base_dt),
        ("first", "id2", base_dt + timedelta(seconds=1)),
        ("first", "id3", base_dt + timedelta(seconds=2)),
        ("first", "id4", base_dt + timedelta(seconds=3)),
        ("first", "id5", base_dt + timedelta(seconds=4)),
    ]

    assert not cache.has_as_recent_as("first", "id1", base_dt)
    cache.add_recent("first", "id1", base_dt)
    assert not cache.has_as_recent_as("second", "id1", base_dt)

    assert cache.should_yield("third", "id1", base_dt)
    assert not cache.should_yield("third", "id1", base_dt)

    for object_name, id, dt in test_entries:
        cache.add_recent(object_name, id, dt)
        assert cache.has_as_recent_as(object_name, id, dt)
        assert cache.has_as_recent_as(object_name, id, dt - timedelta(milliseconds=1))
        assert not cache.has_as_recent_as(object_name, id, dt + timedelta(milliseconds=1))

    assert cache.count_for("first") == 5
    assert cache.count_for("second") == 1
    assert cache.cleanup("first", base_dt) == 1
    assert cache.cleanup("second", base_dt) == 1
    assert cache.count_for("first") == 4
    assert cache.count_for("second") == 0
    assert not cache.has_as_recent_as("first", "id1", base_dt)
    assert not cache.has_as_recent_as("second", "id1", base_dt)
    assert cache.cleanup("first", base_dt + timedelta(seconds=4)) == 4
    assert cache.count_for("first") == 0