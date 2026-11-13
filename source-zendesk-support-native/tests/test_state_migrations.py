import pytest

from source_zendesk_support_native.api import cursor_export_cursor_to_dt
from source_zendesk_support_native.api.shared import s_to_dt


def test_cursor_export_cursor_to_dt() -> None:
    # A decoded cursor from the Bruno "users cursor" example, and the plain epoch
    # string stored before the export's first page.
    assert cursor_export_cursor_to_dt("1791388325.0||54337475674900|") == s_to_dt(1791388325)
    assert cursor_export_cursor_to_dt("1791388325") == s_to_dt(1791388325)

    with pytest.raises(ValueError):
        cursor_export_cursor_to_dt("not a cursor")
