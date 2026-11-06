from datetime import UTC, datetime
from typing import Any

import pytest

from source_okta.models import AfterToken


class TestAfterTokenOrdering:
    def test_earlier_ms_sorts_first_whatever_n(self):
        assert AfterToken(999, 5) < AfterToken(1000, 0)

    def test_n_compares_numerically(self):
        # As strings, "1000_10" < "1000_9".
        assert AfterToken(1000, 9) < AfterToken(1000, 10)
        assert AfterToken.from_str("1000_9") < AfterToken.from_str("1000_10")

    def test_max_prefers_checkpointed_token_at_same_ms(self):
        checkpointed = AfterToken(1000, 3)
        minted = AfterToken.from_timestamp(datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC))
        assert max(checkpointed, minted) == checkpointed


class TestAfterTokenParsing:
    def test_round_trips(self):
        token = AfterToken.from_str("1788464187011_3")
        assert token == AfterToken(1788464187011, 3)
        assert str(token) == "1788464187011_3"

    @pytest.mark.parametrize(
        "token",
        ["0170_0", "17_03", "+17_0", " 17_0", "17_0_1", "abc", "17", "17_"],
    )
    def test_rejects_non_canonical(self, token: str):
        with pytest.raises(ValueError):
            AfterToken.from_str(token)

    def test_from_url(self):
        url = "https://acme.okta.com/api/v1/logs?after=1788464187011_3&limit=1000"
        assert AfterToken.from_url(url) == AfterToken(1788464187011, 3)

    def test_from_url_without_after(self):
        with pytest.raises(RuntimeError):
            AfterToken.from_url("https://acme.okta.com/api/v1/logs?limit=1000")

    def test_cursor_round_trips(self):
        token = AfterToken(1788464187011, 3)
        assert token.to_cursor() == ("1788464187011_3",)
        assert AfterToken.from_cursor(token.to_cursor()) == token

    @pytest.mark.parametrize("cursor", [(), ("1_0", "2_0"), (1,)])
    def test_from_cursor_rejects_other_shapes(self, cursor: Any):
        with pytest.raises(RuntimeError):
            AfterToken.from_cursor(cursor)

    def test_from_timestamp_floors_to_ms(self):
        at = datetime(2026, 9, 3, 12, 0, 0, 123_999, tzinfo=UTC)
        token = AfterToken.from_timestamp(at)
        assert token.n == 0
        assert token.timestamp == at.replace(microsecond=123_000)
