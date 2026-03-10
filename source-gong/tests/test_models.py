from source_gong.models import (
    Call,
    User,
    Scorecard,
    ScorecardDefinition,
    GongResponseEnvelope,
    ResponseContext,
)
from source_gong.api import _parse_response


class TestExtractCursor:
    def test_call_iso_datetime(self):
        data = {"id": 123, "started": "2024-01-15T10:30:00Z", "title": "Demo Call"}
        call = Call.model_validate(data)
        assert call.cursor_value == 1705314600

    def test_call_iso_datetime_with_offset(self):
        data = {"id": 123, "started": "2024-01-15T10:30:00+00:00"}
        call = Call.model_validate(data)
        assert call.cursor_value == 1705314600

    def test_call_unix_timestamp(self):
        data = {"id": 123, "started": 1705312200}
        call = Call.model_validate(data)
        assert call.cursor_value == 1705312200

    def test_call_missing_cursor_field(self):
        data = {"id": 123}
        call = Call.model_validate(data)
        assert call.cursor_value == 0

    def test_call_none_cursor_field(self):
        data = {"id": 123, "started": None}
        call = Call.model_validate(data)
        assert call.cursor_value == 0

    def test_user_created(self):
        data = {"id": 456, "created": "2024-06-01T00:00:00Z"}
        user = User.model_validate(data)
        assert user.cursor_value == 1717200000

    def test_scorecard_custom_id_field(self):
        data = {"scorecardId": 42, "reviewTime": "2024-03-20T15:00:00Z"}
        sc = Scorecard.model_validate(data)
        assert sc.cursor_value > 0

    def test_extra_fields_preserved(self):
        data = {"id": 789, "started": "2024-01-15T10:30:00Z", "duration": 300, "language": "en"}
        call = Call.model_validate(data)
        assert call.model_extra is not None


class TestGongResponseEnvelope:
    def test_parses_cursor(self):
        envelope = GongResponseEnvelope.model_validate({"records": {"cursor": "abc123"}, "calls": []})
        assert envelope.next_cursor == "abc123"

    def test_null_cursor(self):
        envelope = GongResponseEnvelope.model_validate({"records": {"cursor": None}})
        assert envelope.next_cursor is None

    def test_missing_records(self):
        envelope = GongResponseEnvelope.model_validate({"users": [{"id": "u1"}]})
        assert envelope.next_cursor is None

    def test_records_metadata(self):
        envelope = GongResponseEnvelope.model_validate(
            {"records": {"cursor": "x", "totalRecords": 42, "currentPageSize": 10}}
        )
        assert envelope.records is not None
        assert envelope.records.totalRecords == 42
        assert envelope.records.currentPageSize == 10

    def test_extracts_items_with_response_context(self):
        data = {"records": {"cursor": "c1"}, "calls": [{"id": 1, "started": "2024-01-15T10:30:00Z"}]}
        ctx = ResponseContext(item_cls=Call, items_key="calls")
        envelope = GongResponseEnvelope.model_validate(data, context=ctx)
        assert len(envelope.items) == 1
        assert isinstance(envelope.items[0], Call)
        assert envelope.next_cursor == "c1"

    def test_items_empty_without_context(self):
        data = {"records": {"cursor": "c1"}, "calls": [{"id": 1}]}
        envelope = GongResponseEnvelope.model_validate(data)
        assert envelope.items == []

    def test_response_context_is_frozen(self):
        import pytest

        ctx = ResponseContext(item_cls=Call, items_key="calls")
        with pytest.raises(AttributeError):
            ctx.items_key = "users"  # type: ignore[misc]


class TestParseResponse:
    def test_parse_calls(self):
        raw = b'{"records": {"cursor": "next123"}, "calls": [{"id": 1, "started": "2024-01-15T10:30:00Z"}]}'
        items, cursor = _parse_response(Call, "calls", raw)
        assert cursor == "next123"
        assert len(items) == 1
        assert isinstance(items[0], Call)
        assert items[0].cursor_value > 0

    def test_parse_users(self):
        raw = b'{"records": {"cursor": null}, "users": [{"id": 1, "created": "2024-01-01T00:00:00Z"}, {"id": 2, "created": "2024-01-02T00:00:00Z"}]}'
        items, cursor = _parse_response(User, "users", raw)
        assert cursor is None
        assert len(items) == 2
        assert isinstance(items[0], User)

    def test_parse_scorecards(self):
        raw = b'{"records": {"cursor": null}, "scorecards": [{"scorecardId": 1, "reviewTime": "2024-03-20T15:00:00Z"}]}'
        items, cursor = _parse_response(Scorecard, "scorecards", raw)
        assert len(items) == 1
        assert isinstance(items[0], Scorecard)

    def test_parse_scorecard_definitions(self):
        raw = b'{"records": {"cursor": "abc"}, "scorecards": [{"name": "Quality"}]}'
        items, cursor = _parse_response(ScorecardDefinition, "scorecards", raw)
        assert len(items) == 1
        assert isinstance(items[0], ScorecardDefinition)
        assert cursor == "abc"

    def test_empty_items_when_key_absent(self):
        raw = b'{"records": {"cursor": null}}'
        items, cursor = _parse_response(Call, "calls", raw)
        assert items == []

    def test_no_records(self):
        raw = b'{"users": [{"id": 1, "created": "2024-01-01T00:00:00Z"}]}'
        items, cursor = _parse_response(User, "users", raw)
        assert cursor is None
        assert len(items) == 1
