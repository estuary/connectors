import pytest

from source_gong.models import (
    Call,
    CallTranscript,
    ExtensiveCall,
    FilteredGongResource,
    HttpMethod,
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


class TestExtensiveCall:
    # Shape taken from bruno/Calls/extensive.yml against the sandbox.
    META = {
        "id": "7022652889306584427",
        "started": "2026-08-28T08:00:00-07:00",
        "title": "Estuary seed call A",
    }

    def test_carries_no_document_cursor(self):
        call = ExtensiveCall.model_validate({"metaData": self.META})
        assert not hasattr(call, "cursor_value")

    def test_id_coerced_to_int(self):
        call = ExtensiveCall.model_validate({"metaData": self.META})
        assert call.metaData.id == 7022652889306584427

    def test_id_field_mirrors_nested_key(self):
        assert ExtensiveCall.KEY == ["/metaData/id"]
        assert ExtensiveCall.get_key_json_path() == ExtensiveCall.KEY[0]

    def test_content_blocks_ride_on_extra(self):
        call = ExtensiveCall.model_validate(
            {"metaData": self.META, "parties": [{"id": "1"}], "content": {"brief": "b"}}
        )
        dumped = call.model_dump()
        assert dumped["parties"] == [{"id": "1"}]
        assert dumped["content"] == {"brief": "b"}

    def test_media_not_requested(self):
        exposed = ExtensiveCall.BODY_EXTRA["contentSelector"]["exposedFields"]
        assert exposed["media"] is False


class TestCallTranscript:
    # Shape taken from bruno/Calls/transcript.yml against the sandbox.
    ITEM = {
        "callId": "7022652889306584427",
        "transcript": [
            {
                "speakerId": "656861958384341821",
                "topic": None,
                "sentences": [{"start": 160, "end": 740, "text": "chapter I."}],
            }
        ],
    }

    def test_call_id_coerced_to_int_so_it_joins_calls(self):
        transcript = CallTranscript.model_validate(self.ITEM)
        assert (
            transcript.callId
            == Call.model_validate(
                {"id": "7022652889306584427", "started": "2026-08-28T08:00:00Z"}
            ).id
        )

    def test_monologues_ride_on_extra(self):
        transcript = CallTranscript.model_validate(self.ITEM)
        assert transcript.model_dump()["transcript"] == self.ITEM["transcript"]

    def test_carries_no_document_cursor(self):
        assert not hasattr(CallTranscript.model_validate(self.ITEM), "cursor_value")

    def test_posts_a_filter_wrapped_body_without_extras(self):
        assert CallTranscript.METHOD is HttpMethod.POST
        assert CallTranscript.FILTER_WRAPPER is True
        assert CallTranscript.BODY_EXTRA == {}


class TestRequiredClassVars:
    def test_incomplete_subclass_is_rejected(self):
        with pytest.raises(TypeError, match="URL_PATH"):

            class Incomplete(FilteredGongResource):
                NAME = "incomplete"
                KEY = ["/id"]
                ITEMS_KEY = "things"
                FROM_PARAM = "from"
                TO_PARAM = "to"

    def test_abstract_base_is_exempt(self):
        class Abstract(FilteredGongResource):
            ABSTRACT = True

        assert Abstract.ABSTRACT is True


class TestParseNewStreams:
    def test_parse_extensive_calls(self):
        raw = b'{"calls":[{"metaData":{"id":"1","started":"2026-08-28T08:00:00Z"}}],"records":{"cursor":"c1"}}'
        items, cursor = _parse_response(ExtensiveCall, "calls", raw)
        assert len(items) == 1
        assert items[0].metaData.id == 1
        assert cursor == "c1"

    def test_parse_call_transcripts(self):
        raw = b'{"callTranscripts":[{"callId":"1","transcript":[]}],"records":{"totalRecords":1}}'
        items, cursor = _parse_response(CallTranscript, "callTranscripts", raw)
        assert len(items) == 1
        assert items[0].callId == 1
        assert cursor is None
