#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import calendar
import copy
import re
from datetime import datetime
from unittest.mock import patch
from urllib.parse import parse_qsl, urlparse

import pendulum
import pytest
import pytz
import requests
from airbyte_cdk import AirbyteLogger
from source_zendesk_support.source import BasicApiTokenAuthenticator, SourceZendeskSupport
from source_zendesk_support.streams import (
    DATETIME_FORMAT,
    END_OF_STREAM_KEY,
    LAST_END_TIME_KEY,
    AuditLogs,
    BaseSourceZendeskSupportStream,
    Brands,
    CustomRoles,
    GroupMemberships,
    Groups,
    Macros,
    Organizations,
    OrganizationMemberships,
    SatisfactionRatings,
    Schedules,
    SlaPolicies,
    SourceZendeskIncrementalExportStream,
    Tags,
    TicketAudits,
    TicketComments,
    TicketFields,
    TicketForms,
    TicketMetricEvents,
    TicketMetrics,
    Tickets,
    Users,
    UserSettingsStream,
)
from tests.data import TICKET_EVENTS_STREAM_RESPONSE
from tests.utils import read_full_refresh

# prepared config
STREAM_ARGS = {
    "subdomain": "sandbox",
    "start_date": "2021-06-01T00:00:00Z",
    "authenticator": BasicApiTokenAuthenticator("test@airbyte.io", "api_token"),
}

# raw config
TEST_CONFIG = {
    "subdomain": "sandbox",
    "start_date": "2021-06-01T00:00:00Z",
    "credentials": {"credentials": "api_token", "email": "integration-test@airbyte.io", "api_token": "api_token"},
}

# raw config oauth
TEST_CONFIG_OAUTH = {
    "subdomain": "sandbox",
    "start_date": "2021-06-01T00:00:00Z",
    "credentials": {"credentials": "oauth2.0", "access_token": "test_access_token"},
}

DATETIME_STR = "2021-07-22T06:55:55Z"
DATETIME_FROM_STR = datetime.strptime(DATETIME_STR, DATETIME_FORMAT)
STREAM_URL = "https://subdomain.zendesk.com/api/v2/stream.json?&start_time=1647532987&page=1"
URL_BASE = "https://sandbox.zendesk.com/api/v2/"


def snake_case(name):
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def test_date_time_format():
    assert DATETIME_FORMAT == "%Y-%m-%dT%H:%M:%SZ"


def test_last_end_time_key():
    assert LAST_END_TIME_KEY == "_last_end_time"


def test_end_of_stream_key():
    assert END_OF_STREAM_KEY == "end_of_stream"


def test_token_authenticator():
    # we expect base64 from creds input
    expected = "dGVzdEBhaXJieXRlLmlvL3Rva2VuOmFwaV90b2tlbg=="
    result = BasicApiTokenAuthenticator("test@airbyte.io", "api_token")
    assert result._token == expected


@pytest.mark.parametrize(
    "config",
    [(TEST_CONFIG), (TEST_CONFIG_OAUTH)],
    ids=["api_token", "oauth"],
)
def test_convert_config2stream_args(config):
    result = SourceZendeskSupport().convert_config2stream_args(config)
    assert "authenticator" in result


@pytest.mark.parametrize(
    "config, expected",
    [(TEST_CONFIG, "aW50ZWdyYXRpb24tdGVzdEBhaXJieXRlLmlvL3Rva2VuOmFwaV90b2tlbg=="), (TEST_CONFIG_OAUTH, "test_access_token")],
    ids=["api_token", "oauth"],
)
def test_get_authenticator(config, expected):
    # we expect base64 from creds input
    result = SourceZendeskSupport().get_authenticator(config=config)
    assert result._token == expected


@pytest.mark.parametrize(
    "response, start_date, check_passed",
    [({"active_features": {"organization_access_enabled": True}}, "2020-01-01T00:00:00Z", True), ({}, "2020-01-00T00:00:00Z", False)],
    ids=["check_successful", "invalid_start_date"],
)
def test_check(response, start_date, check_passed):
    config = copy.deepcopy(TEST_CONFIG)
    config["start_date"] = start_date
    with patch.object(UserSettingsStream, "get_settings", return_value=response) as mock_method:
        ok, _ = SourceZendeskSupport().check_connection(logger=AirbyteLogger, config=config)
        assert check_passed == ok
        if ok:
            mock_method.assert_called()


@pytest.mark.parametrize(
    "ticket_forms_response, status_code, expected_n_streams, expected_warnings",
    [
        ({"ticket_forms": [{"id": 1, "updated_at": "2021-07-08T00:05:45Z"}]}, 200, 27, []),
        (
            {"error": "Not sufficient permissions"},
            403,
            24,
            ["Skipping stream ticket_forms: Check permissions, error message: Not sufficient permissions."],
        ),
    ],
    ids=["forms_accessible", "forms_inaccessible"],
)
def test_full_access_streams(caplog, requests_mock, ticket_forms_response, status_code, expected_n_streams, expected_warnings):
    requests_mock.get("/api/v2/ticket_forms", status_code=status_code, json=ticket_forms_response)
    result = SourceZendeskSupport().streams(config=TEST_CONFIG)
    assert len(result) == expected_n_streams
    logged_warnings = iter([record for record in caplog.records if record.levelname == "ERROR"])
    for msg in expected_warnings:
        assert msg in next(logged_warnings).message


@pytest.fixture(autouse=True)
def time_sleep_mock(mocker):
    time_mock = mocker.patch("time.sleep", lambda x: None)
    yield time_mock


def test_str2datetime():
    expected = datetime.strptime(DATETIME_STR, DATETIME_FORMAT)
    output = BaseSourceZendeskSupportStream.str2datetime(DATETIME_STR)
    assert output == expected


def test_datetime2str():
    expected = datetime.strftime(DATETIME_FROM_STR.replace(tzinfo=pytz.UTC), DATETIME_FORMAT)
    output = BaseSourceZendeskSupportStream.datetime2str(DATETIME_FROM_STR)
    assert output == expected


def test_str2unixtime():
    expected = calendar.timegm(DATETIME_FROM_STR.utctimetuple())
    output = BaseSourceZendeskSupportStream.str2unixtime(DATETIME_STR)
    assert output == expected


def test_check_start_time_param():
    expected = 1626936955
    start_time = calendar.timegm(pendulum.parse(DATETIME_STR).utctimetuple())
    output = SourceZendeskIncrementalExportStream.check_start_time_param(start_time)
    assert output == expected


def test_next_page_token(requests_mock):
    # mocking the logic of next_page_token
    if TICKET_EVENTS_STREAM_RESPONSE.get(END_OF_STREAM_KEY) is False:
        expected = {"page": "2", "start_time": "1122334455"}
    else:
        expected = None
    requests_mock.get(STREAM_URL, json=TICKET_EVENTS_STREAM_RESPONSE)
    test_response = requests.get(STREAM_URL)
    output = TicketComments(**STREAM_ARGS).next_page_token(test_response)
    assert expected == output


def test_request_params(requests_mock):
    expected = {"start_time": calendar.timegm(pendulum.parse(STREAM_ARGS.get("start_date")).utctimetuple()), "include": "comment_events"}
    stream_state = None
    requests_mock.get(STREAM_URL, json=TICKET_EVENTS_STREAM_RESPONSE)
    test_response = requests.get(STREAM_URL)
    next_page_token = TicketComments(**STREAM_ARGS).next_page_token(test_response)
    output = TicketComments(**STREAM_ARGS).request_params(stream_state, None)
    assert next_page_token is not None
    assert expected == output


def test_parse_response_from_empty_json(requests_mock):
    requests_mock.get(STREAM_URL, text="", status_code=403)
    test_response = requests.get(STREAM_URL)
    output = Schedules(**STREAM_ARGS).parse_response(test_response, {})
    assert list(output) == []


def test_parse_response(requests_mock):
    requests_mock.get(STREAM_URL, json=TICKET_EVENTS_STREAM_RESPONSE)
    test_response = requests.get(STREAM_URL)
    output = TicketComments(**STREAM_ARGS).parse_response(test_response)
    # get the first parsed element from generator
    parsed_output = list(output)[0]
    # check, if we have all transformations correctly
    for entity in TicketComments.list_entities_from_event:
        assert True if entity in parsed_output else False


class TestAllStreams:
    @pytest.mark.parametrize(
        "expected_stream_cls",
        [
            (AuditLogs),
            (GroupMemberships),
            (Groups),
            (Macros),
            (Organizations),
            (OrganizationMemberships),
            (SatisfactionRatings),
            (SlaPolicies),
            (Tags),
            (TicketAudits),
            (TicketComments),
            (TicketFields),
            (TicketForms),
            (TicketMetrics),
            (TicketMetricEvents),
            (Tickets),
            (Users),
            (Brands),
            (CustomRoles),
            (Schedules),
        ],
        ids=[
            "AuditLogs",
            "GroupMemberships",
            "Groups",
            "Macros",
            "Organizations",
            "OrganizationMemberships",
            "SatisfactionRatings",
            "SlaPolicies",
            "Tags",
            "TicketAudits",
            "TicketComments",
            "TicketFields",
            "TicketForms",
            "TicketMetrics",
            "TicketMetricEvents",
            "Tickets",
            "Users",
            "Brands",
            "CustomRoles",
            "Schedules",
        ],
    )
    def test_streams(self, expected_stream_cls):
        with patch.object(TicketForms, "read_records", return_value=[{}]) as mocked_records:
            streams = SourceZendeskSupport().streams(TEST_CONFIG)
            mocked_records.assert_called()
            for stream in streams:
                if expected_stream_cls in streams:
                    assert isinstance(stream, expected_stream_cls)

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (AuditLogs, "audit_logs"),
            (GroupMemberships, "group_memberships"),
            (Groups, "groups"),
            (Macros, "macros"),
            (Organizations, "incremental/organizations"),
            (OrganizationMemberships, "organization_memberships"),
            (SatisfactionRatings, "satisfaction_ratings"),
            (SlaPolicies, "slas/policies.json"),
            (Tags, "tags"),
            (TicketComments, "incremental/ticket_events.json"),
            (TicketFields, "ticket_fields"),
            (TicketForms, "ticket_forms"),
            (TicketMetricEvents, "incremental/ticket_metric_events"),
            (Tickets, "incremental/tickets/cursor"),
            (Users, "incremental/users/cursor"),
            (Brands, "brands"),
            (CustomRoles, "custom_roles"),
            (Schedules, "business_hours/schedules.json"),
        ],
        ids=[
            "AuditLogs",
            "GroupMemberships",
            "Groups",
            "Macros",
            "Organizations",
            "OrganizationMemberships",
            "SatisfactionRatings",
            "SlaPolicies",
            "Tags",
            "TicketComments",
            "TicketFields",
            "TicketForms",
            "TicketMetricEvents",
            "Tickets",
            "Users",
            "Brands",
            "CustomRoles",
            "Schedules",
        ],
    )
    def test_path(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.path()
        assert result == expected


class TestSourceZendeskSupportStream:
    @pytest.mark.parametrize(
        "stream_cls",
        [
            (Macros),
            (SatisfactionRatings),
            (TicketFields),
        ],
        ids=[
            "Macros",
            "SatisfactionRatings",
            "TicketFields",
        ],
    )
    def test_parse_response(self, requests_mock, stream_cls):
        stream = stream_cls(**STREAM_ARGS)
        stream_name = snake_case(stream.__class__.__name__)
        expected = [{"updated_at": "2022-03-17T16:03:07Z"}]
        requests_mock.get(STREAM_URL, json={stream_name: expected})
        test_response = requests.get(STREAM_URL)
        output = list(stream.parse_response(test_response, None))
        assert expected == output

    @pytest.mark.parametrize(
        "stream_cls",
        [
            (Macros),
            (Organizations),
            (SatisfactionRatings),
            (TicketFields),
            (TicketMetrics),
        ],
        ids=[
            "Macros",
            "Organizations",
            "SatisfactionRatings",
            "TicketFields",
            "TicketMetrics",
        ],
    )
    def test_url_base(self, stream_cls):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.url_base
        assert result == URL_BASE

    @pytest.mark.parametrize(
        "stream_cls, current_state, last_record, expected",
        [
            (Macros, {}, {"updated_at": "2022-03-17T16:03:07Z"}, {"updated_at": "2022-03-17T16:03:07Z"}),
            (Groups, {}, {"updated_at": "2022-03-17T16:03:07Z"}, {"updated_at": "2022-03-17T16:03:07Z"}),
            (SatisfactionRatings, {}, {"updated_at": "2022-03-17T16:03:07Z"}, {"updated_at": "2022-03-17T16:03:07Z"}),
            (TicketFields, {}, {"updated_at": "2022-03-17T16:03:07Z"}, {"updated_at": "2022-03-17T16:03:07Z"}),
        ],
        ids=[
            "Macros",
            "Groups",
            "SatisfactionRatings",
            "TicketFields",
        ],
    )
    def test_get_updated_state(self, stream_cls, current_state, last_record, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.get_updated_state(current_state, last_record)
        assert expected == result

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (Macros, None),
            (TicketFields, None),
        ],
        ids=[
            "Macros",
            "TicketFields",
        ],
    )
    def test_next_page_token(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.next_page_token()
        assert expected == result

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (Macros, {"start_time": 1622505600}),
            (TicketFields, {"start_time": 1622505600}),
        ],
        ids=[
            "Macros",
            "TicketFields",
        ],
    )
    def test_request_params(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.request_params()
        assert expected == result


class TestSourceZendeskSupportFullRefreshStream:
    @pytest.mark.parametrize(
        "stream_cls",
        [
            (Tags),
            (SlaPolicies),
            (Brands),
            (CustomRoles),
            (Schedules),
            (UserSettingsStream),
        ],
        ids=[
            "Tags",
            "SlaPolicies",
            "Brands",
            "CustomRoles",
            "Schedules",
            "UserSettingsStream",
        ],
    )
    def test_url_base(self, stream_cls):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.url_base
        assert result == URL_BASE

    @pytest.mark.parametrize(
        "stream_cls",
        [
            (Tags),
            (SlaPolicies),
            (Brands),
            (CustomRoles),
            (Schedules),
            (UserSettingsStream),
        ],
        ids=[
            "Tags",
            "SlaPolicies",
            "Brands",
            "CustomRoles",
            "Schedules",
            "UserSettingsStream",
        ],
    )
    def test_next_page_token(self, requests_mock, stream_cls):
        stream = stream_cls(**STREAM_ARGS)
        stream_name = snake_case(stream.__class__.__name__)
        requests_mock.get(STREAM_URL, json={stream_name: {}})
        test_response = requests.get(STREAM_URL)
        output = stream.next_page_token(test_response)
        assert output is None

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (Tags, {"page[size]": 100}),
            (SlaPolicies, {}),
            (Brands, {"page[size]": 100}),
            (CustomRoles, {}),
            (Schedules, {}),
            (UserSettingsStream, {}),
        ],
        ids=[
            "Tags",
            "SlaPolicies",
            "Brands",
            "CustomRoles",
            "Schedules",
            "UserSettingsStream",
        ],
    )
    def test_request_params(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.request_params(next_page_token=None, stream_state=None)
        assert expected == result


class TestSourceZendeskSupportCursorPaginationStream:
    @pytest.mark.parametrize(
        "stream_cls, current_state, last_record, expected",
        [
            (Groups, {}, {"updated_at": "2022-03-17T16:03:07Z"}, {"updated_at": "2022-03-17T16:03:07Z"}),
            (GroupMemberships, {}, {"updated_at": "2022-03-17T16:03:07Z"}, {"updated_at": "2022-03-17T16:03:07Z"}),
            (TicketForms, {}, {"updated_at": "2023-03-17T16:03:07Z"}, {"updated_at": "2023-03-17T16:03:07Z"}),
            (TicketMetricEvents, {}, {"time": "2024-03-17T16:03:07Z"}, {"time": "2024-03-17T16:03:07Z"}),
            (OrganizationMemberships, {}, {"updated_at": "2025-03-17T16:03:07Z"}, {"updated_at": "2025-03-17T16:03:07Z"}),
        ],
        ids=[
            "Groups",
            "GroupMemberships",
            "TicketForms",
            "TicketMetricEvents",
            "OrganizationMemberships",
        ],
    )
    def test_get_updated_state(self, stream_cls, current_state, last_record, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.get_updated_state(current_state, last_record)
        assert expected == result

    @pytest.mark.parametrize(
        "stream_cls, response, expected",
        [
            (Groups, {}, None),
            (GroupMemberships, {}, None),
            (TicketForms, {}, None),
            (TicketMetricEvents, {}, None),
            (TicketAudits, {}, None),
            (SatisfactionRatings, {}, None),
            (
                OrganizationMemberships,
                {
                    "meta": {"has_more": True, "after_cursor": "<after_cursor>", "before_cursor": "<before_cursor>"},
                    "links": {
                        "prev": "https://subdomain.zendesk.com/api/v2/ticket_metrics.json?page%5Bbefore%5D=<before_cursor>%3D&page%5Bsize%5D=2",
                        "next": "https://subdomain.zendesk.com/api/v2/ticket_metrics.json?page%5Bafter%5D=<after_cursor>%3D&page%5Bsize%5D=2",
                    },
                },
                {"page[after]": "<after_cursor>"},
            ),
        ],
        ids=[
            "Groups",
            "GroupMemberships",
            "TicketForms",
            "TicketMetricEvents",
            "TicketAudits",
            "SatisfactionRatings",
            "OrganizationMemberships",
        ],
    )
    def test_next_page_token(self, requests_mock, stream_cls, response, expected):
        stream = stream_cls(**STREAM_ARGS)
        # stream_name = snake_case(stream.__class__.__name__)
        requests_mock.get(STREAM_URL, json=response)
        test_response = requests.get(STREAM_URL)
        output = stream.next_page_token(test_response)
        assert output == expected

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (Groups, 1622505600),
            (GroupMemberships, 1622505600),
            (TicketForms, 1622505600),
            (TicketMetricEvents, 1622505600),
            (OrganizationMemberships, 1622505600),
        ],
        ids=[
            "Groups",
            "GroupMemberships",
            "TicketForms",
            "TicketMetricEvents",
            "OrganizationMemberships",
        ],
    )
    def test_check_stream_state(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.check_stream_state()
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (Groups, {"page[size]": 100, "start_time": 1622505600, "exclude_deleted": False}),
            (GroupMemberships, {"page[size]": 100, "start_time": 1622505600, "sort_by": "asc"}),
            (TicketForms, {}),
            (TicketMetricEvents, {"page[size]": 1000, "start_time": 1622505600}),
            (TicketAudits, {"page[size]": 100}),
            (SatisfactionRatings, {"page[size]": 100, "start_time": 1622505600}),
            (OrganizationMemberships, {"page[size]": 100, "start_time": 1622505600})
        ],
        ids=[
            "Groups",
            "GroupMemberships",
            "TicketForms",
            "TicketMetricEvents",
            "TicketAudits",
            "SatisfactionRatings",
            "OrganizationMemberships",
        ],
    )
    def test_request_params(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.request_params(stream_state=None, next_page_token=None)
        assert expected == result


class TestSourceZendeskIncrementalExportStream:
    @pytest.mark.parametrize(
        "stream_cls",
        [
            (Users),
            (Tickets),
            (TicketMetrics),
        ],
        ids=[
            "Users",
            "Tickets",
            "TicketMetrics",
        ],
    )
    def test_check_start_time_param(self, stream_cls):
        expected = int(dict(parse_qsl(urlparse(STREAM_URL).query)).get("start_time"))
        stream = stream_cls(**STREAM_ARGS)
        result = stream.check_start_time_param(expected)
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (Users, "incremental/users/cursor"),
            (Tickets, "incremental/tickets/cursor"),
            (TicketMetrics, "incremental/tickets/cursor"),
        ],
        ids=[
            "Users",
            "Tickets",
            "TicketMetrics",
        ],
    )
    def test_path(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.path()
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls",
        [
            (Users),
            (Tickets),
            (TicketMetrics),
            (Organizations),
        ],
        ids=[
            "Users",
            "Tickets",
            "TicketMetrics",
            "Organizations",
        ],
    )
    def test_next_page_token(self, requests_mock, stream_cls):
        stream = stream_cls(**STREAM_ARGS)
        stream_name = snake_case(stream.__class__.__name__)
        requests_mock.get(STREAM_URL, json={stream_name: {}})
        test_response = requests.get(STREAM_URL)
        output = stream.next_page_token(test_response)
        assert output is None

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (Users, {"start_time": 1622505600}),
            (Tickets, {"start_time": 1622505600}),
            (TicketMetrics, {"start_time": 1622505600, "include": "metric_sets"}),
            (Organizations, {"start_time": 1622505600}),
        ],
        ids=[
            "Users",
            "Tickets",
            "TicketMetrics",
            "Organizations",
        ],
    )
    def test_request_params(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.request_params(next_page_token=None, stream_state=None)
        assert expected == result

    @pytest.mark.parametrize(
        "stream_cls",
        [
            (Users),
            (Tickets),
            (Organizations),
        ],
        ids=[
            "Users",
            "Tickets",
            "Organizations",
        ],
    )
    def test_parse_response(self, requests_mock, stream_cls):
        stream = stream_cls(**STREAM_ARGS)
        stream_name = snake_case(stream.__class__.__name__)
        expected = [{"updated_at": "2022-03-17T16:03:07Z"}]
        requests_mock.get(STREAM_URL, json={stream_name: expected})
        test_response = requests.get(STREAM_URL)
        output = list(stream.parse_response(test_response))
        assert expected == output


class TestTicketMetricsStream:
    @pytest.mark.parametrize(
        "stream_cls",
        [
            (TicketMetrics),
        ],
        ids=[
            "TicketMetrics",
        ],
    )
    def test_parse_response(self, requests_mock, stream_cls):
        stream = stream_cls(**STREAM_ARGS)
        stream_name = snake_case(stream.__class__.__name__)
        expected = []
        requests_mock.get(STREAM_URL, json={stream_name: expected})
        test_response = requests.get(STREAM_URL)
        output = list(stream.parse_response(test_response))
        assert expected == output


class TestSourceZendeskSupportTicketEventsExportStream:
    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (TicketComments, True),
        ],
        ids=[
            "TicketComments",
        ],
    )
    def test_update_event_from_record(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.update_event_from_record
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls",
        [
            (TicketComments),
        ],
        ids=[
            "TicketComments",
        ],
    )
    def test_parse_response(self, requests_mock, stream_cls):
        stream = stream_cls(**STREAM_ARGS)
        stream_name = snake_case(stream.__class__.__name__)
        requests_mock.get(STREAM_URL, json={stream_name: []})
        test_response = requests.get(STREAM_URL)
        output = list(stream.parse_response(test_response))
        assert output == []

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (TicketComments, "created_at"),
        ],
        ids=[
            "TicketComments",
        ],
    )
    def test_cursor_field(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.cursor_field
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (TicketComments, "ticket_events"),
        ],
        ids=[
            "TicketComments",
        ],
    )
    def test_response_list_name(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.response_list_name
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (TicketComments, "child_events"),
        ],
        ids=[
            "TicketComments",
        ],
    )
    def test_response_target_entity(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.response_target_entity
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (TicketComments, ["via_reference_id", "ticket_id", "timestamp"]),
        ],
        ids=[
            "TicketComments",
        ],
    )
    def test_list_entities_from_event(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.list_entities_from_event
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (TicketComments, "Comment"),
        ],
        ids=[
            "TicketComments",
        ],
    )
    def test_event_type(self, stream_cls, expected):
        stream = stream_cls(**STREAM_ARGS)
        result = stream.event_type
        assert result == expected


def test_read_tickets_stream(requests_mock):
    requests_mock.get(
        "https://subdomain.zendesk.com/api/v2/incremental/tickets/cursor",
        json={
            "tickets": [
                {"custom_fields": []},
                {},
                {
                    "custom_fields": [
                        {"id": 360023382300, "value": None},
                        {"id": 360004841380, "value": "customer_tickets"},
                        {"id": 360022469240, "value": "5"},
                        {"id": 360023712840, "value": False},
                    ]
                },
            ]
        },
    )

    stream = Tickets(subdomain="subdomain", start_date="2020-01-01T00:00:00Z")
    records = read_full_refresh(stream)
    assert records == [
        {"custom_fields": []},
        {},
        {
            "custom_fields": [
                {"id": 360023382300, "value": None},
                {"id": 360004841380, "value": "customer_tickets"},
                {"id": 360022469240, "value": "5"},
                {"id": 360023712840, "value": "false"},
            ]
        },
    ]
