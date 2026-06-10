#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from contextlib import nullcontext
from unittest.mock import patch

import pendulum
import pytest
import requests
from airbyte_cdk.sources.streams.http import HttpStream
from freezegun import freeze_time
from source_twilio.auth import HttpBasicAuthenticator
from source_twilio.source import SourceTwilio
from source_twilio.streams import (
    TWILIO_API_URL_BASE_VERSIONED,
    Accounts,
    Addresses,
    Alerts,
    Calls,
    DependentPhoneNumbers,
    MessageMedia,
    Messages,
    Recordings,
    TwilioNestedStream,
    TwilioStream,
    UsageRecords,
    UsageTriggers,
)

TEST_CONFIG = {
    "account_sid": "airbyte.io",
    "auth_token": "secret",
    "start_date": "2022-01-01T00:00:00Z",
    "lookback_window": 0,
}
TEST_CONFIG.update(
    **{
        "authenticator": HttpBasicAuthenticator((TEST_CONFIG["account_sid"], TEST_CONFIG["auth_token"])),
    }
)

TEST_INSTANCE = SourceTwilio()


class TestTwilioStream:

    CONFIG = {"authenticator": TEST_CONFIG.get("authenticator")}

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (Accounts, "accounts"),
        ],
    )
    def test_data_field(self, stream_cls, expected):
        stream = stream_cls(**self.CONFIG)
        result = stream.data_field
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (Accounts, ['name']),
        ],
    )
    def test_changeable_fields(self, stream_cls, expected):
        with patch.object(Accounts, "changeable_fields", ['name']):
          stream = stream_cls(**self.CONFIG)
          result = stream.changeable_fields
          assert result == expected

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (Accounts, "Accounts.json"),
        ],
    )
    def test_path(self, stream_cls, expected):
        stream = stream_cls(**self.CONFIG)
        result = stream.path()
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls, test_response, expected",
        [
            (
                Accounts,
                {
                    "next_page_uri": "/2010-04-01/Accounts/ACdad/Addresses.json?PageSize=1000&Page=2&PageToken=PAAD42931b949c0dedce94b2f93847fdcf95"
                },
                {"Page": "2", "PageSize": "1000", "PageToken": "PAAD42931b949c0dedce94b2f93847fdcf95"},
            ),
        ],
    )
    def test_next_page_token(self, requests_mock, stream_cls, test_response, expected):
        stream = stream_cls(**self.CONFIG)
        url = f"{stream.url_base}{stream.path()}"
        requests_mock.get(url, json=test_response)
        response = requests.get(url)
        result = stream.next_page_token(response)
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls, test_response, expected",
        [
            (Accounts, {"accounts": [{"id": "123", "name": "test"}]}, [{"id": "123"}]),
        ],
    )
    def test_parse_response(self, requests_mock, stream_cls, test_response, expected):
        with patch.object(TwilioStream, "changeable_fields", ["name"]):
          stream = stream_cls(**self.CONFIG)
          url = f"{stream.url_base}{stream.path()}"
          requests_mock.get(url, json=test_response)
          response = requests.get(url)
          result = list(stream.parse_response(response))
          assert result[0]['id'] == expected[0]['id']

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (Accounts, "5.5"),
        ],
    )
    def test_backoff_time(self, requests_mock, stream_cls, expected):
        stream = stream_cls(**self.CONFIG)
        url = f"{stream.url_base}{stream.path()}"
        test_headers = {"Retry-After": expected}
        requests_mock.get(url, headers=test_headers)
        response = requests.get(url)
        result = stream.backoff_time(response)
        assert result == float(expected)

    @pytest.mark.parametrize(
        "stream_cls, next_page_token, expected",
        [
            (
                Accounts,
                {"Page": "2", "PageSize": "1000", "PageToken": "PAAD42931b949c0dedce94b2f93847fdcf95"},
                {"Page": "2", "PageSize": "1000", "PageToken": "PAAD42931b949c0dedce94b2f93847fdcf95"},
            ),
        ],
    )
    def test_request_params(self, stream_cls, next_page_token, expected):
        stream = stream_cls(**self.CONFIG)
        result = stream.request_params(stream_state=None, next_page_token=next_page_token)
        assert result == expected

    @pytest.mark.parametrize(
        "original_value, field_schema, expected_value",
        [
            ("Fri, 11 Dec 2020 04:28:40 +0000", {"format": "date-time"}, "2020-12-11T04:28:40Z"),
            ("2020-12-11T04:28:40Z", {"format": "date-time"}, "2020-12-11T04:28:40Z"),
            ("some_string", {}, "some_string"),
        ]
    )
    def test_transform_function(self, original_value, field_schema, expected_value):
        assert Accounts.custom_transform_function(original_value, field_schema) == expected_value


class TestIncrementalTwilioStream:

    CONFIG = TEST_CONFIG
    CONFIG.pop("account_sid")
    CONFIG.pop("auth_token")

    @pytest.mark.parametrize(
        "stream_cls, stream_slice, next_page_token, expected",
        [
            (
                Calls,
                {"EndTime>": "2022-01-01", "EndTime<": "2022-01-02"},
                {"Page": "2", "PageSize": "1000", "PageToken": "PAAD42931b949c0dedce94b2f93847fdcf95"},
                {
                    "EndTime>": "2022-01-01",
                    "EndTime<": "2022-01-02",
                    "Page": "2",
                    "PageSize": "1000",
                    "PageToken": "PAAD42931b949c0dedce94b2f93847fdcf95",
                },
            ),
        ],
    )
    def test_request_params(self, stream_cls, stream_slice, next_page_token, expected):
        stream = stream_cls(**self.CONFIG)
        result = stream.request_params(stream_state=None, stream_slice=stream_slice, next_page_token=next_page_token)
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls, record, expected",
        [
            (Calls, [{"end_time": "2022-02-01T00:00:00Z"}], [{"end_time": "2022-02-01T00:00:00Z"}]),
        ],
    )
    def test_read_records(self, stream_cls, record, expected):
        stream = stream_cls(**self.CONFIG)
        with patch.object(HttpStream, "read_records", return_value=record):
            result = stream.read_records(sync_mode=None)
            assert list(result) == expected

    @pytest.mark.parametrize(
        "stream_cls, parent_cls_records, extra_slice_keywords",
        [
            (Calls, [{"subresource_uris": {"calls": "123"}}, {"subresource_uris": {"calls": "124"}}], ["subresource_uri"]),
            (Alerts, [{}], []),
        ],
    )
    def test_stream_slices(self, mocker, stream_cls, parent_cls_records, extra_slice_keywords):
        stream = stream_cls(
            authenticator=TEST_CONFIG.get("authenticator"), start_date=pendulum.now().subtract(months=13).to_iso8601_string()
        )
        expected_slices = 2 * len(parent_cls_records)  # 2 per year slices per each parent slice
        if isinstance(stream, TwilioNestedStream):
            slices_mock_context = mocker.patch.object(stream.parent_stream_instance, "stream_slices", return_value=[{}])
            records_mock_context = mocker.patch.object(stream.parent_stream_instance, "read_records", return_value=parent_cls_records)
        else:
            slices_mock_context, records_mock_context = nullcontext(), nullcontext()
        with slices_mock_context:
            with records_mock_context:
                slices = list(stream.stream_slices(sync_mode="incremental"))
        assert len(slices) == expected_slices
        for slice_ in slices:
            if isinstance(stream, TwilioNestedStream):
                for kw in extra_slice_keywords:
                    assert kw in slice_
            assert slice_[stream.lower_boundary_filter_field] <= slice_[stream.upper_boundary_filter_field]

    @freeze_time("2022-11-16 12:03:11+00:00")
    @pytest.mark.parametrize(
        "stream_cls, state, expected_dt_ranges",
        (
            (
                Messages,
                {"date_sent": "2022-11-13 23:39:00"},
                [
                    {"DateSent>": "2022-11-13 23:39:00Z", "DateSent<": "2022-11-14 23:39:00Z"},
                    {"DateSent>": "2022-11-14 23:39:00Z", "DateSent<": "2022-11-15 23:39:00Z"},
                    {"DateSent>": "2022-11-15 23:39:00Z", "DateSent<": "2022-11-16 12:03:11Z"},
                ],
            ),
            (UsageRecords, {"start_date": "2021-11-16 00:00:00"}, [{"StartDate": "2021-11-16", "EndDate": "2022-11-16"}]),
            (
                Recordings,
                {"date_created": "2021-11-16 00:00:00"},
                [
                    {"DateCreated>": "2021-11-16 00:00:00Z", "DateCreated<": "2022-11-16 00:00:00Z"},
                    {"DateCreated>": "2022-11-16 00:00:00Z", "DateCreated<": "2022-11-16 12:03:11Z"},
                ],
            ),
        ),
    )
    def test_generate_dt_ranges(self, stream_cls, state, expected_dt_ranges):
        stream = stream_cls(authenticator=TEST_CONFIG.get("authenticator"), start_date="2000-01-01 00:00:00")
        stream.state = state
        dt_ranges = list(stream.generate_date_ranges())
        assert dt_ranges == expected_dt_ranges


class TestTwilioNestedStream:

    CONFIG = {"authenticator": TEST_CONFIG.get("authenticator")}

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (Addresses, {}),
            (DependentPhoneNumbers, {}),
            (MessageMedia, {"num_media": "0"}),
        ],
    )
    def test_media_exist_validation(self, stream_cls, expected):
        stream = stream_cls(**self.CONFIG)
        result = stream.media_exist_validation
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls, parent_stream, record, expected",
        [
            (
                Addresses,
                Accounts,
                [{"subresource_uris": {"addresses": "123"}}],
                [{"subresource_uri": "123"}],
            ),
            (
                DependentPhoneNumbers,
                Addresses,
                [{"subresource_uris": {"addresses": "123"}, "sid": "123", "account_sid": "456"}],
                [{"sid": "123", "account_sid": "456"}],
            ),
        ],
    )
    def test_stream_slices(self, stream_cls, parent_stream, record, expected):
        stream = stream_cls(**self.CONFIG)
        with patch.object(Accounts, "read_records", return_value=record):
            with patch.object(parent_stream, "stream_slices", return_value=record):
                with patch.object(parent_stream, "read_records", return_value=record):
                    result = stream.stream_slices(sync_mode="full_refresh")
                    assert list(result) == expected


class TestUsageNestedStream:

    CONFIG = {"authenticator": TEST_CONFIG.get("authenticator")}

    @pytest.mark.parametrize(
        "stream_cls, expected",
        [
            (UsageTriggers, "Triggers"),
        ],
    )
    def test_path_name(self, stream_cls, expected):
        stream = stream_cls(**self.CONFIG)
        result = stream.path_name
        assert result == expected

    @pytest.mark.parametrize(
        "stream_cls, parent_stream, record, expected",
        [
            (
                UsageTriggers,
                Accounts,
                [{"sid": "234", "account_sid": "678"}],
                [{"account_sid": "234"}],
            ),
        ],
    )
    def test_stream_slices(self, stream_cls, parent_stream, record, expected):
        stream = stream_cls(**self.CONFIG)
        with patch.object(Accounts, "read_records", return_value=record):
            with patch.object(parent_stream, "stream_slices", return_value=record):
                with patch.object(parent_stream, "read_records", return_value=record):
                    result = stream.stream_slices()
                    assert list(result) == expected


class TestAccountsSubaccountFiltering:
    """Tests for the `sync_subaccounts` option on the Accounts stream."""

    MAIN = {"sid": "AC_main", "owner_account_sid": "AC_main", "friendly_name": "main"}
    SUB_1 = {"sid": "AC_sub1", "owner_account_sid": "AC_main", "friendly_name": "sub1"}
    SUB_2 = {"sid": "AC_sub2", "owner_account_sid": "AC_main", "friendly_name": "sub2"}

    def _parse(self, stream, records, requests_mock):
        requests_mock.get(
            f"{TWILIO_API_URL_BASE_VERSIONED}Accounts.json",
            json={"accounts": records},
        )
        response = requests.get(f"{TWILIO_API_URL_BASE_VERSIONED}Accounts.json")
        return list(stream.parse_response(response))

    def test_sync_subaccounts_enabled_returns_all_accounts(self, requests_mock):
        stream = Accounts(authenticator=TEST_CONFIG.get("authenticator"), sync_subaccounts=True)
        result = self._parse(stream, [self.MAIN, self.SUB_1, self.SUB_2], requests_mock)
        assert result == [self.MAIN, self.SUB_1, self.SUB_2]

    def test_sync_subaccounts_default_is_enabled(self, requests_mock):
        stream = Accounts(authenticator=TEST_CONFIG.get("authenticator"))
        result = self._parse(stream, [self.MAIN, self.SUB_1], requests_mock)
        assert result == [self.MAIN, self.SUB_1]

    def test_sync_subaccounts_disabled_returns_only_main_account(self, requests_mock):
        stream = Accounts(
            authenticator=TEST_CONFIG.get("authenticator"),
            sync_subaccounts=False,
            config_account_sid="SK_some_api_key_sid",
        )
        result = self._parse(stream, [self.MAIN, self.SUB_1, self.SUB_2], requests_mock)
        assert result == [self.MAIN]

    def test_sync_subaccounts_disabled_retains_configured_subaccount(self, requests_mock):
        # A capture authenticated with a subaccount's own credentials only sees that
        # subaccount, whose owner_account_sid differs from its sid. It must still be
        # captured when subaccount sync is disabled.
        stream = Accounts(
            authenticator=TEST_CONFIG.get("authenticator"),
            sync_subaccounts=False,
            config_account_sid="AC_sub1",
        )
        result = self._parse(stream, [self.SUB_1], requests_mock)
        assert result == [self.SUB_1]

    def test_nested_stream_parent_inherits_flags(self):
        stream = Calls(
            authenticator=TEST_CONFIG.get("authenticator"),
            start_date=TEST_CONFIG["start_date"],
            sync_subaccounts=False,
            config_account_sid="AC_main",
        )
        parent = stream.parent_stream_instance
        assert parent.sync_subaccounts is False
        assert parent.config_account_sid == "AC_main"
