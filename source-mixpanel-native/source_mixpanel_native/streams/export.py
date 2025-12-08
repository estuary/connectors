#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import re
from functools import cache
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

import pendulum
import requests
from pendulum import Date

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.core import IncrementalMixin, StreamData
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator

from ..property_transformation import transform_property_names
from .base import DateSlicesMixin, IncrementalMixpanelStream, MixpanelStream


class ExportSchema(MixpanelStream):
    """
    Export helper stream for dynamic schema extraction.
    :: reqs_per_hour_limit: int - property is set to the value of 1 million,
       to get the sleep time close to the zero, while generating dynamic schema.
       When `reqs_per_hour_limit = 0` - it means we skip this limits.
    """

    primary_key: str = None
    data_field: str = None
    reqs_per_hour_limit: int = 0  # see the docstring

    @property
    def state_checkpoint_interval(self) -> int:
        # to meet the requirement of emitting state at least once per 15 minutes,
        # we assume there's at least 1 record per request returned. Given that each request is followed by a 60 seconds sleep
        # we'll have to emit state every 15 records
        return 100000

    def path(self, **kwargs) -> str:
        return "events/properties/top"

    def process_response(self, response: requests.Response, **kwargs) -> Iterable[str]:
        """
        response.json() example:
        {
            "$browser": {
                "count": 6
            },
            "$browser_version": {
                "count": 6
            },
            "$current_url": {
                "count": 6
            },
            "mp_lib": {
                "count": 6
            },
            "noninteraction": {
                "count": 6
            },
            "$event_name": {
                "count": 6
            },
            "$duration_s": {},
            "$event_count": {},
            "$origin_end": {},
            "$origin_start": {}
        }
        """
        records = response.json()
        for property_name in records:
            yield property_name


class Export(DateSlicesMixin, MixpanelStream, IncrementalMixin):
    """Export event data as it is received and stored within Mixpanel, complete with all event properties
     (including distinct_id) and the exact timestamp the event was fired.

    API Docs: https://developer.mixpanel.com/reference/export
    Endpoint: https://data.mixpanel.com/api/2.0/export

    Raw Export API Rate Limit (https://help.mixpanel.com/hc/en-us/articles/115004602563-Rate-Limits-for-API-Endpoints):
     A maximum of 100 concurrent queries,
     3 queries per second and 60 queries per hour.
    """

    primary_key: str = "insert_id"
    cursor_field: str = "time"
    _cursor_value = ""

    transformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)

    def __init__(
        self,
        authenticator: HttpAuthenticator,
        region: str,
        project_timezone: str,
        start_date: Date = None,
        end_date: Date = None,
        date_window_size: int = 30,  # in days
        attribution_window: int = 0,  # in days
        project_id: int = None,
        reqs_per_hour_limit: int = MixpanelStream.DEFAULT_REQS_PER_HOUR_LIMIT,
        **kwargs,
    ):
        # This stream has a ton of data, so we can't use large date windows or we'll OOM the connector.
        # 30 days seems to work for existing connectors.
        MAX_DATE_WINDOW_SIZE = 30
        smaller_date_window = min([date_window_size, MAX_DATE_WINDOW_SIZE])

        super().__init__(
            authenticator=authenticator,
            region=region,
            project_timezone=project_timezone,
            start_date=start_date,
            end_date=end_date,
            date_window_size=smaller_date_window,
            attribution_window=attribution_window,
            project_id=project_id,
            reqs_per_hour_limit=reqs_per_hour_limit,
            **kwargs,
        )

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]

    @property
    def url_base(self):
        prefix = "-eu" if self.region == "EU" else ""
        return f"https://data{prefix}.mixpanel.com/api/2.0/"

    def path(self, **kwargs) -> str:
        return "export"

    def should_retry(self, response: requests.Response) -> bool:
        try:
            # trying to parse response to avoid ConnectionResetError and retry if it occurs
            self.iter_dicts(response.iter_lines(decode_unicode=True))
        except ConnectionResetError:
            return True
        return super().should_retry(response)

    def is_valid_record(self, potential_record: Any) -> bool:
        return isinstance(potential_record, dict) and potential_record.get('event', None) and potential_record.get('properties', None)

    def iter_dicts(self, lines):
        """
        The incoming stream has to be JSON lines format.
        From time to time for some reason, the one record can be split into multiple lines.
        We try to combine such split parts into one record only if parts go nearby.
        """
        parts = []
        for record_line in lines:
            if record_line == "terminated early":
                self.logger.warning(f"Couldn't fetch data from Export API. Response: {record_line}")
                return
            try:
                record = json.loads(record_line)
                if self.is_valid_record(record):
                    yield record
                else:
                    raise ValueError()
            except ValueError:
                parts.append(record_line)
            else:
                parts = []

            if len(parts) > 1:
                try:
                    record = json.loads("".join(parts))
                    if self.is_valid_record(record):
                        yield record
                    else:
                        raise ValueError()
                except ValueError:
                    pass
                else:
                    parts = []

    def process_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """Export API return response in JSONL format but each line is a valid JSON object
        Raw item example:
            {
                "event": "Viewed E-commerce Page",
                "properties": {
                    "time": 1623860880,
                    "distinct_id": "1d694fd9-31a5-4b99-9eef-ae63112063ed",
                    "$browser": "Chrome",                                           -> will be renamed to "browser"
                    "$browser_version": "91.0.4472.101",
                    "$current_url": "https://unblockdata.com/solutions/e-commerce/",
                    "$insert_id": "c5eed127-c747-59c8-a5ed-d766f48e39a4",
                    "$mp_api_endpoint": "api.mixpanel.com",
                    "mp_lib": "Segment: analytics-wordpress",
                    "mp_processing_time_ms": 1623886083321,
                    "noninteraction": true
                }
            }
        """

        # We prefer response.iter_lines() to response.text.split_lines() as the later can missparse text properties embeding linebreaks
        for record in self.iter_dicts(response.iter_lines(decode_unicode=True)):
            # transform record into flat dict structure
            item = {"event": record["event"]}
            properties = record["properties"]
            for result in transform_property_names(properties.keys()):
                # Convert all values to string (this is default property type)
                # because API does not provide properties type information

                # Sometimes, the API returns a document that has a `insert_id�` property instead of `insert_id`. 
                # When this happens, we have to remove the extra byte(s).
                if "insert_id" == result.transformed_name[:9]:
                    item["insert_id"] = str(properties[result.source_name])
                # Cancelled Session events have an "Id" property instead of an "id" property. We force the property to be "id".
                elif "id" == result.transformed_name.casefold():
                    item["id"] = str(properties[result.source_name])
                else:
                    item[result.transformed_name] = str(properties[result.source_name])

            # convert timestamp to datetime string
            item["time"] = pendulum.from_timestamp(int(item["time"]), tz="UTC").to_iso8601_string()

            yield item

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        # additional filter by timestamp because required start date and end date only allow to filter by date
        # Only apply this filter when not re-capturing events in the attribution window.
        cursor_param = stream_slice.get(self.cursor_field)
        use_attribution_window = stream_slice.get("use_attribution_window")
        if not use_attribution_window and cursor_param:
            timestamp = int(pendulum.parse(cursor_param).timestamp())
            params["where"] = f'properties["$time"]>=datetime({timestamp})'

        return params

    def request_kwargs(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {"stream": True}

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        most_recent_cursor = self.state.get(self.cursor_field, None)

        for record in super().read_records(sync_mode, cursor_field, stream_slice, stream_state):
            yield record

            record_cursor_value = record.get(self.cursor_field)

            if record_cursor_value:
                most_recent_cursor = max(record_cursor_value, most_recent_cursor)

            self.state = {self.cursor_field: most_recent_cursor}
