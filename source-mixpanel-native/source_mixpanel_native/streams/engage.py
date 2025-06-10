#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.core import IncrementalMixin, StreamData
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer

from .base import MixpanelStream

DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%S"


def _dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)

def _str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


class Engage(MixpanelStream, IncrementalMixin):
    """Return list of all users
    API Docs: https://developer.mixpanel.com/reference/engage
    Endpoint: https://mixpanel.com/api/2.0/engage
    """

    http_method: str = "POST"
    data_field: str = "results"
    primary_key: str = "distinct_id"
    _total: Any = None
    _cursor_value = ''

    @property
    def cursor_field(self) -> str:
        """Name of the field associated with the state"""
        return "last_seen"

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]

    @property
    def source_defined_cursor(self) -> bool:
        return False

    @property
    def supports_incremental(self) -> bool:
        return True

    # enable automatic object mutation to align with desired schema before outputting to the destination
    transformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        config_start_date = datetime.combine(self.start_date, datetime.min.time())
        cursor: str | None = self.state.get(self.cursor_field, None)

        start_date = _str_to_dt(cursor) if cursor else config_start_date
        end_date = _str_to_dt(pendulum.now(tz=self.project_timezone).to_datetime_string())
        window_size = min(30, self.date_window_size)

        while start_date < end_date:
            slice_end = min(start_date + timedelta(days=window_size), end_date)
            stream_slice = {
                "start": start_date,
                "end": slice_end,
            }

            yield stream_slice

            start_date = slice_end

    def path(self, **kwargs) -> str:
        return "engage"

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:
        return {"include_all_users": True}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        params = {
            **params,
            "page_size": self.page_size,
            "where": f'properties["$last_seen"]>"{_dt_to_str(stream_slice['start'])}" and properties["$last_seen"]<="{_dt_to_str(stream_slice['end'])}"'
        }

        if next_page_token:
            params.update(next_page_token)

        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        page_number = decoded_response.get("page")
        total = decoded_response.get("total")  # exist only on first page
        if total:
            self._total = total

        if self._total and page_number is not None and self._total > self.page_size * (page_number + 1):
            return {
                "session_id": decoded_response.get("session_id"),
                "page": page_number + 1,
            }
        else:
            self._total = None
            return None

    def process_response(self, response: requests.Response, stream_state: Mapping[str, Any], **kwargs) -> Iterable[Mapping]:
        """
        {
            "page": 0
            "page_size": 1000
            "session_id": "1234567890-EXAMPL"
            "status": "ok"
            "total": 1
            "results": [{
                "$distinct_id": "9d35cd7f-3f06-4549-91bf-198ee58bb58a"
                "$properties":{
                    "$browser":"Chrome"
                    "$browser_version":"83.0.4103.116"
                    "$city":"Leeds"
                    "$country_code":"GB"
                    "$region":"Leeds"
                    "$timezone":"Europe/London"
                    "unblocked":"true"
                    "$email":"nadine@asw.com"
                    "$first_name":"Nadine"
                    "$last_name":"Burzler"
                    "$name":"Nadine Burzler"
                    "id":"632540fa-d1af-4535-bc52-e331955d363e"
                    "$last_seen":"2020-06-28T12:12:31"
                    ...
                    }
                },{
                ...
                }
            ]

        }
        """
        records = response.json().get(self.data_field, [])
        for record in records:
            item = {"distinct_id": record["$distinct_id"]}
            properties = record["$properties"]
            for property_name in properties:
                this_property_name = property_name
                if property_name.startswith("$"):
                    # Just remove leading '$' for 'reserved' mixpanel properties name, example:
                    # from API: '$browser'
                    # to stream: 'browser'
                    this_property_name = this_property_name[1:]
                item[this_property_name] = properties[property_name]
            item_cursor = item.get(self.cursor_field)
            if item.get("last_seen"):
                item["last_seen"] = item["last_seen"] + "+00:00"
            state_cursor = stream_state.get(self.cursor_field)
            if not item_cursor or not state_cursor or item_cursor >= state_cursor:
                yield item

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        yield from self._read_pages(
            lambda req, res, state, _slice: self.parse_response(res, stream_slice=_slice, stream_state=state), stream_slice, stream_state
        )

        # Update state after finishing reading a stream slice.
        self.state = {self.cursor_field: _dt_to_str(stream_slice['end'])}

    def request_kwargs(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {"stream": True}
