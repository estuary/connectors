#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from functools import cache
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer

from .base import IncrementalMixpanelStream, MixpanelStream


class Engage(IncrementalMixpanelStream):
    """Return list of all users
    API Docs: https://developer.mixpanel.com/reference/engage
    Endpoint: https://mixpanel.com/api/2.0/engage
    """

    http_method: str = "POST"
    data_field: str = "results"
    primary_key: str = "distinct_id"
    page_size: int = 50000  # min 100
    _total: Any = None
    cursor_field = "last_seen"

    @property
    def state_checkpoint_interval(self) -> int:
        # to meet the requirement of emitting state at least once per 15 minutes,
        # we assume there's at least 1 record per request returned. Given that each request is followed by a 60 seconds sleep
        # we'll have to emit state every 15 records
        return 10000

    @property
    def source_defined_cursor(self) -> bool:
        return False

    @property
    def supports_incremental(self) -> bool:
        return True

    # enable automatic object mutation to align with desired schema before outputting to the destination
    transformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)

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
        params = {**params, "page_size": self.page_size}
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

    def set_cursor(self, cursor_field: List[str]):
        if not cursor_field:
            raise Exception("cursor_field is not defined")
        if len(cursor_field) > 1:
            raise Exception("multidimensional cursor_field is not supported")
        self.cursor_field = cursor_field[0]

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        if sync_mode == SyncMode.incremental:
            self.set_cursor(cursor_field)
        return super().stream_slices(sync_mode=sync_mode, cursor_field=cursor_field, stream_state=stream_state)
