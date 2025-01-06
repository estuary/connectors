#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, List, Mapping, Optional, MutableMapping

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer

from .base import MixpanelStream
from .cohorts import Cohorts


class CohortMembers(MixpanelStream):
    """Return list of users grouped by cohort"""
    http_method: str = "POST"
    data_field: str = "results"
    primary_key: str = "distinct_id"
    _total: Any = None

    # enable automatic object mutation to align with desired schema before outputting to the destination
    transformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)

    def __init__(
        self,
        minimal_cohort_members_properties: bool = True,
        page_size: int = 50000,
        **kwargs,
    ):

        # Since we know that each result received from the Mixpanel API will be a consistent, pretty small size when the
        # minimal_cohort_members_properties flag is set, we can use a very large page size to page through results faster.
        # if minimal_cohort_members_properties:
        #     page_size = 1_000_000

        super().__init__(
            page_size=page_size,
            minimal_cohort_members_properties=minimal_cohort_members_properties,
            **kwargs,
        )

    def path(self, **kwargs) -> str:
        return "engage"

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:
        # example: {"filter_by_cohort": {"id": 1343181}}
        return {"filter_by_cohort": stream_slice}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        params = {**params, "page_size": self.page_size}

        # if self.minimal_cohort_members_properties:
        #     params['output_properties'] = '"$last_seen"'

        if next_page_token:
            params.update(next_page_token)

        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        page_number = response_json.get("page")
        total = response_json.get("total")  # exists only on first page
        if total:
            self._total = total

        if self._total and page_number is not None and self._total > self.page_size * (page_number + 1):
            return {
                "session_id": response_json.get("session_id"),
                "page": page_number + 1,
            }
        else:
            self._total = None
            return None

    def stream_slices(
        self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        # full refresh is needed for Cohorts because even though some cohorts might already have been read
        # they can still have members added or removed.
        cohorts = Cohorts(**self.get_stream_params()).read_records(SyncMode.full_refresh)
        # A single cohort could be empty (i.e. no members), so we only check for members in non-empty cohorts. 
        filtered_cohorts = [cohort for cohort in cohorts if cohort["count"] > 0]

        for cohort in filtered_cohorts:
            yield {"id": cohort["id"]}

    def process_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        for record in response.json().get(self.data_field, []):
            # Format record
            item = {"distinct_id": record["$distinct_id"]}
            properties = record["$properties"]
            for property_name in properties:
                this_property_name = property_name
                if property_name.startswith("$"):
                    # Remove leading '$' for 'reserved' mixpanel property names.
                    this_property_name = this_property_name[1:]
                item[this_property_name] = properties[property_name]

            item_cursor: str | None = item.get('last_seen', None)
            if item_cursor:
                item_cursor += "+00:00"
                item['last_seen'] = item_cursor

            item["cohort_id"] = stream_slice["id"]

            yield item
