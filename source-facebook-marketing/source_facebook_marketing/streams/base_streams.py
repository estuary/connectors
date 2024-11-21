#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from functools import partial
from queue import Queue
from typing import TYPE_CHECKING, Any, Iterable, List, Mapping, MutableMapping, Optional

import pendulum
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.api import FacebookAdsApiBatch, FacebookRequest, FacebookResponse

from .common import deep_merge

if TYPE_CHECKING:  # pragma: no cover
    from source_facebook_marketing.api import API

logger = logging.getLogger("airbyte")

FACEBOOK_BATCH_ERROR_CODE = 960


class FBMarketingStream(Stream, ABC):
    """Base stream class"""

    primary_key = "id"
    transformer: TypeTransformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)

    # use batch API to retrieve details for each record in a stream
    use_batch = True
    # this flag will override `include_deleted` option for streams that does not support it
    enable_deleted = True
    # entity prefix for `include_deleted` filter, it usually matches singular version of stream name
    entity_prefix = None

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def __init__(self, api: "API", account_ids: List[str], include_deleted: bool = False, page_size: int = 100, max_batch_size: int = 50, source_defined_primary_key: list | None = None,**kwargs):
        super().__init__(**kwargs)
        self._api = api
        self.page_size = page_size if page_size is not None else 100
        self._account_ids = account_ids
        self._include_deleted = include_deleted if self.enable_deleted else False
        self.max_batch_size = max_batch_size if max_batch_size is not None else 50
        self.source_defined_primary_key = source_defined_primary_key if source_defined_primary_key is not None else ["id"]

        self._fields = None

    def fields(self, **kwargs) -> List[str]:
        """List of fields that we want to query, for now just all properties from stream's schema"""
        if self._fields:
            return self._fields
        self._saved_fields = list(self.get_json_schema().get("properties", {}).keys())
        return self._saved_fields

    @staticmethod
    def add_account_id(record, account_id: str):
        if "account_id" not in record:
            record["account_id"] = account_id

    def get_account_state(self, account_id: str, stream_state: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        if stream_state and account_id and account_id in stream_state:
            account_state = stream_state.get(account_id)

            # copy `include_deleted` from general stream state
            if "include_deleted" in stream_state:
                account_state["include_deleted"] = stream_state["include_deleted"]
            return account_state
        elif len(self._account_ids) == 1:
            return stream_state
        else:
            return {}

    def _transform_state(self, state: Mapping[str, Any], fields_to_move: List[str] = None) -> Mapping[str, Any]:
        """
        Transform state from the previous format that did not include account ids
        to the new format that contains account ids.
        """
        # If the state already contains any account IDs, it does not need transformed.
        for id in self._account_ids:
            if id in state:
                return state

        # If there is pre-existing state AND there's a single account ID,
        # nest the existing state under that account id.
        if state and len(self._account_ids) == 1:
            nested_state = {self._account_ids[0]: state}

            for field in fields_to_move:
                if field in state:
                    nested_state[field] = state.pop(field)

            return nested_state

        # Otherwise, the state is empty and we should return an empty dict.
        return {}

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        account_id = stream_slice["account_id"]
        account_state = stream_slice.get("stream_state", {})

        for record in self.list_objects(
            params=self.request_params(stream_state=account_state),
            account_id=account_id,
        ):
            if isinstance(record, AbstractObject):
                record = record.export_all_data()
            self.add_account_id(record, account_id)
            yield record

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for account_id in self._account_ids:
            account_state = self.get_account_state(account_id, stream_state)
            yield {"account_id": account_id, "stream_state": account_state}

    @abstractmethod
    def list_objects(self, params: Mapping[str, Any], account_id: str) -> Iterable:
        """List FB objects, these objects will be loaded in read_records later with their details.

        :param params: params to make request
        :return: list of FB objects to load
        """

    def request_params(self, **kwargs) -> MutableMapping[str, Any]:
        """Parameters that should be passed to query_records method"""
        params = {"limit": self.page_size}

        if self._include_deleted:
            params.update(self._filter_all_statuses())

        return params

    def _filter_all_statuses(self) -> MutableMapping[str, Any]:
        """Filter that covers all possible statuses thus including deleted/archived records"""
        filt_values = [
            "active",
            "archived",
            "completed",
            "limited",
            "not_delivering",
            "deleted",
            "not_published",
            "pending_review",
            "permanently_deleted",
            "recently_completed",
            "recently_rejected",
            "rejected",
            "scheduled",
            "inactive",
        ]

        return {
            "filtering": [
                {"field": f"{self.entity_prefix}.delivery_info", "operator": "IN", "value": filt_values},
            ],
        }


class FBMarketingIncrementalStream(FBMarketingStream, ABC):
    """Base class for incremental streams"""

    cursor_field = "updated_time"

    def __init__(self, start_date: datetime, end_date: datetime, **kwargs):
        super().__init__(**kwargs)
        self._start_date = pendulum.instance(start_date)
        self._end_date = pendulum.instance(end_date)

        if self._end_date < self._start_date:
            logger.error("The end_date must be after start_date.")

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]):
        """Update stream state from latest record"""
        # Get the stream state for the account associated with the latest record.
        account_id = latest_record["account_id"]
        stream_state = self._transform_state(current_stream_state, ["include_deleted"])
        account_state = self.get_account_state(account_id, stream_state)

        # Determine the max cursor value seen so far for this account id.
        potentially_new_records_in_the_past = self._include_deleted and not account_state.get("include_deleted", False)
        record_value = latest_record[self.cursor_field]
        state_value = account_state.get(self.cursor_field) or record_value
        max_cursor = max(pendulum.parse(state_value), pendulum.parse(record_value))
        if potentially_new_records_in_the_past:
            max_cursor = record_value

        # Update the stream state.
        stream_state.setdefault(account_id, {})[self.cursor_field] = str(max_cursor)
        stream_state["include_deleted"] = self._include_deleted

        return stream_state

    def request_params(self, stream_state: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        """Include state filter"""
        params = super().request_params(**kwargs)
        params = deep_merge(params, self._state_filter(stream_state=stream_state or {}))
        return params

    def _state_filter(self, stream_state: Mapping[str, Any]) -> Mapping[str, Any]:
        """Additional filters associated with state if any set"""
        state_value = stream_state.get(self.cursor_field)
        filter_value = self._start_date if not state_value else pendulum.parse(state_value)

        potentially_new_records_in_the_past = self._include_deleted and not stream_state.get("include_deleted", False)
        if potentially_new_records_in_the_past:
            self.logger.info(f"Ignoring bookmark for {self.name} because of enabled `include_deleted` option")
            filter_value = self._start_date

        return {
            "filtering": [
                {
                    "field": f"{self.entity_prefix}.{self.cursor_field}",
                    "operator": "GREATER_THAN",
                    "value": filter_value.int_timestamp,
                },
            ],
        }


class FBMarketingReversedIncrementalStream(FBMarketingIncrementalStream, ABC):
    """The base class for streams that don't support filtering and return records sorted desc by cursor_value"""

    enable_deleted = False  # API don't have any filtering, so implement include_deleted in code
    cursor_field: str

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cursor_values = {}
        # self._max_cursor_value = None

    @property
    def state(self) -> Mapping[str, Any]:
        s: dict[str, Any] = {}

        if self._cursor_values:
            for account_id, cursor_value in self._cursor_values.items():
                s[account_id] = {self.cursor_field: cursor_value}

            s["include_deleted"] = self._include_deleted

        return s

    @state.setter
    def state(self, value: Mapping[str, Any]):
        """State setter, ignore state if current settings mismatch saved state"""
        transformed_state = self._transform_state(value, ["include_deleted"])
        if self._include_deleted and not transformed_state.get("include_deleted"):
            logger.info(f"Ignoring bookmark for {self.name} because of enabled `include_deleted` option")
            return

        self._cursor_values = {}
        for account_id in self._account_ids:
            cursor_value = transformed_state.get(account_id, {}).get(self.cursor_field, None)
            if cursor_value is not None:
                self._cursor_values[account_id] = pendulum.parse(cursor_value)

    def _state_filter(self, stream_state: Mapping[str, Any]) -> Mapping[str, Any]:
        """Don't have classic cursor filtering"""
        return {}

    def get_record_deleted_status(self, record) -> bool:
        return False

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """Main read method used by CDK
        - save initial state
        - save maximum value (it is the first one)
        - update state only when we reach the end
        - stop reading when we reached the end
        """
        account_id = stream_slice["account_id"]
        account_state = stream_slice.get("stream_state")

        cursor = self._cursor_values.get(account_id)
        max_cursor = None

        for record in self.list_objects(params=self.request_params(stream_state=account_state), account_id=account_id):
            record_cursor_value = pendulum.parse(record[self.cursor_field])
            if cursor and record_cursor_value < cursor:
                break
            if not self._include_deleted and self.get_record_deleted_status(record):
                continue

            if max_cursor:
                max_cursor = max(max_cursor, record_cursor_value)
            else:
                max_cursor = record_cursor_value

            record = record.export_all_data()
            self.add_account_id(record, account_id)
            yield record

        self._cursor_values[account_id] = max_cursor
