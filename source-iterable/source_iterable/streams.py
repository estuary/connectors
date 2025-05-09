#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import csv
import json
import urllib.parse as urlparse
from abc import ABC, abstractmethod
from io import StringIO
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Union

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.core import IncrementalMixin, package_name_from_class
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException, UserDefinedBackoffException
from airbyte_cdk.sources.utils.schema_helpers import ResourceSchemaLoader
from pendulum.datetime import DateTime
from datetime import datetime
from requests import HTTPError, codes
from requests.exceptions import ChunkedEncodingError
from source_iterable.slice_generators import AdjustableSliceGenerator, RangeSliceGenerator, StreamSlice
from source_iterable.utils import dateutil_parse

EVENT_ROWS_LIMIT = 200
CAMPAIGNS_PER_REQUEST = 20


class IterableStream(HttpStream, ABC):
    # in case we get a 401 error (api token disabled or deleted) on a stream slice, do not make further requests within the current stream
    # to prevent 429 error on other streams
    ignore_further_slices = False

    url_base = "https://api.iterable.com/api/"
    primary_key = "id"

    def __init__(self, authenticator):
        self._cred = authenticator
        self._slice_retry = 0
        super().__init__(authenticator)

    @property
    def retry_factor(self) -> int:
        return 20

    # With factor 20 it would be from 20 to 400 seconds delay
    @property
    def max_retries(self) -> Union[int, None]:
        return 10

    @property
    @abstractmethod
    def data_field(self) -> str:
        """
        :return: Default field name to get data from response
        """

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Iterable API does not support pagination
        """
        return None

    def check_unauthorized_key(self, response: requests.Response) -> bool:
        if response.status_code == codes.UNAUTHORIZED:
            self.logger.warning(f"Provided API Key has not sufficient permissions to read from stream: {self.data_field}")
            return True

    def check_generic_error(self, response: requests.Response) -> bool:
        """
        https://github.com/airbytehq/oncall/issues/1592#issuecomment-1499109251
        https://github.com/airbytehq/oncall/issues/1985
        """
        codes = ["Generic Error", "GenericError"]
        msg_pattern = "Please try again later"

        if response.status_code == 500:
            # I am not sure that all 500 errors return valid json
            try:
                response_json = json.loads(response.text)
            except ValueError:
                return
            if response_json.get("code") in codes and msg_pattern in response_json.get("msg", ""):
                return True

    def request_kwargs(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        """
        https://requests.readthedocs.io/en/latest/user/advanced/#timeouts
        https://github.com/airbytehq/oncall/issues/1985#issuecomment-1559276465
        """
        return {"timeout": (60, 300)}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json() or {}
        records = response_json.get(self.data_field, [])
        for record in records:
            yield record

    def should_retry(self, response: requests.Response) -> bool:
        if self.check_generic_error(response):
            self._slice_retry += 1
            if self._slice_retry < 3:
                return True
            return False
        return super().should_retry(response)

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        self._slice_retry = 0
        if self.ignore_further_slices:
            return

        try:
            yield from super().read_records(sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)
        except (HTTPError, UserDefinedBackoffException, DefaultBackoffException) as e:
            response = e.response
            if self.check_unauthorized_key(response):
                self.ignore_further_slices = True
                return
            if self.check_generic_error(response):
                return
            raise e


class IterableExportStream(IterableStream, IncrementalMixin, ABC):
    """
    This stream utilize "export" Iterable api for getting large amount of data.
    It returns results in JSONL format. Data can be split up into date ranges
    by applying startDateTime and endDateTime parameters. A single request 
    could return a large volume of data, and the endpoint's rate limit is 4
    requests per minute.

    Returned results are not sorted per their cursor fields, so cursors cannot be checkpointed
    by inspecting each result's cursor field value & checkpointing the max value found so far.
    That strategy causes the connector's cursor to skip ahead before processing all results in
    a date window. Instead, the state is updated to the end of the date window after all results
    in a date window are processed.

    Details: https://api.iterable.com/api/docs#export_exportDataJson
    """

    cursor_field = "createdAt"
    primary_key = None

    def __init__(self, start_date=None, end_date=None, **kwargs):
        super().__init__(**kwargs)
        self._start_date = pendulum.parse(start_date)
        self._end_date = end_date and pendulum.parse(end_date)
        self.stream_params = {"dataTypeName": self.data_field}
        self._state = {}

    def path(self, **kwargs) -> str:
        return "export/data.json"
    
    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._state = value

    @staticmethod
    def _field_to_datetime(value: Union[int, str]) -> pendulum.datetime:
        if isinstance(value, int):
            value = pendulum.from_timestamp(value / 1000.0)
        elif isinstance(value, str):
            value = dateutil_parse(value)
        else:
            raise ValueError(f"Unsupported type of datetime field {type(value)}")
        return value


    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: StreamSlice,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:

        params = super().request_params(stream_state=stream_state)
        params.update(
            {
                "startDateTime": stream_slice.start_date.strftime("%Y-%m-%d %H:%M:%S"),
                "endDateTime": stream_slice.end_date.strftime("%Y-%m-%d %H:%M:%S"),
            },
            **self.stream_params,
        )
        return params

    def parse_response(self, response: requests.Response, stream_slice: StreamSlice = None, **kwargs) -> Iterable[Mapping]:
        for obj in response.iter_lines():
            record = json.loads(obj)
            record[self.cursor_field] = self._field_to_datetime(record[self.cursor_field])
            yield record

        self.state = {self.cursor_field: stream_slice.end_date}

    def request_kwargs(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        """
        https://api.iterable.com/api/docs#export_exportDataJson
        Sending those type of requests could download large piece of json
        objects splitted with newline character.
        Passing stream=True argument to requests.session.send method to avoid
        loading whole analytics report content into memory.
        """
        return {
            **super().request_kwargs(stream_state, stream_slice, next_page_token),
            "stream": True,
        }

    def get_start_date(self, stream_state: Mapping[str, Any]) -> DateTime:
        stream_state = stream_state or {}
        start_datetime = self._start_date
        if stream_state.get(self.cursor_field):
            start_datetime = pendulum.parse(stream_state[self.cursor_field])
        return start_datetime

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Optional[StreamSlice]]:

        start_datetime = self.get_start_date(stream_state)
        return [StreamSlice(start_datetime, self._end_date or pendulum.now("UTC"))]


class IterableExportStreamRanged(IterableExportStream, ABC):
    """
    This class use RangeSliceGenerator class to break single request into
    ranges with same (or less for final range) number of days. By default it 90
    days.
    """

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Optional[StreamSlice]]:

        start_datetime = self.get_start_date(stream_state)

        return RangeSliceGenerator(start_datetime, self._end_date)


class IterableExportStreamAdjustableRange(IterableExportStream, ABC):
    """
    For streams that could produce large amount of data in single request so we
    cant just use IterableExportStreamRanged to split it in even ranges. If
    request processing takes a lot of time API server could just close
    connection and connector code would fail with ChunkedEncodingError.

    To solve this problem we use AdjustableSliceGenerator that able to adjust
    next slice range based on two factor:
    1. Previous slice range / time to process ratio.
    2. Had previous request failed with ChunkedEncodingError

    In case of slice processing request failed with ChunkedEncodingError (which
    means that API server closed connection cause of request takes to much
    time) make CHUNKED_ENCODING_ERROR_RETRIES (6) retries each time reducing
    slice length.

    See AdjustableSliceGenerator description for more details on next slice length adjustment alghorithm.
    """

    _adjustable_generator: AdjustableSliceGenerator = None
    CHUNKED_ENCODING_ERROR_RETRIES = 6

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Optional[StreamSlice]]:

        start_datetime = self.get_start_date(stream_state)
        self._adjustable_generator = AdjustableSliceGenerator(start_datetime, self._end_date)
        return self._adjustable_generator

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str],
        stream_slice: StreamSlice,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        start_time = pendulum.now()
        for _ in range(self.CHUNKED_ENCODING_ERROR_RETRIES):
            try:

                self.logger.info(
                    f"Processing slice of {(stream_slice.end_date - stream_slice.start_date).total_days()} days for stream {self.name}"
                )
                for record in super().read_records(
                    sync_mode=sync_mode,
                    cursor_field=cursor_field,
                    stream_slice=stream_slice,
                    stream_state=stream_state,
                ):
                    now = pendulum.now()
                    self._adjustable_generator.adjust_range(now - start_time)
                    yield record
                    start_time = now
                break
            except ChunkedEncodingError:
                self.logger.warn("ChunkedEncodingError occurred, decrease days range and try again")
                stream_slice = self._adjustable_generator.reduce_range()
        else:
            raise Exception(f"ChunkedEncodingError: Reached maximum number of retires: {self.CHUNKED_ENCODING_ERROR_RETRIES}")


class IterableExportEventsStreamAdjustableRange(IterableExportStreamAdjustableRange, ABC):
    def get_json_schema(self) -> Mapping[str, Any]:
        """All child stream share the same 'events' schema"""
        return ResourceSchemaLoader(package_name_from_class(self.__class__)).get_schema("events")


class Lists(IterableStream):
    data_field = "lists"

    def path(self, **kwargs) -> str:
        return "lists"


class ListUsers(IterableStream):
    primary_key = "listId"
    data_field = "getUsers"
    name = "list_users"
    # enable caching, because this stream used by other ones
    use_cache = True

    def path(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> str:
        return f"lists/{self.data_field}?listId={stream_slice['list_id']}"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        lists = Lists(authenticator=self._cred)
        for list_record in lists.read_records(sync_mode=kwargs.get("sync_mode", SyncMode.full_refresh)):
            yield {"list_id": list_record["id"]}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        list_id = self._get_list_id(response.url)
        for user in response.iter_lines():
            yield {"email": user.decode(), "listId": list_id}

    @staticmethod
    def _get_list_id(url: str) -> int:
        parsed_url = urlparse.urlparse(url)
        for q in parsed_url.query.split("&"):
            key, value = q.split("=")
            if key == "listId":
                return int(value)


class Campaigns(IterableStream):
    data_field = "campaigns"
    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "campaigns"


class CampaignsMetrics(IterableStream):
    name = "campaigns_metrics"
    primary_key = "id"
    data_field = None

    def __init__(self, start_date: str, end_date: Optional[str] = None, **kwargs):
        """
        https://api.iterable.com/api/docs#campaigns_metrics
        """
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date

    def path(self, **kwargs) -> str:
        return "campaigns/metrics"

    def request_params(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["campaignId"] = stream_slice.get("campaign_ids")
        params["startDateTime"] = self.start_date
        if self.end_date:
            params["endDateTime"] = self.end_date
        return params

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        lists = Campaigns(authenticator=self._cred)
        campaign_ids = []
        for list_record in lists.read_records(sync_mode=kwargs.get("sync_mode", SyncMode.full_refresh)):
            campaign_ids.append(list_record["id"])

            if len(campaign_ids) == CAMPAIGNS_PER_REQUEST:
                yield {"campaign_ids": campaign_ids}
                campaign_ids = []

        if campaign_ids:
            yield {"campaign_ids": campaign_ids}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        content = response.content.decode()
        records = self._parse_csv_string_to_dict(content)

        for record in records:
            yield {"data": record, "id": record['id']}

    @staticmethod
    def _parse_csv_string_to_dict(csv_string: str) -> List[Dict[str, Any]]:
        """
        Parse a response with a csv type to dict object
        Example:
            csv_string = "a,b,c,d
                          1,2,,3
                          6,,1,2"

            output = [{"a": 1, "b": 2, "d": 3},
                      {"a": 6, "c": 1, "d": 2}]


        :param csv_string: API endpoint response with csv format
        :return: parsed API response

        """

        reader = csv.DictReader(StringIO(csv_string), delimiter=",")
        result = []

        for row in reader:
            for key, value in row.items():
                if value == "":
                    continue
                try:
                    row[key] = int(value)
                except ValueError:
                    row[key] = float(value)
            row = {k: v for k, v in row.items() if v != ""}

            result.append(row)

        return result


class Channels(IterableStream):
    data_field = "channels"

    def path(self, **kwargs) -> str:
        return "channels"


class MessageTypes(IterableStream):
    data_field = "messageTypes"
    name = "message_types"

    def path(self, **kwargs) -> str:
        return "messageTypes"


class Metadata(IterableStream):
    primary_key = None
    data_field = "results"

    def path(self, **kwargs) -> str:
        return "metadata"


class Events(IterableStream):
    """
    https://api.iterable.com/api/docs#export_exportUserEvents
    """

    primary_key = None
    data_field = "events"
    common_fields = ("itblInternal", "_type", "createdAt", "email")

    def path(self, **kwargs) -> str:
        return "export/userEvents"

    def request_params(self, stream_slice: Optional[Mapping[str, Any]], **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params.update({"email": stream_slice["email"], "includeCustomEvents": "true"})

        return params

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        lists = ListUsers(authenticator=self._cred)
        stream_slices = lists.stream_slices()

        for stream_slice in stream_slices:
            for list_record in lists.read_records(sync_mode=kwargs.get("sync_mode", SyncMode.full_refresh), stream_slice=stream_slice):
                yield {"email": list_record["email"]}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Parse jsonl response body.
        Put common event fields at the top level.
        Put the rest of the fields in the `data` subobject.
        """
        jsonl_records = StringIO(response.text)
        for record in jsonl_records:
            record_dict = json.loads(record)
            record_dict_common_fields = {}
            for field in self.common_fields:
                record_dict_common_fields[field] = record_dict.pop(field, None)

            yield {**record_dict_common_fields, "data": record_dict}


class EmailBounce(IterableExportStreamAdjustableRange):
    data_field = "emailBounce"
    primary_key = ["messageId", "email"]


class EmailClick(IterableExportStreamAdjustableRange):
    data_field = "emailClick"
    primary_key = ["messageId", "email"]


class EmailComplaint(IterableExportStreamAdjustableRange):
    data_field = "emailComplaint"
    primary_key = ["messageId", "email"]


class EmailOpen(IterableExportStreamAdjustableRange):
    data_field = "emailOpen"
    primary_key = ["messageId", "email"]


class EmailSend(IterableExportStreamAdjustableRange):
    data_field = "emailSend"
    primary_key = ["messageId", "email"]

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str],
        stream_slice: StreamSlice,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        start_time = pendulum.now()
        for _ in range(self.CHUNKED_ENCODING_ERROR_RETRIES):
            try:

                self.logger.info(
                    f"Processing slice of {(stream_slice.end_date - stream_slice.start_date).total_days()} days for stream {self.name}"
                )
                for record in super().read_records(
                    sync_mode=sync_mode,
                    cursor_field=cursor_field,
                    stream_slice=stream_slice,
                    stream_state=stream_state,
                ):
                    now = pendulum.now()
                    self._adjustable_generator.adjust_range(now - start_time)
                    if record.get("transactionalData") and type(record["transactionalData"]) is str:
                        record["transactionalData"] = json.loads(record["transactionalData"])

                    if record.get("createdAt") and type(record.get("createdAt")) is str:
                        record["createdAt"] = datetime.strptime(record["createdAt"], "%Y-%m-%d %H:%M:%S %z")
                    
                    if record.get("itblInternal"):
                        if record["itblInternal"].get("documentCreatedAt") and type(record["itblInternal"]["documentCreatedAt"]) is str:
                            record["itblInternal"]["documentCreatedAt"] = datetime.strptime(record["itblInternal"]["documentCreatedAt"], "%Y-%m-%d %H:%M:%S %z")
                            
                        if record["itblInternal"].get("documentUpdatedAt") and type(record["itblInternal"]["documentCreatedAt"]) is str:
                            record["itblInternal"]["documentUpdatedAt"] = datetime.strptime(record["itblInternal"]["documentUpdatedAt"], "%Y-%m-%d %H:%M:%S %z")

                    if record.get("transactionalData",{}).get("createdAt") and type(record["transactionalData"]["createdAt"]) is str:
                        record["transactionalData"]["createdAt"] = datetime.strptime(record["transactionalData"]["createdAt"], "%Y-%m-%d %H:%M:%S %z")
                    
                    if record.get("transactionalData",{}).get("eventUpdatedAt") and type(record["transactionalData"]["eventUpdatedAt"]) is str:
                        record["transactionalData"]["eventUpdatedAt"] = datetime.strptime(record["transactionalData"]["eventUpdatedAt"], "%Y-%m-%d %H:%M:%S %z")
                    
                    if record.get("transactionalData",{}).get("itblInternal"):
                        if record["transactionalData"]["itblInternal"].get("documentCreatedAt") and type(record["transactionalData"]["itblInternal"]["documentCreatedAt"]) is str:
                            record["transactionalData"]["itblInternal"]["documentCreatedAt"] = datetime.strptime(record["transactionalData"]["itblInternal"]["documentCreatedAt"], "%Y-%m-%d %H:%M:%S %z")
                            
                        if record["transactionalData"]["itblInternal"].get("documentUpdatedAt") and type(record["transactionalData"]["itblInternal"]["documentUpdatedAt"]) is str:
                            record["transactionalData"]["itblInternal"]["documentUpdatedAt"] = datetime.strptime(record["transactionalData"]["itblInternal"]["documentUpdatedAt"], "%Y-%m-%d %H:%M:%S %z")

                    yield record
                    start_time = now
                break
            except ChunkedEncodingError:
                self.logger.warn("ChunkedEncodingError occurred, decrease days range and try again")
                stream_slice = self._adjustable_generator.reduce_range()
        else:
            raise Exception(f"ChunkedEncodingError: Reached maximum number of retires: {self.CHUNKED_ENCODING_ERROR_RETRIES}")


class EmailSendSkip(IterableExportStreamAdjustableRange):
    data_field = "emailSendSkip"
    primary_key = ["messageId", "email"]


class EmailSubscribe(IterableExportStreamAdjustableRange):
    data_field = "emailSubscribe"
    primary_key = ["email", "createdAt"]


class EmailUnsubscribe(IterableExportStreamAdjustableRange):
    data_field = "emailUnsubscribe"
    primary_key = ["email"]


class PushSend(IterableExportEventsStreamAdjustableRange):
    data_field = "pushSend"
    primary_key = ["email", "createdAt"]


class PushSendSkip(IterableExportEventsStreamAdjustableRange):
    data_field = "pushSendSkip"
    primary_key = ["email", "createdAt"]


class PushOpen(IterableExportEventsStreamAdjustableRange):
    data_field = "pushOpen"
    primary_key = ["email", "createdAt"]


class PushUninstall(IterableExportEventsStreamAdjustableRange):
    data_field = "pushUninstall"
    primary_key = ["email", "createdAt"]


class PushBounce(IterableExportEventsStreamAdjustableRange):
    data_field = "pushBounce"
    primary_key = ["email", "createdAt"]


class WebPushSend(IterableExportEventsStreamAdjustableRange):
    data_field = "webPushSend"
    primary_key = ["email", "createdAt"]


class WebPushClick(IterableExportEventsStreamAdjustableRange):
    data_field = "webPushClick"
    primary_key = ["email", "createdAt"]


class WebPushSendSkip(IterableExportEventsStreamAdjustableRange):
    data_field = "webPushSendSkip"
    primary_key = ["email", "createdAt"]


class InAppSend(IterableExportEventsStreamAdjustableRange):
    data_field = "inAppSend"
    primary_key = ["email", "createdAt"]


class InAppOpen(IterableExportEventsStreamAdjustableRange):
    data_field = "inAppOpen"
    primary_key = ["email", "createdAt"]


class InAppClick(IterableExportEventsStreamAdjustableRange):
    data_field = "inAppClick"
    primary_key = ["email", "createdAt"]


class InAppClose(IterableExportEventsStreamAdjustableRange):
    data_field = "inAppClose"
    primary_key = ["email", "createdAt"]


class InAppDelete(IterableExportEventsStreamAdjustableRange):
    data_field = "inAppDelete"
    primary_key = ["email", "createdAt"]


class InAppDelivery(IterableExportEventsStreamAdjustableRange):
    data_field = "inAppDelivery"
    primary_key = ["email", "createdAt"]


class InAppSendSkip(IterableExportEventsStreamAdjustableRange):
    data_field = "inAppSendSkip"
    primary_key = ["email", "createdAt"]


class InboxSession(IterableExportEventsStreamAdjustableRange):
    data_field = "inboxSession"
    primary_key = ["email", "createdAt"]


class InboxMessageImpression(IterableExportEventsStreamAdjustableRange):
    data_field = "inboxMessageImpression"
    primary_key = ["email", "createdAt"]


class SmsSend(IterableExportEventsStreamAdjustableRange):
    data_field = "smsSend"
    primary_key = ["email", "createdAt"]


class SmsBounce(IterableExportEventsStreamAdjustableRange):
    data_field = "smsBounce"
    primary_key = ["email", "createdAt"]


class SmsClick(IterableExportEventsStreamAdjustableRange):
    data_field = "smsClick"
    primary_key = ["email", "createdAt"]


class SmsReceived(IterableExportEventsStreamAdjustableRange):
    data_field = "smsReceived"
    primary_key = ["email", "createdAt"]


class SmsSendSkip(IterableExportEventsStreamAdjustableRange):
    data_field = "smsSendSkip"
    primary_key = ["email", "createdAt"]


class SmsUsageInfo(IterableExportEventsStreamAdjustableRange):
    data_field = "smsUsageInfo"
    primary_key = ["email", "createdAt"]


class Purchase(IterableExportEventsStreamAdjustableRange):
    data_field = "purchase"
    primary_key = ["email", "createdAt"]


class CustomEvent(IterableExportEventsStreamAdjustableRange):
    data_field = "customEvent"
    primary_key = ["email", "createdAt"]


class HostedUnsubscribeClick(IterableExportEventsStreamAdjustableRange):
    data_field = "hostedUnsubscribeClick"
    primary_key = ["email", "createdAt"]


class Templates(IterableExportStreamRanged):
    data_field = "templates"
    template_types = ["Base", "Blast", "Triggered", "Workflow"]
    message_types = ["Email", "Push", "InApp", "SMS"]
    primary_key = "templateId"

    def path(self, **kwargs) -> str:
        return "templates"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        for template in self.template_types:
            for message in self.message_types:
                self.stream_params = {"templateType": template, "messageMedium": message}
                yield from super().read_records(stream_slice=stream_slice, **kwargs)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        records = response_json.get(self.data_field, [])

        for record in records:
            record[self.cursor_field] = self._field_to_datetime(record[self.cursor_field])
            yield record


class Users(IterableExportStreamRanged):
    data_field = "user"
    cursor_field = "profileUpdatedAt"
    primary_key = "email"


    def read_records(self, **kwargs) -> Iterable[Mapping[str, Any]]:
        # These datetime fields are not ISO 8601 compliant, so we re-format them so they comply
        # with their schematized format: datetime annotation.
        DATETIME_FIELDS_TO_FORMAT = ["signupDate", "shopify_created_at", "shopify_updated_at"]

        for record in super().read_records(**kwargs):
            for field in DATETIME_FIELDS_TO_FORMAT:
                if record.get(field):
                    record[field] = datetime.strptime(record[field], "%Y-%m-%d %H:%M:%S %z")
            yield record


class AccessCheck(Users):
    # since 401 error is failed silently in all the streams,
    # we need another class to distinguish an empty stream from 401 response
    def check_unauthorized_key(self, response: requests.Response) -> bool:
        # this allows not retrying 401 and raising the error upstream
        return response.status_code != codes.UNAUTHORIZED
