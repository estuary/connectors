#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import calendar
import functools
import logging
import re
import time
from abc import ABC
from collections import deque
from concurrent.futures import Future, ProcessPoolExecutor
from datetime import datetime, timedelta, UTC
from functools import partial
from math import ceil
from pickle import PickleError, dumps
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union, Callable
from urllib.parse import parse_qsl, urljoin, urlparse

import pendulum
import pytz
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.core import IncrementalMixin, StreamData
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth.core import HttpAuthenticator
from airbyte_cdk.sources.streams.http.availability_strategy import HttpAvailabilityStrategy
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException
from airbyte_cdk.sources.streams.http.rate_limiting import TRANSIENT_EXCEPTIONS
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from requests.auth import AuthBase
from requests_futures.sessions import PICKLE_ERROR, FuturesSession
from source_zendesk_support.ZendeskSupportAvailabilityStrategy import ZendeskSupportAvailabilityStrategy

DATETIME_FORMAT: str = "%Y-%m-%dT%H:%M:%SZ"
LAST_END_TIME_KEY: str = "_last_end_time"
END_OF_STREAM_KEY: str = "end_of_stream"

logger = logging.getLogger("airbyte")

# For some streams, multiple http requests are running at the same time for performance reasons.
# However, it may result in hitting the rate limit, therefore subsequent requests have to be made after a pause.
# The idea is to sustain a pause once and continue making multiple requests at a time.
# A single `retry_at` variable is introduced here, which prevents us from duplicate sleeping in the main thread
# before each request is made as it used to be in prior versions.
# It acts like a global counter - increased each time a 429 status is met
# only if it is greater than the current value. On the other hand, no request may be made before this moment.
# Because the requests are made in parallel, time.sleep will be called in parallel as well.
# This is possible because it is a point in time, not timedelta.
retry_at: Optional[datetime] = None


def sleep_before_executing(sleep_time: float):
    def wrapper(function):
        @functools.wraps(function)
        def inner(*args, **kwargs):
            logger.info(f"Sleeping {sleep_time} seconds before next request")
            time.sleep(int(sleep_time))
            result = function(*args, **kwargs)
            return result, datetime.utcnow()

        return inner

    return wrapper


def to_int(s):
    "https://github.com/airbytehq/airbyte/issues/13673"
    if isinstance(s, str):
        res = re.findall(r"[-+]?\d+", s)
        if res:
            return res[0]
    return s


def _s_to_dt_str(s: int) -> str:
    """
    Converts a UNIX timestamp in seconds to a date-time formatted string.
    """
    return datetime.fromtimestamp(s, tz=UTC).isoformat(' ')

def _dt_str_to_s(dt_str: str) -> int:
    """
    Converts a date-time formatted string to a UNIX timestamp in seconds.
    """
    return int(datetime.fromisoformat(dt_str).timestamp())

class SourceZendeskException(Exception):
    """default exception of custom SourceZendesk logic"""


class SourceZendeskSupportFuturesSession(FuturesSession):
    """
    Check the docs at https://github.com/ross/requests-futures
    Used to async execute a set of requests.
    """

    def send_future(self, request: requests.PreparedRequest, **kwargs) -> Future:
        """
        Use instead of default `Session.send()` method.
        `Session.send()` should not be overridden as it used by `requests-futures` lib.
        """

        if self.session:
            func = self.session.send
        else:
            sleep_time = 0
            now = datetime.utcnow()
            if retry_at and retry_at > now:
                sleep_time = (retry_at - datetime.utcnow()).seconds
            # avoid calling super to not break pickled method
            func = partial(requests.Session.send, self)
            func = sleep_before_executing(sleep_time)(func)

        if isinstance(self.executor, ProcessPoolExecutor):
            self.logger.warning("ProcessPoolExecutor is used to perform IO related tasks for unknown reason!")
            # verify function can be pickled
            try:
                dumps(func)
            except (TypeError, PickleError):
                raise RuntimeError(PICKLE_ERROR)

        return self.executor.submit(func, request, **kwargs)


class BaseSourceZendeskSupportStream(HttpStream, ABC):
    raise_on_http_errors = True

    def __init__(self, subdomain: str, start_date: str, ignore_pagination: bool = False, **kwargs):
        super().__init__(**kwargs)

        self._start_date = start_date
        self._subdomain = subdomain
        self._ignore_pagination = ignore_pagination

    @property
    def availability_strategy(self) -> Optional[AvailabilityStrategy]:
        return HttpAvailabilityStrategy()

    def backoff_time(self, response: requests.Response) -> Union[int, float]:
        """
        The rate limit is 700 requests per minute
        # monitoring-your-request-activity
        See https://developer.zendesk.com/api-reference/ticketing/account-configuration/usage_limits/
        The response has a Retry-After header that tells you for how many seconds to wait before retrying.
        """

        retry_after = int(to_int(response.headers.get("Retry-After", 0)))
        if retry_after > 0:
            return retry_after

        # the header X-Rate-Limit returns the amount of requests per minute
        rate_limit = float(response.headers.get("X-Rate-Limit", 0))
        if rate_limit and rate_limit > 0:
            return 60.0 / rate_limit
        return super().backoff_time(response)

    @staticmethod
    def str2datetime(str_dt: str) -> datetime:
        """convert string to datetime object
        Input example: '2021-07-22T06:55:55Z' FORMAT : "%Y-%m-%dT%H:%M:%SZ"
        """
        if not str_dt:
            return None
        return datetime.strptime(str_dt, DATETIME_FORMAT)

    @staticmethod
    def datetime2str(dt: datetime) -> str:
        """convert datetime object to string
        Output example: '2021-07-22T06:55:55Z' FORMAT : "%Y-%m-%dT%H:%M:%SZ"
        """
        return datetime.strftime(dt.replace(tzinfo=pytz.UTC), DATETIME_FORMAT)

    @staticmethod
    def str2unixtime(str_dt: str) -> Optional[int]:
        """convert string to unixtime number
        Input example: '2021-07-22T06:55:55Z' FORMAT : "%Y-%m-%dT%H:%M:%SZ"
        Output example: 1626936955"
        """
        if not str_dt:
            return None
        dt = datetime.strptime(str_dt, DATETIME_FORMAT)
        return calendar.timegm(dt.utctimetuple())

    def parse_response(self, response: requests.Response, stream_state: Mapping[str, Any], **kwargs) -> Iterable[Mapping]:
        """try to select relevant data only"""

        try:
            records = response.json().get(self.response_list_name or self.name) or []
        except requests.exceptions.JSONDecodeError:
            records = []

        if not self.cursor_field:
            yield from records
        else:
            cursor_date = (stream_state or {}).get(self.cursor_field)
            for record in records:
                updated = record[self.cursor_field]
                if not cursor_date or updated > cursor_date:
                    yield record

    def should_retry(self, response: requests.Response) -> bool:
        if response.status_code == 403 or response.status_code == 404:
            try:
                error = response.json().get("error")
            except requests.exceptions.JSONDecodeError:
                error = {"title": f"{response.reason}", "message": "Received empty JSON response"}
            self.logger.error(f"Skipping stream {self.name}: Check permissions, error message: {error}.")
            setattr(self, "raise_on_http_errors", False)
            return False
        if response.status_code != 200:
            self.logger.warning(f"Received a {response.status_code} response.")
        return super().should_retry(response)


class SourceZendeskSupportStream(BaseSourceZendeskSupportStream):
    """Basic Zendesk class"""

    primary_key = "id"

    page_size = 100
    cursor_field = "updated_at"

    response_list_name: str = None
    future_requests: deque = None

    transformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)

    def __init__(self, authenticator: Union[AuthBase, HttpAuthenticator] = None, **kwargs):
        super().__init__(**kwargs)

        self._session = SourceZendeskSupportFuturesSession()
        self._session.auth = authenticator
        self.future_requests = deque()

    @property
    def url_base(self) -> str:
        return f"https://{self._subdomain}.zendesk.com/api/v2/"

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return ZendeskSupportAvailabilityStrategy()

    def path(self, **kwargs):
        return self.name

    def next_page_token(self, *args, **kwargs):
        return None

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_benchmark = latest_record[self.cursor_field]
        if current_stream_state.get(self.cursor_field):
            return {self.cursor_field: max(latest_benchmark, current_stream_state[self.cursor_field])}
        return {self.cursor_field: latest_benchmark}

    def get_api_records_count(self, stream_slice: Mapping[str, Any] = None, stream_state: Mapping[str, Any] = None):
        """
        Count stream records before generating the future requests
        to then correctly generate the pagination parameters.
        """

        count_url = urljoin(self.url_base, f"{self.path(stream_state=stream_state, stream_slice=stream_slice)}/count.json")

        start_date = self._start_date
        params = {}
        if self.cursor_field and stream_state:
            start_date = stream_state.get(self.cursor_field)
        if start_date:
            params["start_time"] = self.str2datetime(start_date)

        response = self._session.request("get", count_url).result()
        records_count = response.json().get("count", {}).get("value", 0)

        return records_count

    def generate_future_requests(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ):
        records_count = self.get_api_records_count(stream_slice=stream_slice, stream_state=stream_state)
        self.logger.info(f"Records count is {records_count}")
        page_count = ceil(records_count / self.page_size)
        for page_number in range(1, page_count + 1):
            params = self.request_params(stream_state=stream_state, stream_slice=stream_slice)
            params["page"] = page_number
            request_headers = self.request_headers(stream_state=stream_state, stream_slice=stream_slice)

            request = self._create_prepared_request(
                path=self.path(stream_state=stream_state, stream_slice=stream_slice),
                headers=dict(request_headers, **self.authenticator.get_auth_header()),
                params=params,
                json=self.request_body_json(stream_state=stream_state, stream_slice=stream_slice),
                data=self.request_body_data(stream_state=stream_state, stream_slice=stream_slice),
            )

            request_kwargs = self.request_kwargs(stream_state=stream_state, stream_slice=stream_slice)
            self.future_requests.append(
                {
                    "future": self._send_request(request, request_kwargs),
                    "request": request,
                    "request_kwargs": request_kwargs,
                    "retries": 0,
                }
            )
        self.logger.info(f"Generated {len(self.future_requests)} future requests")

    def _send(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> Future:
        response: Future = self._session.send_future(request, **request_kwargs)
        return response

    def _send_request(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> Future:
        return self._send(request, request_kwargs)

    def request_params(
        self, stream_state: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, **kwargs
    ) -> MutableMapping[str, Any]:
        params = {}
        stream_state = stream_state or {}
        # try to search all records with generated_timestamp > start_time
        current_state = stream_state.get(self.cursor_field)
        if current_state and isinstance(current_state, str) and not current_state.isdigit():
            current_state = self.str2unixtime(current_state)
        start_time = current_state or calendar.timegm(pendulum.parse(self._start_date).utctimetuple())
        # +1 because the API returns all records where generated_timestamp >= start_time

        now = calendar.timegm(datetime.now().utctimetuple())
        if start_time > now - 60:
            # start_time must be more than 60 seconds ago
            start_time = now - 61
        params["start_time"] = start_time

        return params

    def _retry(
        self,
        request: requests.PreparedRequest,
        retries: int,
        original_exception: Exception = None,
        response: requests.Response = None,
        finished_at: Optional[datetime] = None,
        **request_kwargs,
    ):
        if retries == self.max_retries:
            if original_exception:
                raise original_exception
            raise DefaultBackoffException(request=request, response=response)
        sleep_time = self.backoff_time(response)
        if response is not None and finished_at and sleep_time:
            current_retry_at = finished_at + timedelta(seconds=sleep_time)
            global retry_at
            if not retry_at or (retry_at < current_retry_at):
                retry_at = current_retry_at
            self.logger.info(f"Adding a request to be retried in {sleep_time} seconds")
        self.future_requests.append(
            {
                "future": self._send_request(request, request_kwargs),
                "request": request,
                "request_kwargs": request_kwargs,
                "retries": retries + 1,
            }
        )

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        self.generate_future_requests(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)

        while len(self.future_requests) > 0:
            self.logger.info("Starting another while loop iteration")
            item = self.future_requests.popleft()
            request, retries, future, kwargs = item["request"], item["retries"], item["future"], item["request_kwargs"]

            try:
                response, finished_at = future.result()
            except TRANSIENT_EXCEPTIONS as exc:
                self.logger.info("Will retry the request because of a transient exception")
                self._retry(request=request, retries=retries, original_exception=exc, **kwargs)
                continue
            if self.should_retry(response):
                self.logger.info("Will retry the request for other reason")
                self._retry(request=request, retries=retries, response=response, finished_at=finished_at, **kwargs)
                continue
            self.logger.info("Request successful, will parse the response now")
            yield from self.parse_response(response, stream_state=stream_state, stream_slice=stream_slice)


class SourceZendeskSupportFullRefreshStream(BaseSourceZendeskSupportStream):
    """
    Endpoints don't provide the updated_at/created_at fields
    Thus we can't implement an incremental logic for them
    """

    page_size = 100
    primary_key = "id"
    response_list_name: str = None

    @property
    def url_base(self) -> str:
        return f"https://{self._subdomain}.zendesk.com/api/v2/"

    def path(self, **kwargs):
        return self.name

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if self._ignore_pagination:
            return None

        meta = {}
        if response.content:
            meta = response.json().get("meta", {})

        return {"page[after]": meta.get("after_cursor")} if meta.get("has_more") else None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {"page[size]": self.page_size}
        if next_page_token:
            params.update(next_page_token)
        return params


class SourceZendeskSupportCursorPaginationStream(SourceZendeskSupportFullRefreshStream):
    """
    Endpoints provide a cursor pagination and sorting mechanism
    """

    cursor_field = "updated_at"
    next_page_field = "next_page"
    prev_start_time = None

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        # try to save maximum value of a cursor field
        old_value = str((current_stream_state or {}).get(self.cursor_field, ""))
        new_value = str((latest_record or {}).get(self.cursor_field, ""))
        return {self.cursor_field: max(new_value, old_value)}

    def check_stream_state(self, stream_state: Mapping[str, Any] = None):
        """
        Returns the state value, if exists. Otherwise, returns user defined `Start Date`.
        """
        state = stream_state.get(self.cursor_field) or self._start_date if stream_state else self._start_date
        return calendar.timegm(pendulum.parse(state).utctimetuple())
    
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            "start_time": self.check_stream_state(stream_state),
            "page[size]": self.page_size,
        }
        if next_page_token:
            params.pop("start_time", None)
            params.update(next_page_token)
        return params


class SourceZendeskIncrementalExportStream(SourceZendeskSupportCursorPaginationStream):
    """Incremental Export from Tickets stream:
    https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/#incremental-ticket-export-time-based

    @ param response_list_name: the main nested entity to look at inside of response, default = response_list_name
    @ param sideload_param : parameter variable to include various information to response
        more info: https://developer.zendesk.com/documentation/ticketing/using-the-zendesk-api/side_loading/#supported-endpoints
    """

    response_list_name: str = None
    sideload_param: str = None

    @staticmethod
    def check_start_time_param(requested_start_time: int, value: int = 1):
        """
        Requesting tickets in the future is not allowed, hits 400 - bad request.
        We get current UNIX timestamp minus `value` from now(), default = 1 (minute).

        Returns: either close to now UNIX timestamp or previously requested UNIX timestamp.
        """
        now = calendar.timegm(pendulum.now().subtract(minutes=value).utctimetuple())
        return now if requested_start_time > now else requested_start_time

    def path(self, **kwargs) -> str:
        return f"incremental/{self.response_list_name}.json"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Returns next_page_token based on `end_of_stream` parameter inside of response
        """
        next_page_token = super().next_page_token(response)
        return None if response.json().get(END_OF_STREAM_KEY, False) else next_page_token

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {}
        next_page_token = next_page_token or {}
        parsed_state = self.check_stream_state(stream_state)
        if self.cursor_field:
            params = {"start_time": next_page_token.get(self.cursor_field, parsed_state)}
        else:
            params = {"start_time": calendar.timegm(pendulum.parse(self._start_date).utctimetuple())}

        # check "start_time" is not in the future
        params["start_time"] = self.check_start_time_param(params["start_time"])
        if self.sideload_param:
            params["include"] = self.sideload_param
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for record in response.json().get(self.response_list_name, []):
            yield record


class SourceZendeskSupportIncrementalTimeExportStream(SourceZendeskSupportFullRefreshStream, IncrementalMixin):
    """
    Incremental time-based export streams.
    API docs: https://developer.zendesk.com/documentation/ticketing/managing-tickets/using-the-incremental-export-api/#time-based-incremental-exports
    Airbyte Incremental Stream Docs: https://docs.airbyte.com/connector-development/cdk-python/incremental-stream for some background.

    Uses IncrementalMixin's state setter & getter to persist cursors between requests.

    Note: Incremental time-based export streams theoretically can get stuck in a loop if
    1000+ resources are updated at the exact same time.
    """
    state_checkpoint_interval = 1000
    _cursor_value = ""

    @property
    def cursor_field(self) -> str:
        """Name of the field associated with the state"""
        return "updated_at"

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]

    def path(self, **kwargs) -> str:
        return f"incremental/{self.response_list_name}"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if self._ignore_pagination:
            return None

        response_json = response.json()

        pagination_complete = response_json.get(END_OF_STREAM_KEY, False)
        if pagination_complete:
            return None

        next_start_time = response_json.get("end_time", None)
        if next_start_time:
            return {"start_time": next_start_time}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        if next_page_token:
            return next_page_token

        start_time_state = self.state.get(self.cursor_field, None)
        if start_time_state:
            return {"start_time": _dt_str_to_s(start_time_state)}
        else:
            return {"start_time": calendar.timegm(pendulum.parse(self._start_date).utctimetuple())}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()

        records = response_json.get(self.response_list_name, [])
        for record in records:
            yield record
        
        cursor = response_json.get("end_time", None)
        if cursor and len(records) != 0:
            self.state = {self.cursor_field: _s_to_dt_str(cursor)}


class SourceZendeskSupportIncrementalCursorExportStream(SourceZendeskIncrementalExportStream, IncrementalMixin):
    """
    Incremental cursor export for Users and Tickets streams
    Zendesk API docs: https://developer.zendesk.com/documentation/ticketing/managing-tickets/using-the-incremental-export-api/#cursor-based-incremental-exports
    Airbyte Incremental Stream Docs: https://docs.airbyte.com/connector-development/cdk-python/incremental-stream for some background.
    
    Uses IncrementalMixin's state setter & getter to persist cursors between requests.
    """

    state_checkpoint_interval = 1000
    _cursor_value = ""

    def __init__(self, is_parent = False, **kwargs):
        super().__init__(**kwargs)
        self._is_parent = is_parent

    @property
    def cursor_field(self) -> str:
        """Name of the field associated with the state"""
        return "after_cursor"

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]

    def path(self, **kwargs) -> str:
        return f"incremental/{self.response_list_name}/cursor"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if self._ignore_pagination:
            return None

        response_json = response.json()

        pagination_complete = response_json.get(END_OF_STREAM_KEY, None)
        if pagination_complete:
            return None

        cursor = response_json.get("after_cursor", None)
        if cursor:
            # Use the cursor from the most recent response to get the next page.
            return {"cursor": cursor}

    def request_params(
        self, stream_state: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, **kwargs
    ) -> MutableMapping[str, Any]:
        # Get the remaining pages if _ignore_pagination is false.
        if next_page_token:
            return next_page_token

        # Get the latest cursor from state if this is the start of a sweep.
        latest_cursor = self.state.get(self.cursor_field, None)
        if latest_cursor:
            return { "cursor": latest_cursor }

        # Otherwise, this is the first request and we need to provide a `start_time`.
        params = {"start_time": calendar.timegm(pendulum.parse(self._start_date).utctimetuple())}
        # check "start_time" is not in the future
        params["start_time"] = self.check_start_time_param(params["start_time"])

        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()

        records = response_json.get(self.response_list_name, [])
        cursor = response_json.get("after_cursor", None)

        total = len(records)
        count = 0

        for record in records:
            # If this stream is acting as a parent stream, yield the cursor before yielding the final "slice" of this response.
            # This is necessary due to Airbyte's state checkpointing semantics; state is checkpointed immediately after finishing processing a slice.
            if (
                self._is_parent and
                cursor and
                total != 0 and
                count == total - 1
                ):
                yield {'cursor': cursor}

            yield record
            count += 1


        if cursor and total != 0:
            self.state = {self.cursor_field: cursor}


class SourceZendeskSupportTicketEventsExportStream(SourceZendeskIncrementalExportStream):
    """Incremental Export from TicketEvents stream:
    https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/#incremental-ticket-event-export

    @ param response_list_name: the main nested entity to look at inside of response, default = "ticket_events"
    @ param response_target_entity: nested property inside of `response_list_name`, default = "child_events"
    @ param list_entities_from_event : the list of nested child_events entities to include from parent record
    @ param event_type : specific event_type to check ["Audit", "Change", "Comment", etc]
    """

    state_checkpoint_interval = 1000

    cursor_field = "created_at"
    response_list_name: str = "ticket_events"
    response_target_entity: str = "child_events"
    list_entities_from_event: List[str] = None
    event_type: str = None
    sideload_param: str = None
    next_page_field: str = "next_page"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Returns all necessary query params for the next page of results by parsing the next_page_field of the response.

        Note: This is different from other streams' next_page_token method (that return a single cursor param) due to 
        how this API endpoint's responses are structured.
        """
        if self._ignore_pagination:
            return None

        response_json = response.json()
        is_end_of_stream = response_json.get(END_OF_STREAM_KEY, False)

        if is_end_of_stream:
            return None

        params = dict(parse_qsl(urlparse(response_json.get(self.next_page_field, "")).query))

        return params

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """
        If this is not the first request in a sweep, returns the query params for the next page from the previous response.
        Otherwise, returns the start time param from the stream's state/config and the sideload param.
        """
        if next_page_token:
            # Check if the next_page_token's start time is the the same or earlier than
            # the previous request's / checkpointed state's start time.
            next_page_start_time = int(next_page_token.get("start_time"))
            checkpointed_start_time = self.check_stream_state(stream_state=stream_state)

            if next_page_start_time <= checkpointed_start_time:
                self.logger.warning(f"start_time query param {next_page_start_time} is less than or equal to the previous start_time param {checkpointed_start_time}. Check if the stream is stuck in a loop.")

            return next_page_token

        start_time = self.check_stream_state(stream_state=stream_state)
        start_time = self.check_start_time_param(start_time)

        params = {"start_time": start_time}

        if self.sideload_param:
            params.update({"include": self.sideload_param})

        return params

    @property
    def update_event_from_record(self) -> bool:
        """Returns True/False based on list_entities_from_event property"""
        return True if len(self.list_entities_from_event) > 0 else False

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for record in super().parse_response(response, **kwargs):
            for event in record.get(self.response_target_entity, []):
                if event.get("event_type") == self.event_type:
                    if self.update_event_from_record:
                        for prop in self.list_entities_from_event:
                            event[prop] = record.get(prop)
                    yield event


class Organizations(SourceZendeskSupportIncrementalTimeExportStream):
    """
    API docs: https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/#incremental-organization-export

    Note: This stream theoretically can get stuck in a loop if 1000+ organizations are
    updated at the exact same time. I don't anticipate this will be an issue, but we
    should keep this in mind if we notice an Organizations stream is stuck.
    """
    response_list_name = "organizations"


class OrganizationMemberships(SourceZendeskSupportCursorPaginationStream):
    """OrganizationMemberships stream: https://developer.zendesk.com/api-reference/ticketing/organizations/organization_memberships/"""


class AuditLogs(SourceZendeskSupportCursorPaginationStream):
    """AuditLogs stream: https://developer.zendesk.com/api-reference/ticketing/account-configuration/audit_logs/#list-audit-logs
    
    This endpoint does not respect the start_time param. It requires two query params with the same name to filter by date.
    See https://support.zendesk.com/hc/en-us/community/posts/4859612547866-Audit-Log-API-error.
    """

    response_list_name: str = "audit_logs"
    # audit_logs doesn't have the 'updated_by' field
    cursor_field = "created_at"
    state_checkpoint_interval = 100

    def request_params(self, next_page_token: Mapping[str, Any] = None, stream_state: Mapping[str, Any] = None,**kwargs) -> MutableMapping[str, Any]:
        cursor_value = stream_state.get(self.cursor_field, None)
        start_time = cursor_value or self._start_date
        # The end_time is moved a little in the past to avoid missing records that share the same "created_at" since 
        # records with the current cursor value are ignored.
        end_time = (datetime.now(tz=UTC) - timedelta(seconds=30)).strftime(DATETIME_FORMAT)

        params = {
            "page[size]": self.page_size,
            # By default, results are returned in descending order.
            # "sort" is required to get results returned in ascending order.
            "sort": "created_at",
            # "filter[created_at][]" filters the responses' results to only records created within the specified timespan.
            "filter[created_at][]": [start_time, end_time]
        }

        if next_page_token:
            params.update(next_page_token)

        return params


class Users(SourceZendeskSupportIncrementalCursorExportStream):
    """Users stream: https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/#incremental-user-export-cursor-based"""

    response_list_name: str = "users"


class Tickets(SourceZendeskSupportIncrementalCursorExportStream):
    """Tickets stream: https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/#incremental-ticket-export-cursor-based"""

    response_list_name: str = "tickets"
    transformer: TypeTransformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)

    @staticmethod
    def check_start_time_param(requested_start_time: int, value: int = 1):
        """
        The stream returns 400 Bad Request StartTimeTooRecent when requesting tasks 1 second before now.
        Figured out during experiments that the most recent time needed for request to be successful is 3 seconds before now.
        """
        return SourceZendeskIncrementalExportStream.check_start_time_param(requested_start_time, value=3)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for record in super().parse_response(response, **kwargs):
            # Additional handling to coerce custom fields' boolean values to strings.
            custom_fields = record.get("custom_fields", [])

            for field in custom_fields:
                value = field.get("value", None)
                if isinstance(value, bool):
                    field["value"] = str(value).lower()

            yield record


class TicketComments(SourceZendeskSupportTicketEventsExportStream):
    """
    Fetch the TicketComments incrementaly from TicketEvents Export stream
    """

    list_entities_from_event = ["via_reference_id", "ticket_id", "timestamp"]
    sideload_param = "comment_events"
    event_type = "Comment"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for record in super().parse_response(response, **kwargs):
            # https://github.com/airbytehq/oncall/issues/1001
            if type(record.get("via")) is not dict:
                record["via"] = None
            yield record


class Groups(SourceZendeskSupportCursorPaginationStream):
    """Groups stream: https://developer.zendesk.com/api-reference/ticketing/groups/groups/"""

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        # Zendesk by default excludes deleted groups. To include deleted groups in the API response, we have to
        # use the exclude_deleted query param.
        params.update({"exclude_deleted": False})
        return params


class GroupMemberships(SourceZendeskSupportCursorPaginationStream):
    """GroupMemberships stream: https://developer.zendesk.com/api-reference/ticketing/groups/group_memberships/"""

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params.update({"sort_by": "asc"})
        return params


class SatisfactionRatings(SourceZendeskSupportCursorPaginationStream):
    """
    SatisfactionRatings stream: https://developer.zendesk.com/api-reference/ticketing/ticket-management/satisfaction_ratings/
    """

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            "start_time": self.check_stream_state(stream_state=stream_state),
            "page[size]": self.page_size,
        }
        if next_page_token:
            # need start time param for subsequent requests to this endpoint.
            params.update(next_page_token)
        return params


class TicketFields(SourceZendeskSupportStream):
    """TicketFields stream: https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_fields/"""


class TicketForms(SourceZendeskSupportCursorPaginationStream):
    """TicketForms stream: https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_forms"""

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        # This endpoint does not use query params, so we override the parent class' request_params method.
        return {}


class TicketMetrics(Tickets):
    """TicketMetrics stream: https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_metrics

    Ticket metrics are obtained by sideloading the Tickets stream. https://developer.zendesk.com/documentation/ticketing/using-the-zendesk-api/side_loading/#supported-endpoints
    """
    sideload_param: str = "metric_sets"
    response_target_entity: str = "metric_set"

    def request_params(
        self, stream_state: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, **kwargs
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, next_page_token=next_page_token, **kwargs)
        params.update({"include": self.sideload_param})
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for record in super().parse_response(response=response, **kwargs):
            metric_set = record.get(self.response_target_entity, None)
            # Deleted tickets have no metrics, so we have to check that the metric set exists before yielding it.
            if metric_set is not None:
                yield metric_set


class TicketMetricEvents(SourceZendeskSupportCursorPaginationStream):
    """
    TicketMetricEvents stream: https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_metric_events/
    """

    state_checkpoint_interval = 1000
    cursor_field = "time"
    page_size = 1000

    def path(self, **kwargs):
        return "incremental/ticket_metric_events"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            "start_time": self.check_stream_state(stream_state),
            "page[size]": self.page_size,
        }
        if next_page_token:  
            # need start time param for subsequent requests to this endpoint.
            params.update(next_page_token)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        last_date = None
        for record in super().parse_response(response=response, **kwargs):
            yield record
            last_date = record.get(self.cursor_field)

        if last_date:
            logger.info(f"Read TicketMetricEvents documents up to {last_date}.")


class Macros(SourceZendeskSupportStream):
    """Macros stream: https://developer.zendesk.com/api-reference/ticketing/business-rules/macros/"""


class TicketAudits(SourceZendeskSupportFullRefreshStream, IncrementalMixin):
    response_list_name = "audits"

    _cursor_value = ""

    @property
    def cursor_field(self) -> str:
        """Name of the field associated with the state"""
        return "after_cursor"

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.parent = Tickets(
            is_parent= True,
            **kwargs
        )

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: Optional[List[str]] = None, stream_state: Optional[Mapping[str, Any]] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        latest_cursor = self.state.get(self.cursor_field, None)

        self.parent.state = {self.cursor_field: latest_cursor}

        parent_records = self.parent.read_records(
            sync_mode=SyncMode.incremental, cursor_field=cursor_field
        )

        for record in parent_records:
            next_cursor = record.get('cursor', None)
            if next_cursor:
                # Update state if we receive a cursor
                self.state = {self.cursor_field: next_cursor}
            else:
                yield {"parent": record}

    def path(
        self,
        *,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        ticket_id = stream_slice.get("parent").get("id")
        return f"tickets/{ticket_id}/audits"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for record in response.json().get(self.response_list_name, []):
            yield record


class Tags(SourceZendeskSupportFullRefreshStream):
    """Tags stream: https://developer.zendesk.com/api-reference/ticketing/ticket-management/tags/"""

    # doesn't have the 'id' field
    primary_key = "name"


class SlaPolicies(SourceZendeskSupportFullRefreshStream):
    """SlaPolicies stream: https://developer.zendesk.com/api-reference/ticketing/business-rules/sla_policies/"""

    def path(self, *args, **kwargs) -> str:
        return "slas/policies.json"

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        # This endpoint does not use query params, so we override the parent class' request_params method.
        return {}


class Brands(SourceZendeskSupportFullRefreshStream):
    """Brands stream: https://developer.zendesk.com/api-reference/ticketing/account-configuration/brands/#list-brands"""


class CustomRoles(SourceZendeskSupportFullRefreshStream):
    """CustomRoles stream: https://developer.zendesk.com/api-reference/ticketing/account-configuration/custom_roles/#list-custom-roles"""

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        # This endpoint does not use query params, so we override the parent class' request_params method.
        return {}

class Schedules(SourceZendeskSupportFullRefreshStream):
    """Schedules stream: https://developer.zendesk.com/api-reference/ticketing/ticket-management/schedules/#list-schedules"""

    def path(self, *args, **kwargs) -> str:
        return "business_hours/schedules.json"

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        # This endpoint does not use query params, so we override the parent class' request_params method.
        return {}


class UserSettingsStream(SourceZendeskSupportFullRefreshStream):
    """Stream for checking of a request token and permissions"""

    def path(self, *args, **kwargs) -> str:
        return "account/settings.json"

    def next_page_token(self, *args, **kwargs) -> Optional[Mapping[str, Any]]:
        # this data without listing
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """returns data from API"""
        settings = response.json().get("settings")
        if settings:
            yield settings

    def get_settings(self) -> Mapping[str, Any]:
        for resp in self.read_records(SyncMode.full_refresh):
            return resp
        raise SourceZendeskException("not found settings")

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        # This endpoint does not use query params, so we override the parent class' request_params method.
        return {}


class AccountAttributes(SourceZendeskSupportFullRefreshStream):
    """Account attributes: https://developer.zendesk.com/api-reference/ticketing/ticket-management/skill_based_routing/#list-account-attributes"""

    response_list_name = "attributes"

    def path(self, *args, **kwargs) -> str:
        return "routing/attributes"
    
    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        # This endpoint does not use query params, so we override the parent class' request_params method.
        return {}


class AttributeDefinitions(SourceZendeskSupportFullRefreshStream):
    """Attribute definitions: https://developer.zendesk.com/api-reference/ticketing/ticket-management/skill_based_routing/#list-routing-attribute-definitions"""

    primary_key = "subject"

    def path(self, *args, **kwargs) -> str:
        return "routing/attributes/definitions"

    def parse_response(self, response: requests.Response, stream_state: Mapping[str, Any], **kwargs) -> Iterable[Mapping]:
        # If there is no content in the response (i.e. not an enterprise Zendesk account), return early.
        if not response.content:
            return {}

        defs_all = response.json()["definitions"]["conditions_all"]
        for d in defs_all:
            d["condition"] = "all"
            yield d

        defs_any = response.json()["definitions"]["conditions_any"]
        for d in defs_any:
            d["condition"] = "any"
            yield d

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        # This endpoint does not use query params, so we override the parent class' request_params method.
        return {}


class TicketSkips(SourceZendeskSupportCursorPaginationStream):
    """Ticket skips: https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_skips/"""

    response_list_name = "skips"

    def path(self, **kwargs):
        return "skips.json"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if self._ignore_pagination:
            return None
        meta = response.json().get("meta", {})
        return meta.get("after_cursor") if meta.get("has_more", False) else None

    def request_params(self, stream_state: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = {
            "start_time": self.check_stream_state(stream_state),
            "page[size]": self.page_size,
        }

        if next_page_token:
            params.pop("start_time", None)
            params["page[after]"] = next_page_token

        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for record in super().parse_response(response, **kwargs):
            # Additional handling to coerce custom fields' boolean values to strings.
            custom_fields = record.get("ticket", {}).get("custom_fields", [])

            for field in custom_fields:
                value = field.get("value", None)
                if isinstance(value, bool):
                    field["value"] = str(value).lower()

            yield record


class Posts(SourceZendeskSupportCursorPaginationStream):
    """Posts: https://developer.zendesk.com/api-reference/help_center/help-center-api/posts/#list-posts"""

    # Turn on caching to improve performance for streams that have Posts as their parent. 
    # See https://github.com/airbytehq/airbyte/blob/master/docs/connector-development/cdk-python/http-streams.md#nested-streams--caching
    use_cache = True

    cursor_field = "updated_at"

    def path(self, **kwargs):
        return "community/posts"


class PostComments(SourceZendeskSupportFullRefreshStream, HttpSubStream):
    """Post comments: https://developer.zendesk.com/api-reference/help_center/help-center-api/post_comments/"""

    response_list_name = "comments"

    def __init__(self, **kwargs):
        parent = Posts(**kwargs)
        super().__init__(parent=parent, **kwargs)

    def path(
            self, 
            *, 
            stream_state: Mapping[str, Any] = None, 
            stream_slice: Mapping[str, Any] = None, 
            next_page_token: Mapping[str, Any] = None,
        ) -> str:
            post_id = stream_slice.get("parent").get("id")
            return f"community/posts/{post_id}/comments"


class PostVotes(SourceZendeskSupportFullRefreshStream, HttpSubStream):
    """Post votes: https://developer.zendesk.com/api-reference/help_center/help-center-api/votes/"""

    response_list_name = "votes"

    def __init__(self, **kwargs):
        parent = Posts(**kwargs)
        super().__init__(parent=parent, **kwargs)
    
    def path(
        self,
        *,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        post_id = stream_slice.get("parent").get("id")
        return f"community/posts/{post_id}/votes"


class PostCommentVotes(SourceZendeskSupportFullRefreshStream, HttpSubStream):
    """Post comment votes: https://developer.zendesk.com/api-reference/help_center/help-center-api/votes/"""

    response_list_name = "votes"

    def __init__(self, **kwargs):
        parent = PostComments(**kwargs)
        super().__init__(parent=parent, **kwargs)

    def path(
        self,
        *,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        post_id = stream_slice.get("parent").get("post_id")
        comment_id = stream_slice.get("parent").get("id")
        return f"community/posts/{post_id}/comments/{comment_id}/votes"
