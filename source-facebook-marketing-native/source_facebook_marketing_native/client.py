import base64
import json
import re
from dataclasses import dataclass
from enum import Enum
from logging import Logger
from typing import Any, AsyncGenerator, Generic, List, NamedTuple, TypeVar

from estuary_cdk.http import HTTPSession
from pydantic import (
    BaseModel,
    ConfigDict,
    RootModel,
    TypeAdapter,
    field_serializer,
    ValidationError,
)

from .models import FacebookResource, FacebookError
from .permissions import PermissionManager
from .fields import DELIVERY_INFO_FIELDS

BASE_URL = "https://graph.facebook.com/v23.0"
DEFAULT_PAGE_SIZE = 100
MAX_ASYNC_JOBS = 10
UNSUPPORTED_FIELDS = {
    "unique_conversions",
    "unique_ctr",
    "unique_clicks",
    "age_targeting",
    "gender_targeting",
    "labels",
    "location",
    "estimated_ad_recall_rate_lower_bound",
    "estimated_ad_recall_rate_upper_bound",
    "estimated_ad_recallers_lower_bound",
    "estimated_ad_recallers_upper_bound",
}

# Permission error codes defined by Facebook API
PERMISSION_ERROR_CODES = {100, 200, 10}


class FacebookPermissionErrorType(Enum):
    BUSINESS_MANAGEMENT = "business_management"
    FUNDING_SOURCE = "funding_source"
    PROMOTABLE_OBJECTS = "promotable_objects"
    UNSUPPORTED_REQUEST = "unsupported_request"


@dataclass(frozen=True)
class ErrorPattern:
    error_type: FacebookPermissionErrorType
    field_name: str
    patterns: tuple[re.Pattern, ...]

    def matches(self, message: str) -> bool:
        return any(pattern.search(message.lower()) for pattern in self.patterns)


ERROR_PATTERNS = (
    ErrorPattern(
        error_type=FacebookPermissionErrorType.BUSINESS_MANAGEMENT,
        field_name="owner",
        patterns=(
            re.compile(r"business_management permission.*manage.*object"),
            re.compile(r"\bowner\b.*permission"),
            re.compile(r"requires business_management permission"),
        ),
    ),
    ErrorPattern(
        error_type=FacebookPermissionErrorType.FUNDING_SOURCE,
        field_name="funding_source_details",
        patterns=(
            re.compile(r"funding_source_details.*permission"),
            re.compile(r"unsupported request.*method type.*get"),
        ),
    ),
    ErrorPattern(
        error_type=FacebookPermissionErrorType.PROMOTABLE_OBJECTS,
        field_name="ad_account_promotable_objects",
        patterns=(
            re.compile(r"promotable object.*permission"),
            re.compile(r"does not have.*permission.*promotable object"),
        ),
    ),
)


TFacebookResource = TypeVar("TFacebookResource", bound=FacebookResource)


class FacebookPaging(BaseModel, extra="allow"):
    class Cursors(BaseModel, extra="allow"):
        after: str | None = None

    cursors: Cursors | None = None


class FacebookPaginatedResponse(BaseModel, Generic[TFacebookResource]):
    data: list[TFacebookResource]
    paging: FacebookPaging | None = None
    error: FacebookError | None = None

    @property
    def next_cursor(self) -> str | None:
        if (
            self.paging is not None
            and self.paging.cursors is not None
            and self.paging.cursors.after is not None
        ):
            return self.paging.cursors.after

        return None


# This model handles non-paginated responses which return a single resource object
# as the root of the JSON response or an "error" object in case of an error.
class FacebookSingleResponse(RootModel[TFacebookResource | FacebookError]):
    @property
    def error(self) -> FacebookError | None:
        return self.root if isinstance(self.root, FacebookError) else None

    @property
    def resource(self) -> TFacebookResource | None:
        return self.root if not isinstance(self.root, FacebookError) else None


FacebookResourceType = (
    FacebookSingleResponse[TFacebookResource]
    | FacebookPaginatedResponse[TFacebookResource]
)
FacebookTypeAdapter = TypeAdapter[FacebookResourceType]


class FacebookAPIError(Exception):
    def __init__(self, error: FacebookError, status_code: int = 400):
        self.error = error
        self.status_code = status_code
        super().__init__(f"Facebook API Error ({error.code}): {error.message}")

    def is_permission_error(self) -> bool:
        if self.error.code in PERMISSION_ERROR_CODES:
            return True

        return any(pattern.matches(self.error.message) for pattern in ERROR_PATTERNS)


class AfterCursor(NamedTuple):
    cursor: str | None


class FacebookRequestParams(BaseModel):
    fields: list[str] = []
    limit: int = DEFAULT_PAGE_SIZE
    after_cursor: str | None = None
    since: int | None = None  # Used for Activities stream (Unix timestamp)
    filtering: list[dict] | None = None  # Used for standard incremental streams

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
    )

    @field_serializer("fields", mode="plain")
    def serialize_fields(self, fields: list[str]) -> str | None:
        request_fields = set(fields or [])
        if unsupported := request_fields.intersection(UNSUPPORTED_FIELDS):
            raise ValidationError(
                f"The following fields are unsupported and will possibly lead to errors on Facebook side: {unsupported}"
            )

        return ",".join(fields) if fields else None

    @field_serializer("after_cursor", mode="plain")
    def serialize_after_cursor(self, after_cursor: str | None) -> str | None:
        return after_cursor

    @field_serializer("since", mode="plain")
    def serialize_since(self, since: int | None) -> int | None:
        return since

    @field_serializer("filtering", mode="plain")
    def serialize_filtering(self, filtering: list[dict] | None) -> str | None:
        if not filtering:
            return None

        return json.dumps(filtering)

    def model_dump(self, **kwargs) -> dict:
        data = super().model_dump(**kwargs)
        # Map after_cursor to the correct Facebook API parameter name 'after'
        if "after_cursor" in data:
            data["after"] = data.pop("after_cursor")
        # Remove None values to avoid sending them to the API
        return {k: v for k, v in data.items() if v is not None}


# Although Facebook already has a Python client SDK, it is not asynchronous. We _could_ use asyncio.to_thread to
# run the synchronous methods in a thread, but we've seen that approach lead to performance issues in high-load scenarios.
# So we've implemented a minimal async client with only the functionality we need.
class FacebookAPIClient:
    def __init__(self, http: HTTPSession, log: Logger, include_deleted: bool):
        self.http = http
        self.base_url = BASE_URL
        self.log = log
        self._include_deleted = include_deleted
        self.permission_manager = PermissionManager(http, BASE_URL, log)

    async def _fetch_resource_data(
        self,
        resource_model: type[TFacebookResource],
        adapter: FacebookTypeAdapter,
        url: str,
        params: FacebookRequestParams,
    ) -> (
        FacebookSingleResponse[TFacebookResource]
        | FacebookPaginatedResponse[TFacebookResource]
    ):
        if self._include_deleted:
            if hasattr(resource_model, "entity_prefix"):
                filter = {
                    "field": f"{resource_model.entity_prefix}.delivery_info",
                    "operator": "IN",
                    "value": DELIVERY_INFO_FIELDS,
                }
                if params.filtering:
                    params.filtering.append(filter)
                else:
                    params.filtering = [filter]
            else:
                self.log.debug(
                    f"Resource {resource_model.__name__} does not have entity_prefix, skipping delivery_info filter"
                )

        request_params = params.model_dump(exclude_none=True)
        response = adapter.validate_json(
            await self.http.request(
                self.log,
                url,
                params=request_params,
            )
        )

        if response.error:
            raise FacebookAPIError(response.error)

        return response

    def _build_adapter(
        self,
        resource_model: type[TFacebookResource],
    ) -> TypeAdapter:
        return TypeAdapter(
            FacebookSingleResponse[resource_model]  # type: ignore
            | FacebookPaginatedResponse[resource_model]  # type: ignore
        )

    def _build_url(
        self,
        resource_model: type[TFacebookResource],
        account_id: str | None = None,
    ) -> str:
        url = f"{self.base_url}/{resource_model.endpoint}"
        if account_id:
            url = f"{self.base_url}/act_{account_id}/{resource_model.endpoint}"

        return url

    async def _prepare_fetch_request(
        self,
        resource_model: type[TFacebookResource],
        params: FacebookRequestParams,
        account_id: str | None = None,
    ) -> tuple[str, TypeAdapter, FacebookRequestParams]:
        if resource_model.requires_account_id and not account_id:
            raise ValueError(
                "Account ID is required for this resource model's API requests"
            )

        url = self._build_url(resource_model, account_id)
        adapter = self._build_adapter(resource_model)
        request_params = params.model_copy()

        if account_id:
            filtered_fields = await self.permission_manager.get_filtered_fields(
                resource_model, account_id
            )
            if filtered_fields != params.fields:
                self.log.info(
                    f"Using permission-filtered fields for {resource_model.name}: {len(filtered_fields)} fields"
                )
                request_params.fields = filtered_fields

        return url, adapter, request_params

    async def _fetch_with_error_handling(
        self,
        resource_model: type[TFacebookResource],
        adapter: TypeAdapter,
        url: str,
        request_params: FacebookRequestParams,
        account_id: str | None = None,
    ) -> (
        FacebookSingleResponse[TFacebookResource]
        | FacebookPaginatedResponse[TFacebookResource]
    ):
        try:
            return await self._fetch_resource_data(
                resource_model, adapter, url, request_params
            )
        except FacebookAPIError as e:
            if e.is_permission_error() and account_id:
                return await self._handle_permission_error(
                    e,
                    resource_model,
                    adapter,
                    url,
                    request_params,
                )
            else:
                raise

    async def fetch_all(
        self,
        resource_model: type[TFacebookResource],
        params: FacebookRequestParams,
        account_id: str | None = None,
    ) -> AsyncGenerator[FacebookResource, None]:
        url, adapter, request_params = await self._prepare_fetch_request(
            resource_model, params, account_id
        )

        while True:
            response: (
                FacebookSingleResponse[TFacebookResource]
                | FacebookPaginatedResponse[TFacebookResource]
            ) = await self._fetch_with_error_handling(
                resource_model, adapter, url, request_params, account_id
            )

            self.log.debug(f"GOT DATA: {response}", {"type": str(type(response))})

            if isinstance(response, FacebookSingleResponse):
                if response.resource is not None:
                    self.log.debug(
                        f"Yielding single resource from response.resource: {response.resource}"
                    )
                    yield response.resource
                break

            self.log.debug(
                f"Got {len(response.data)} items in paginated response, next_cursor: {response.next_cursor}"
            )
            for item in response.data:
                yield item

            if not response.next_cursor:
                self.log.debug("No next_cursor found, ending pagination")
                break

            self.log.debug(f"Setting after_cursor to: {response.next_cursor}")
            request_params.after_cursor = response.next_cursor

    async def fetch_page(
        self,
        resource_model: type[TFacebookResource],
        params: FacebookRequestParams,
        account_id: str | None = None,
    ) -> AsyncGenerator[FacebookResource | AfterCursor, None]:
        url, adapter, request_params = await self._prepare_fetch_request(
            resource_model, params, account_id
        )

        response: (
            FacebookSingleResponse[TFacebookResource]
            | FacebookPaginatedResponse[TFacebookResource]
        ) = await self._fetch_with_error_handling(
            resource_model, adapter, url, request_params, account_id
        )

        match response:
            case FacebookPaginatedResponse():
                for item in response.data:
                    yield item
                yield AfterCursor(response.next_cursor)
            case FacebookSingleResponse():
                if response.resource is not None:
                    yield response.resource
            case _:
                try:
                    raw_json = json.dumps(response, default=str)
                except Exception:
                    raw_json = repr(response)

                self.log.error(
                    "Unknown Facebook API response type detected",
                    extra={"type": str(type(response)), "content": raw_json},
                )
                raise ValueError(
                    f"Unexpected response type: {type(response)}; content: {raw_json}"
                )

    async def fetch_thumbnail_data_url(self, url: str) -> str | None:
        try:
            response_bytes = await self.http.request(self.log, url)
            content_type = "image/jpeg"  # Default fallback
            data = base64.b64encode(response_bytes)
            return f"data:{content_type};base64,{data.decode('ascii')}"
        except Exception as exc:
            self.log.warning(
                f"Got {str(exc)} while requesting thumbnail image from {url}"
            )
            return None

    async def get_insights_async_job(
        self,
        log: Logger,
        account_id: str,
        level: str,
        fields: List[str],
        time_range: dict[str, str],
        breakdowns: List[str] | None = None,
        action_breakdowns: List[str] | None = None,
        time_increment: int = 1,
    ) -> str:
        url = f"{self.base_url}/act_{account_id}/insights"

        params = {
            "level": level,
            "fields": ",".join(fields),
            "time_range": json.dumps(time_range),
            "time_increment": time_increment,
            "limit": DEFAULT_PAGE_SIZE,
        }

        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)

        if action_breakdowns:
            params["action_breakdowns"] = ",".join(action_breakdowns)

        log.debug(f"Submitting async insights job with params: {params}")

        response_bytes = await self.http.request(
            log,
            url,
            method="POST",
            params=params,
        )

        response_data = json.loads(response_bytes)

        if "error" in response_data:
            error_data = response_data["error"]
            raise FacebookAPIError(
                error=FacebookError(
                    message=error_data.get("message", "Unknown error"),
                    type=error_data.get("type", "UnknownError"),
                    code=error_data.get("code", 0),
                    error_subcode=error_data.get("error_subcode"),
                    fbtrace_id=error_data.get("fbtrace_id"),
                )
            )

        report_run_id = response_data.get("report_run_id")
        if not report_run_id:
            raise ValueError(f"No report_run_id in response: {response_data}")

        log.info(f"Submitted async insights job: {report_run_id}")
        return report_run_id

    async def get_insights_job_status(
        self, log: Logger, job_id: str
    ) -> dict[str, Any]:
        url = f"{self.base_url}/{job_id}"

        log.debug(f"Checking status for job: {job_id}")

        response_bytes = await self.http.request(log, url)
        response_data = json.loads(response_bytes)

        if "error" in response_data:
            error_data = response_data["error"]
            raise FacebookAPIError(
                error=FacebookError(
                    message=error_data.get("message", "Unknown error"),
                    type=error_data.get("type", "UnknownError"),
                    code=error_data.get("code", 0),
                    error_subcode=error_data.get("error_subcode"),
                    fbtrace_id=error_data.get("fbtrace_id"),
                )
            )

        return response_data

    async def get_insights_job_result(
        self, log: Logger, job_id: str
    ) -> AsyncGenerator[dict[str, Any], None]:
        url = f"{self.base_url}/{job_id}/insights"
        params = {"limit": DEFAULT_PAGE_SIZE}

        log.debug(f"Fetching results for job: {job_id}")

        while True:
            response_bytes = await self.http.request(log, url, params=params)
            response_data = json.loads(response_bytes)

            if "error" in response_data:
                error_data = response_data["error"]
                raise FacebookAPIError(
                    error=FacebookError(
                        message=error_data.get("message", "Unknown error"),
                        type=error_data.get("type", "UnknownError"),
                        code=error_data.get("code", 0),
                        error_subcode=error_data.get("error_subcode"),
                        fbtrace_id=error_data.get("fbtrace_id"),
                    )
                )

            data = response_data.get("data", [])
            log.debug(f"Got {len(data)} results from job {job_id}")

            for item in data:
                yield item

            # Check for pagination
            paging = response_data.get("paging", {})
            next_url = paging.get("next")

            if not next_url:
                log.debug(f"No more pages for job {job_id}")
                break

            # Use the next URL directly
            url = next_url
            params = {}  # Next URL already has params

    @staticmethod
    def _identify_problematic_field(error_message: str) -> str | None:
        for pattern in ERROR_PATTERNS:
            if pattern.matches(error_message):
                return pattern.field_name
        return None

    async def _handle_permission_error(
        self,
        error: FacebookAPIError,
        resource_model: type[TFacebookResource],
        adapter: TypeAdapter,
        url: str,
        request_params: FacebookRequestParams,
    ):
        field_to_remove = self._identify_problematic_field(error.error.message)

        match field_to_remove:
            case str() if field_to_remove in request_params.fields:
                self.log.warning(
                    f"Removing field '{field_to_remove}' due to permission error: {error.error.message}"
                )

                updated_fields = [
                    f for f in request_params.fields if f != field_to_remove
                ]
                request_params.fields = updated_fields

                return await self._fetch_resource_data(
                    resource_model, adapter, url, request_params
                )

            case str():
                self.log.debug(
                    f"Identified problematic field '{field_to_remove}' but it's not in current request fields"
                )
                raise error

            case None:
                self.log.warning(
                    f"Could not identify problematic field from error message: {error.error.message}"
                )
                raise error
