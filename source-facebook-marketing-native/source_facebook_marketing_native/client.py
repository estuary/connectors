import base64
import json
import re
from dataclasses import dataclass
from enum import Enum
from logging import Logger
from typing import AsyncGenerator, Generic, TypeVar

from estuary_cdk.http import HTTPSession, Headers
from pydantic import (
    BaseModel,
    ConfigDict,
    RootModel,
    TypeAdapter,
    field_serializer,
    ValidationError,
)

from .constants import BASE_URL
from .models import FacebookResource
from .permissions import PermissionManager
from .fields import DELIVERY_INFO_FIELDS
from .enums import Field


MAX_500_ERROR_RETRY_ATTEMPTS = 5
DEFAULT_PAGE_SIZE = 100
UNSUPPORTED_FIELDS = {
    Field.UNIQUE_CONVERSIONS,
    Field.UNIQUE_CTR,
    Field.UNIQUE_CLICKS,
    Field.AGE_TARGETING,
    Field.GENDER_TARGETING,
    Field.LABELS,
    Field.LOCATION,
    Field.ESTIMATED_AD_RECALL_RATE_LOWER_BOUND,
    Field.ESTIMATED_AD_RECALL_RATE_UPPER_BOUND,
    Field.ESTIMATED_AD_RECALLERS_LOWER_BOUND,
    Field.ESTIMATED_AD_RECALLERS_UPPER_BOUND,
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


class FacebookError(BaseModel):
    message: str
    type: str
    code: int
    error_subcode: int | None = None
    fbtrace_id: str | None = None


class FacebookPaging(BaseModel):
    """Facebook pagination information."""

    class Cursors(BaseModel):
        after: str | None = None

        model_config = ConfigDict(extra="allow")

    cursors: Cursors | None = None
    next: str | None = None  # Full URL for next page

    model_config = ConfigDict(extra="allow")


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


# This model handles non-paginated responses. Facebook's API will return a single resource object
# as the root of the JSON response or an "error" object in case of an error.
class FacebookSingleResponse(RootModel[TFacebookResource | FacebookError]):
    @property
    def error(self) -> FacebookError | None:
        return self.root if isinstance(self.root, FacebookError) else None

    @property
    def resource(self) -> TFacebookResource | None:
        return self.root if not isinstance(self.root, FacebookError) else None


FacebookResourceType = (
    FacebookSingleResponse[FacebookResource]
    | FacebookPaginatedResponse[FacebookResource]
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


class FacebookCursor(BaseModel):
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

    @field_serializer("filtering", mode="plain")
    def serialize_filtering(self, filtering: list[dict] | None) -> str | None:
        if not filtering:
            return None

        return json.dumps(filtering)

    def model_dump(self, **kwargs) -> dict:
        data = super().model_dump(**kwargs)
        if "after_cursor" in data:
            # API request uses "after" instead of the easier to understand "after_cursor"
            data["after"] = data.pop("after_cursor")

        # Remove None values to avoid sending them to the API
        return {k: v for k, v in data.items() if v is not None}


# Although Facebook already has a Python client SDK, it is not asynchronous. We could use asyncio.to_thread to
# run the synchronous methods in a thread, but we've seen that approach lead to performance issues in high-load scenarios.
# So we've implemented a minimal async client with only the functionality we need.
class FacebookAPIClient:
    def __init__(
        self,
        http: HTTPSession,
        log: Logger,
    ):
        self.http = http
        self.base_url = BASE_URL
        self.log = log
        self.permission_manager = PermissionManager(http, BASE_URL, log)

    async def _fetch_resource_data(
        self,
        resource_model: type[TFacebookResource],
        adapter: FacebookTypeAdapter,
        url: str,
        params: FacebookRequestParams,
        include_deleted: bool,
    ) -> FacebookResourceType:
        if include_deleted and resource_model.enable_deleted_filter:
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
    ) -> FacebookTypeAdapter:
        return TypeAdapter(
            FacebookSingleResponse[resource_model]
            | FacebookPaginatedResponse[resource_model]
        )

    def _build_url(
        self,
        resource_model: type[TFacebookResource],
        account_id: str | None = None,
    ) -> str:
        url = f"{self.base_url}/{resource_model.endpoint}"
        if account_id:
            url = f"{self.base_url}/act_{account_id}/{resource_model.endpoint}"

        self.log.debug(f"Built URL for {resource_model.name}: {url}")

        return url

    async def _prepare_fetch_request(
        self,
        resource_model: type[TFacebookResource],
        params: FacebookRequestParams,
        account_id: str | None = None,
    ) -> tuple[str, FacebookTypeAdapter, FacebookRequestParams]:
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
        adapter: FacebookTypeAdapter,
        url: str,
        request_params: FacebookRequestParams,
        include_deleted: bool,
        account_id: str | None = None,
    ) -> FacebookResourceType:
        try:
            return await self._fetch_resource_data(
                resource_model, adapter, url, request_params, include_deleted
            )
        except FacebookAPIError as e:
            if e.is_permission_error() and account_id:
                return await self._handle_permission_error(
                    e,
                    resource_model,
                    adapter,
                    url,
                    request_params,
                    include_deleted,
                )
            else:
                raise

    async def fetch_all(
        self,
        resource_model: type[TFacebookResource],
        params: FacebookRequestParams,
        include_deleted: bool,
        account_id: str | None = None,
    ) -> AsyncGenerator[FacebookResource, None]:
        url, adapter, request_params = await self._prepare_fetch_request(
            resource_model, params, account_id
        )

        while True:
            response: FacebookResourceType = await self._fetch_with_error_handling(
                resource_model,
                adapter,
                url,
                request_params,
                include_deleted,
                account_id,
            )

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
        include_deleted: bool,
        account_id: str | None = None,
    ) -> AsyncGenerator[FacebookResource | FacebookCursor, None]:
        url, adapter, request_params = await self._prepare_fetch_request(
            resource_model, params, account_id
        )

        response: FacebookResourceType = await self._fetch_with_error_handling(
            resource_model, adapter, url, request_params, include_deleted, account_id
        )

        match response:
            case FacebookPaginatedResponse():
                for item in response.data:
                    yield item

                yield FacebookCursor(cursor=response.next_cursor)
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
            headers, body = await self.http.request_stream(
                self.log,
                url,
            )
            content_type = headers.get("Content-Type")
            if not content_type:
                raise KeyError("Content-Type header not found in response")

            response_bytes = b""
            async for chunk in body():
                response_bytes += chunk

            data = base64.b64encode(response_bytes)
            return f"data:{content_type};base64,{data.decode('ascii')}"
        except Exception as exc:
            self.log.warning(
                f"Got {str(exc)} while requesting thumbnail image from {url}"
            )

        return None

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
        include_deleted: bool,
    ):
        field_to_remove = self._identify_problematic_field(error.error.message)

        if field_to_remove is None:
            self.log.warning(
                f"Could not identify problematic field from error message: {error.error.message}"
            )
            raise error

        if field_to_remove not in request_params.fields:
            self.log.debug(
                f"Identified problematic field '{field_to_remove}' but it's not in current request fields"
            )
            raise error

        # Field identified and present in request - remove it and retry
        self.log.warning(
            f"Removing field '{field_to_remove}' due to permission error: {error.error.message}"
        )

        updated_fields = [f for f in request_params.fields if f != field_to_remove]
        request_params.fields = updated_fields

        return await self._fetch_resource_data(
            resource_model, adapter, url, request_params, include_deleted
        )
