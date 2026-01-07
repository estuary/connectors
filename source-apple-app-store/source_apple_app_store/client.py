from datetime import date
from logging import Logger
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from estuary_cdk.http import HTTPError, HTTPMixin
from estuary_cdk.incremental_csv_processor import (
    CSVConfig,
    IncrementalCSVProcessor,
)
from estuary_cdk.gunzip_stream import GunzipStream
from pydantic import BaseModel

from .auth import AppleJWTTokenSource
from .models import (
    AnalyticsReport,
    AnalyticsReportAccessType,
    AnalyticsReportGranularity,
    AnalyticsReportInstance,
    AnalyticsReportRequest,
    AnalyticsReportSegment,
    ApiResponse,
    AppleAnalyticsRow,
    AppleCredentials,
    AppReview,
    AppleResourceValidationContext,
)

API_BASE = "https://api.appstoreconnect.apple.com/v1"
MAX_PAGE_SIZE = 200
CREATED_AT_DESC = "-createdDate"
CSV_CONFIG = CSVConfig(
    delimiter="\t",
    quotechar='"',
    lineterminator="\n",
    encoding="utf-8",
)

TPaginatedResponse = TypeVar(name="TPaginatedResponse")


class App(BaseModel, extra="allow"):
    id: str


class AppleAppStoreClient:
    """
    Apple App Store Connect API client.

    Handles all technical details of interacting with Apple's API including:
    - JWT authentication via custom token source
    - HTTP request/response handling
    - Error parsing and handling
    - Data extraction and processing
    - Pagination handling

    This client is independent logic and can be used standalone.
    """

    def __init__(
        self,
        log: Logger,
        credentials: AppleCredentials,
        http_mixin: HTTPMixin,
    ):
        self.log = log
        self.http = http_mixin
        self.http.token_source = AppleJWTTokenSource(credentials)

    async def _paginated_request(
        self,
        url: str,
        response_type: Type[TPaginatedResponse],
        initial_params: Optional[Dict[str, Any]] = None,
    ) -> List[TPaginatedResponse]:
        params = initial_params or {}
        results = []

        while True:
            response = ApiResponse[List[response_type]].model_validate_json(
                await self.http.request(self.log, url, params=params)
            )

            results.extend(response.data)

            cursor = response.cursor
            if cursor:
                params["cursor"] = cursor
            else:
                break

        return results

    async def list_apps(self) -> List[str]:
        url = f"{API_BASE}/apps"
        params = {"limit": 200}

        apps = await self._paginated_request(url, App, params)
        return [app.id for app in apps]

    async def create_analytics_report_request(
        self,
        app_id: str,
        report_type: AnalyticsReportAccessType,
    ) -> AnalyticsReportRequest:
        url = f"{API_BASE}/analyticsReportRequests"

        body = {
            "data": {
                "type": "analyticsReportRequests",
                "attributes": {
                    "accessType": report_type.value,
                },
                "relationships": {
                    "app": {
                        "data": {
                            "type": "apps",
                            "id": app_id,
                        }
                    }
                },
            }
        }

        response = ApiResponse[AnalyticsReportRequest].model_validate_json(
            await self.http.request(
                self.log,
                url,
                method="POST",
                json=body,
            )
        )

        return response.data

    async def list_analytics_report_requests(
        self,
        app_id: str,
        access_type: AnalyticsReportAccessType,
        limit: int = MAX_PAGE_SIZE,
    ) -> List[AnalyticsReportRequest]:
        assert limit > 0 and limit <= MAX_PAGE_SIZE, (
            f"Limit must be between 1 and {MAX_PAGE_SIZE}"
        )

        url = f"{API_BASE}/apps/{app_id}/analyticsReportRequests"
        params = {
            "fields[analyticsReportRequests]": "accessType,stoppedDueToInactivity",
            "filter[accessType]": access_type.value,
            "limit": limit,
        }

        return await self._paginated_request(
            url, AnalyticsReportRequest, params
        )

    async def get_analytics_report_request_details(
        self, request_id: str
    ) -> AnalyticsReportRequest:
        url = f"{API_BASE}/analyticsReportRequests/{request_id}"

        response = ApiResponse[AnalyticsReportRequest].model_validate_json(
            await self.http.request(
                self.log,
                url,
            )
        )

        return response.data

    async def list_analytics_reports(
        self,
        request_id: str,
        limit: int = MAX_PAGE_SIZE,
    ) -> List[AnalyticsReport]:
        assert limit > 0 and limit <= MAX_PAGE_SIZE, (
            f"Limit must be between 1 and {MAX_PAGE_SIZE}"
        )

        url = f"{API_BASE}/analyticsReportRequests/{request_id}/reports"
        params = {"limit": limit}

        return await self._paginated_request(url, AnalyticsReport, params)

    async def get_analytics_report_details(
        self, report_id: str
    ) -> AnalyticsReport:
        url = f"{API_BASE}/analyticsReports/{report_id}"

        response = ApiResponse[AnalyticsReport].model_validate_json(
            await self.http.request(
                self.log,
                url,
            )
        )

        return response.data

    async def list_analytic_report_instances(
        self,
        report_id: str,
        granularity: AnalyticsReportGranularity,
        processing_date: Optional[date] = None,
        limit: int = MAX_PAGE_SIZE,
    ) -> List[AnalyticsReportInstance]:
        assert limit > 0 and limit <= MAX_PAGE_SIZE, (
            f"Limit must be between 1 and {MAX_PAGE_SIZE}"
        )

        url = f"{API_BASE}/analyticsReports/{report_id}/instances"
        params = {
            "fields[analyticsReportInstances]": "granularity,processingDate,segments",
            "limit": limit,
        }

        if processing_date:
            params["filter[processingDate]"] = processing_date.isoformat()
        if granularity:
            params["filter[granularity]"] = granularity.value

        return await self._paginated_request(
            url, AnalyticsReportInstance, params
        )

    async def list_analytics_report_segments(
        self,
        instance_id: str,
        limit: int = MAX_PAGE_SIZE,
    ) -> List[AnalyticsReportSegment]:
        url = f"{API_BASE}/analyticsReportInstances/{instance_id}/segments"
        params = {
            "fields[analyticsReportSegments]": "checksum,sizeInBytes,url",
            "limit": limit,
        }

        return await self._paginated_request(
            url, AnalyticsReportSegment, params
        )

    async def get_analytics_report_segment_details(
        self,
        segment_id: str,
    ) -> AnalyticsReportSegment:
        url = f"{API_BASE}/analyticsReportSegments/{segment_id}"

        response = ApiResponse[AnalyticsReportSegment].model_validate_json(
            await self.http.request(
                self.log,
                url,
            )
        )

        return response.data

    async def get_or_create_report_request(
        self,
        app_id: str,
        report_type: AnalyticsReportAccessType,
    ) -> str:
        try:
            created_report_request = await self.create_analytics_report_request(
                app_id,
                report_type,
            )
            self.log.debug(f"Created report request {created_report_request.id} for app {app_id}")
            return created_report_request.id
        except HTTPError as e:
            if e.code == 409:
                report_requests = await self.list_analytics_report_requests(
                    app_id,
                    report_type,
                )
                for request in report_requests:
                    self.log.debug(f"Checking request {request.id} for app {app_id}")
                    # assuming there is only one request
                    return request.id
            raise

    async def stream_tsv_data(
        self,
        filename: str,
        download_url: str,
        model: type[AppleAnalyticsRow],
    ) -> AsyncGenerator[AppleAnalyticsRow, None]:
        """
        Stream and process TSV data from Apple analytics report.

        Yields:
            - AppleAnalyticsRow objects for each data row
            - bytes object as remainder for pagination (if applicable)
        """
        _, body_generator = await self.http.request_stream(
            self.log,
            download_url,
            with_token=False, # Apple's API returns signed/authenticated URLs; using Authorization header causes HTTP 400 errors
        )

        async def uncompressed_body() -> AsyncGenerator[bytes, None]:
            async for chunk in GunzipStream(body_generator()):
                if not chunk:
                    continue
                yield chunk

        validation_context = model.validation_context_model(filename=filename)

        processor = IncrementalCSVProcessor(
            uncompressed_body(),
            model,
            config=CSV_CONFIG,
            validation_context=validation_context,
        )

        async for item in processor:
            yield item

    async def list_app_reviews(
        self,
        app_id: str,
        cursor: Optional[str] = None,
        limit: int = MAX_PAGE_SIZE,
        sort: str = CREATED_AT_DESC,
    ) -> Tuple[List[AppReview], Optional[str]]:
        assert limit > 0 and limit <= MAX_PAGE_SIZE, (
            f"Limit must be between 1 and {MAX_PAGE_SIZE}"
        )

        url = f"{API_BASE}/apps/{app_id}/customerReviews"

        params = {
            "limit": limit,
            "include": "response",
            "sort": sort,
        }

        if cursor:
            params["cursor"] = cursor

        validation_context = AppleResourceValidationContext(app_id=app_id)

        response = ApiResponse[List[AppReview]].model_validate_json(
            await self.http.request(
                self.log,
                url,
                params=params,
            ),
            context=validation_context,
        )

        next_cursor = None
        next_link = None
        if response.links and response.links.next:
            next_link = response.links.next
        if next_link and "cursor=" in next_link:
            next_cursor = next_link.split("cursor=")[1].split("&")[0]

        return response.data, next_cursor
