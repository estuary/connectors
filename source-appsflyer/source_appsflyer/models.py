from abc import ABCMeta
from datetime import datetime, timedelta
from functools import reduce
from typing import Any, ClassVar, Self
from urllib.parse import urljoin

import xxhash
from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.capture.webhook.resources import WebhookDocument
from estuary_cdk.flow import AccessToken
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    model_validator,
)

ConnectorState = GenericConnectorState[ResourceState]


class EndpointConfig(BaseModel):
    app_ids: str = Field(
        title="App IDs",
        description=(
            "Comma-delimited list of AppsFlyer App IDs. "
            "Provide the unified app ID, which may include a platform specific prefix (ex: com.example.app, roku-4321, id121244). "
            "See https://dev.appsflyer.com/hc/reference/raw_data_pull_api_tokenv2-overview#path-parameters for more details."
        ),
        json_schema_extra={"order": 0},
    )
    start_date: datetime = Field(
        title="Start Date",
        description="Start date for historical data replication (UTC)",
        json_schema_extra={"order": 1},
    )
    credentials: AccessToken = Field(
        title="Authentication",
        json_schema_extra={"discriminator": "credentials_title", "order": 2},
    )

    class Advanced(BaseModel):
        window_size: timedelta = Field(
            title="Window Size",
            description=(
                "Window size for incremental streams in ISO 8601 format. "
                "ex: P30D means 30 days, PT6H means 6 hours."
            ),
            default=timedelta(days=7),
            ge=timedelta(days=1),
            le=timedelta(days=90),
        )

    advanced: Advanced = Field(
        default_factory=Advanced,
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True, "order": 3},
    )

    def parsed_app_ids(self) -> list[str]:
        return [s.strip() for s in self.app_ids.split(",") if s.strip()]

    @model_validator(mode="after")
    def _require_app_ids(self) -> Self:
        if not self.parsed_app_ids():
            raise ValueError("app_ids must contain at least one non-empty value")
        return self


class EventTypesResponse(BaseModel):
    event_types: list[str]


class AppsFlyerWebhookDocument(WebhookDocument):
    class Meta(WebhookDocument.Meta):
        model_config = ConfigDict(  # pyright: ignore[reportUnannotatedClassAttribute]
            validate_assignment=True
        )
        estuary_id: str = Field(
            default="",
            description="The surrogate key generated from characteristic fields",
        )

    meta_: Meta = Field(  # pyright: ignore[reportIncompatibleVariableOverride]
        default_factory=lambda: AppsFlyerWebhookDocument.Meta(headers={}, reqPath=""),
        alias="_meta",
        description="Document metadata",
    )

    app_id: str
    appsflyer_id: str
    event_name: str
    event_time: str
    event_value: str
    api_version: str

    @model_validator(mode="after")
    def inject_estuary_id(self) -> Self:
        parts = [
            self.app_id,
            self.appsflyer_id,
            self.event_name,
            self.event_time,
            self.event_value,
            self.api_version,
        ]
        self.meta_.estuary_id = xxhash.xxh128("|".join(parts).encode()).hexdigest()

        return self


class PullApiDocument(BaseDocument, metaclass=ABCMeta):
    model_config = ConfigDict(  # pyright: ignore[reportUnannotatedClassAttribute]
        extra="allow",
    )

    resource_name: ClassVar[str]
    base_url: ClassVar[str]
    endpoint: ClassVar[str]
    requires_permission_check: ClassVar[bool] = False
    date_format: ClassVar[str]
    # Composite natural key for a row. Column names must match the CSV headers
    # returned by AppsFlyer exactly (including spaces, case, and parentheses).
    key_fields: ClassVar[tuple[str, ...]]

    class Meta(BaseDocument.Meta):
        model_config = ConfigDict(  # pyright: ignore[reportUnannotatedClassAttribute]
            validate_assignment=True
        )
        app_id: str = Field(
            description="AppsFlyer App ID this row was fetched for", default=""
        )

    meta_: Meta = Field(  # pyright: ignore[reportIncompatibleVariableOverride]
        default_factory=Meta,
        alias="_meta",
        description="Document metadata",
    )

    @model_validator(mode="before")
    @classmethod
    def _inject_app_id(cls, data: Any, info: ValidationInfo) -> Any:
        if not isinstance(data, dict):
            return data
        if info.context and "app_id" in info.context:
            meta = data.get("_meta") or {}
            meta["app_id"] = info.context["app_id"]
            data["_meta"] = meta
            # return {**data, "app_id": info.context["app_id"]}
        return data

    @classmethod
    def get_api_url(cls, app_id: str) -> str:
        return reduce(urljoin, [cls.base_url, app_id + "/", cls.endpoint])


class RawReportDocument(PullApiDocument, metaclass=ABCMeta):
    base_url: ClassVar[str] = "https://hq1.appsflyer.com/api/raw-data/export/app/"
    date_format: ClassVar[str] = "%Y-%m-%d %H:%M:%S"


class AggregateReportDocument(PullApiDocument, metaclass=ABCMeta):
    base_url: ClassVar[str] = "https://hq1.appsflyer.com/api/agg-data/export/app/"
    date_format: ClassVar[str] = "%Y-%m-%d"


# TODO: When we add support for the remaining reports below, we need to add
# `key_fields` class variables on them.

# --- Raw Report Documents ---


# Non-organic
class InstallsReport(RawReportDocument):
    resource_name: ClassVar[str] = "installs_raw_report"
    endpoint: ClassVar[str] = "installs_report/v5"


class InAppEventsReport(RawReportDocument):
    resource_name: ClassVar[str] = "in_app_events_raw_report"
    endpoint: ClassVar[str] = "in_app_events_report/v5"


class UninstallsReport(RawReportDocument):
    resource_name: ClassVar[str] = "uninstall_events_raw_report"
    endpoint: ClassVar[str] = "uninstall_events_report/v5"


class ReinstallsReport(RawReportDocument):
    resource_name: ClassVar[str] = "reinstalls_raw_report"
    endpoint: ClassVar[str] = "reinstalls/v5"


# Organic
class OrganicInstallsReport(RawReportDocument):
    resource_name: ClassVar[str] = "organic_installs_raw_report"
    endpoint: ClassVar[str] = "organic_installs_report/v5"


class OrganicInAppEventsReport(RawReportDocument):
    resource_name: ClassVar[str] = "organic_in_app_events_raw_report"
    endpoint: ClassVar[str] = "organic_in_app_events_report/v5"


class OrganicUninstallsReport(RawReportDocument):
    resource_name: ClassVar[str] = "organic_uninstalls_events_raw_report"
    endpoint: ClassVar[str] = "organic_uninstall_events_report/v5"


class OrganicReinstallsReport(RawReportDocument):
    resource_name: ClassVar[str] = "organic_reinstalls_raw_report"
    endpoint: ClassVar[str] = "reinstalls_organic/v5"


# Retargeting
class RetargetingConversionsReport(RawReportDocument):
    resource_name: ClassVar[str] = "retargeting_conversions_raw_report"
    endpoint: ClassVar[str] = "installs-retarget/v5"


class RetargetingInAppEventsReport(RawReportDocument):
    resource_name: ClassVar[str] = "retargeting_in_app_events_raw_report"
    endpoint: ClassVar[str] = "in-app-events-retarget/v5"


# Ad Revenue
class AdRevenueReport(RawReportDocument):
    resource_name: ClassVar[str] = "ad_revenue_raw_report"
    endpoint: ClassVar[str] = "ad_revenue_raw/v5"


class AdRevenueOrganicReport(RawReportDocument):
    resource_name: ClassVar[str] = "ad_revenue_organic_raw_report"
    endpoint: ClassVar[str] = "ad_revenue_organic_raw/v5"


class AdRevenueRetargetingReport(RawReportDocument):
    resource_name: ClassVar[str] = "ad_revenue_retargeting_raw_report"
    endpoint: ClassVar[str] = "ad-revenue-raw-retarget/v5"


# Protect360
class Protect360Document(RawReportDocument):
    requires_permission_check: ClassVar[bool] = True


class BlockedInstallsReport(Protect360Document):
    resource_name: ClassVar[str] = "blocked_installs_raw_report"
    endpoint: ClassVar[str] = "blocked_installs_report/v5"


class BlockedInstallsPostAttributionReport(Protect360Document):
    resource_name: ClassVar[str] = "blocked_installs_post_attribution_raw_report"
    endpoint: ClassVar[str] = "detection/v5"


class BlockedInAppEventsReport(Protect360Document):
    resource_name: ClassVar[str] = "blocked_in_app_events_raw_report"
    endpoint: ClassVar[str] = "blocked_in_app_events_report/v5"


class BlockedInAppEventsPostAttributionReport(Protect360Document):
    resource_name: ClassVar[str] = "blocked_in_app_events_post_attribution_raw_report"
    endpoint: ClassVar[str] = "fraud-post-inapps/v5"


class BlockedClicksReport(Protect360Document):
    resource_name: ClassVar[str] = "blocked_clicks_raw_report"
    endpoint: ClassVar[str] = "blocked_clicks_report/v5"


class BlockedInstallPostbacksReport(Protect360Document):
    resource_name: ClassVar[str] = "blocked_install_postbacks_raw_report"
    endpoint: ClassVar[str] = "blocked_install_postbacks/v5"


# Postbacks
class InstallPostbacksReport(RawReportDocument):
    resource_name: ClassVar[str] = "install_postbacks_raw_report"
    endpoint: ClassVar[str] = "postbacks/v5"


class InAppEventPostbacksReport(RawReportDocument):
    resource_name: ClassVar[str] = "in_app_event_postbacks_raw_report"
    endpoint: ClassVar[str] = "in-app-events-postbacks/v5"


class RetargetingInAppEventPostbacksReport(RawReportDocument):
    resource_name: ClassVar[str] = "retargeting_in_app_event_postbacks_raw_report"
    endpoint: ClassVar[str] = "retarget_in_app_events_postbacks/v5"


class RetargetingConversionPostbacksReport(RawReportDocument):
    resource_name: ClassVar[str] = "retargeting_conversion_postbacks_raw_report"
    endpoint: ClassVar[str] = "retarget_install_postbacks/v5"


# --- Aggregate Report Documents ---


class AggPartnersReport(AggregateReportDocument):
    resource_name: ClassVar[str] = "partners_aggregate_report"
    endpoint: ClassVar[str] = "partners_report/v5"


class AggPartnersDailyReport(AggregateReportDocument):
    resource_name: ClassVar[str] = "daily_partners_aggregate_report"
    endpoint: ClassVar[str] = "partners_by_date_report/v5"


class AggDailyReport(AggregateReportDocument):
    resource_name: ClassVar[str] = "daily_aggregate_report"
    endpoint: ClassVar[str] = "daily_report/v5"


class AggGeoReport(AggregateReportDocument):
    resource_name: ClassVar[str] = "geo_aggregate_report"
    endpoint: ClassVar[str] = "geo_report/v5"


class AggGeoDailyReport(AggregateReportDocument):
    resource_name: ClassVar[str] = "daily_geo_aggregate_report"
    endpoint: ClassVar[str] = "geo_by_date_report/v5"
    key_fields: ClassVar[tuple[str, ...]] = (
        "Date",
        "Country",
        "Media Source (pid)",
        "Campaign (c)",
    )

    date: str = Field(alias="Date")
    country: str = Field(alias="Country")
    media_source: str = Field(alias="Media Source (pid)")
    campaign: str = Field(alias="Campaign (c)")
