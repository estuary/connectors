from logging import Logger, getLogger
from datetime import timedelta, datetime, UTC
from dateutil.relativedelta import relativedelta
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    AsyncGenerator,
    Callable,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    AwareDatetime,
    PositiveInt,
    model_validator,
    field_validator,
    create_model,
)
from typing import Literal


from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceState,
    ConnectorState as GenericConnectorState,
    PageCursor,
    LogCursor,
)
from estuary_cdk.flow import (
    OAuth2Spec,
    BaseOAuth2Credentials,
    AccessToken,
    ValidationError,
)
from .fields import (
    COMMON_FIELD_TYPES,
    RESOURCE_FIELD_TYPES,
    NESTED_OBJECT_SCHEMAS,
    AD_INSIGHTS_VALID_FIELDS,
    AD_INSIGHTS_VALID_BREAKDOWNS,
    AD_INSIGHTS_VALID_ACTION_BREAKDOWNS,
)

logger: Logger = getLogger(__name__)
ConnectorState = GenericConnectorState[ResourceState]

# Facebook store metrics maximum of 37 months old. Any time range that
# older that 37 months from current date would result in 400 Bad request
# HTTP response.
# https://developers.facebook.com/docs/marketing-api/reference/ad-account/insights/#overview
DATA_RETENTION_PERIOD = 37


# Type mapping for Facebook API fields (similar to Sage Intacct's DATATYPE_MAP)
# Maps field names to Python types for proper schema generation and validation
# Based on ads_insights.json schema from Airbyte source-facebook-marketing
FACEBOOK_INSIGHTS_TYPE_MAP: dict[str, type] = {
    # String fields - IDs and names
    "account_currency": str,
    "account_id": str,
    "account_name": str,
    "ad_id": str,
    "ad_name": str,
    "adset_id": str,
    "adset_name": str,
    "attribution_setting": str,
    "buying_type": str,
    "campaign_id": str,
    "campaign_name": str,
    "conversion_rate_ranking": str,
    "created_time": str,  # Date string format
    "date_start": str,  # Date string format
    "date_stop": str,  # Date string format
    "engagement_rate_ranking": str,
    "objective": str,
    "optimization_goal": str,
    "quality_ranking": str,
    "updated_time": str,  # Date string format
    # Integer fields - counts
    "clicks": int,
    "impressions": int,
    "inline_link_clicks": int,
    "inline_post_engagement": int,
    "reach": int,
    "unique_clicks": int,
    "unique_inline_link_clicks": int,
    # Float/Number fields - metrics and rates
    "auction_bid": float,
    "auction_competitiveness": float,
    "auction_max_competitor_bid": float,
    "canvas_avg_view_percent": float,
    "canvas_avg_view_time": float,
    "cost_per_estimated_ad_recallers": float,
    "cost_per_inline_link_click": float,
    "cost_per_inline_post_engagement": float,
    "cost_per_unique_click": float,
    "cost_per_unique_inline_link_click": float,
    "cpc": float,
    "cpm": float,
    "cpp": float,
    "ctr": float,
    "estimated_ad_recall_rate": float,
    "estimated_ad_recallers": float,
    "frequency": float,
    "full_view_impressions": float,
    "full_view_reach": float,
    "inline_link_click_ctr": float,
    "instant_experience_clicks_to_open": float,
    "instant_experience_clicks_to_start": float,
    "qualifying_question_qualify_answer_rate": float,
    "social_spend": float,
    "spend": float,
    "unique_ctr": float,
    "unique_inline_link_click_ctr": float,
    "unique_link_clicks_ctr": float,
    # Complex array/object fields (ads_action_stats)
    "action_values": list,
    "actions": list,
    "ad_click_actions": list,
    "ad_impression_actions": list,
    "catalog_segment_actions": list,
    "catalog_segment_value": list,
    "catalog_segment_value_mobile_purchase_roas": list,
    "catalog_segment_value_omni_purchase_roas": list,
    "catalog_segment_value_website_purchase_roas": list,
    "conversion_values": list,
    "conversions": list,
    "converted_product_quantity": list,
    "converted_product_value": list,
    "cost_per_15_sec_video_view": list,
    "cost_per_2_sec_continuous_video_view": list,
    "cost_per_action_type": list,
    "cost_per_ad_click": list,
    "cost_per_conversion": list,
    "cost_per_outbound_click": list,
    "cost_per_thruplay": list,
    "cost_per_unique_action_type": list,
    "cost_per_unique_outbound_click": list,
    "instant_experience_outbound_clicks": list,
    "mobile_app_purchase_roas": list,
    "outbound_clicks": list,
    "outbound_clicks_ctr": list,
    "purchase_roas": list,
    "unique_actions": list,
    "unique_outbound_clicks": list,
    "unique_outbound_clicks_ctr": list,
    "video_15_sec_watched_actions": list,
    "video_30_sec_watched_actions": list,
    "video_avg_time_watched_actions": list,
    "video_continuous_2_sec_watched_actions": list,
    "video_p100_watched_actions": list,
    "video_p25_watched_actions": list,
    "video_p50_watched_actions": list,
    "video_p75_watched_actions": list,
    "video_p95_watched_actions": list,
    "video_play_actions": list,
    "video_play_curve_actions": list,  # ads_histogram_stats
    "video_play_retention_0_to_15s_actions": list,  # ads_histogram_stats
    "video_play_retention_20_to_60s_actions": list,  # ads_histogram_stats
    "video_play_retention_graph_actions": list,  # ads_histogram_stats
    "video_time_watched_actions": list,
    "website_ctr": list,
    "website_purchase_roas": list,
    # Breakdown dimensions (string fields) - for breakdowns parameter
    "age": str,
    "gender": str,
    "country": str,
    "region": str,
    "dma": str,
    "publisher_platform": str,
    "platform_position": str,
    "device_platform": str,
    "impression_device": str,
    "product_id": str,
    "hourly_stats_aggregated_by_advertiser_time_zone": str,
    "hourly_stats_aggregated_by_audience_time_zone": str,
    # Action breakdown dimensions
    "action_type": str,
    "action_target_id": str,
    "action_destination": str,
}


OAUTH2_SPEC = OAuth2Spec(
    provider="facebook",
    authUrlTemplate=(
        "https://www.facebook.com/v23.0/dialog/oauth"
        "?client_id={{client_id}}"
        "&redirect_uri={{redirect_uri}}"
        "&scope=ads_management,ads_read,read_insights,business_management"
        "&state={{state}}"
    ),
    accessTokenUrlTemplate=(
        "https://graph.facebook.com/v23.0/oauth/access_token"
        "?client_id={{client_id}}"
        "&client_secret={{client_secret}}"
        "&code={{code}}"
        "&redirect_uri={{redirect_uri}}"
    ),
    accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
    accessTokenBody=(
        "client_id={{{#urlencode}}}{{{ client_id }}}{{{/urlencode}}}"
        "&client_secret={{{#urlencode}}}{{{ client_secret }}}{{{/urlencode}}}"
        "&code={{{#urlencode}}}{{{ code }}}{{{/urlencode}}}"
        "&redirect_uri={{{#urlencode}}}{{{ redirect_uri }}}{{{/urlencode}}}"
        "&grant_type=authorization_code"
    ),
    accessTokenResponseMap={
        "access_token": "/access_token",
        "expires_in": "/expires_in",
    },
)

if TYPE_CHECKING:
    OAuth2Credentials = BaseOAuth2Credentials
    from .client import FacebookAPIClient
    from .job_manager import FacebookInsightsJobManager
else:
    OAuth2Credentials = BaseOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)


class FacebookError(BaseModel):
    """Facebook API error response model."""

    message: str
    type: str
    code: int
    error_subcode: int | None = None
    fbtrace_id: str | None = None


class FacebookInsightsJobStatus(BaseModel):
    """Model for Facebook async insights job status response.

    Represents the status of an async insights job submitted to Facebook.
    Used by the job manager to track job progress and completion.
    """

    id: str = Field(description="Job ID (report_run_id)")
    async_status: str = Field(
        description="Job status: 'Job Completed', 'Job Failed', 'Job Running', 'Job Skipped'"
    )
    async_percent_completion: int = Field(
        description="Progress percentage (0-100)", ge=0, le=100
    )


class CommonConfigMixin(BaseModel):
    start_date: AwareDatetime = Field(
        title="Start Date",
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data before this date will not be replicated.",
        examples=["2017-01-25T00:00:00Z"],
    )
    end_date: AwareDatetime | None = Field(
        title="End Date",
        description=(
            "The date until which you'd like to replicate data for this stream, in the format YYYY-MM-DDT00:00:00Z. "
            "All data generated between the start date and this end date will be replicated. "
            "Not setting this option will result in always syncing the latest data."
        ),
        default=None,
        examples=["2017-01-26T00:00:00Z"],
    )
    insights_lookback_window: PositiveInt | None = Field(
        title="Insights Lookback Window",
        description=(
            "The attribution window. Facebook freezes insight data 28 days after it was generated, "
            "which means that all data from the past 28 days may have changed since we last emitted it, "
            "so you can retrieve refreshed insights from the past by setting this parameter. "
            "If you set a custom lookback window value in Facebook account, please provide the same value here."
        ),
        default=28,
    )

    @field_validator("start_date")
    @classmethod
    def _validate_start_date(cls, start_date: datetime) -> datetime:
        now = datetime.now(tz=start_date.tzinfo)
        today = now.replace(microsecond=0, second=0, minute=0, hour=0)
        retention_date = today - relativedelta(months=DATA_RETENTION_PERIOD)

        if retention_date.day != today.day:
            # Month subtraction can be erroneous, for instance:
            # 2023-03-31 - 37 months = 2020-02-29 which is incorrect, should be 2020-03-01
            # that's why we're adjusting the date to the 1st day of the next month
            retention_date = retention_date.replace(day=1) + relativedelta(months=1)
        else:
            # Facebook does not use UTC for the insights API and instead uses the
            # user's timezone. To avoid timezone related issues when a user has a
            # positive timezone offset, we add a day to the retention date so we're
            # always within the retention period.
            retention_date = retention_date + timedelta(days=1)

        if start_date > now:
            message = f"The start date cannot be in the future. Set start date to today's date - {today}."
            logger.warning(message)
            return today
        elif start_date < retention_date:
            message = f"The start date cannot be beyond {DATA_RETENTION_PERIOD} months from the current date. Set start date to {retention_date}."
            logger.warning(message)
            return retention_date
        return start_date

    @model_validator(mode="after")
    def _validate_end_date(self) -> "CommonConfigMixin":
        if self.end_date:
            if self.end_date <= self.start_date:
                raise ValidationError(["The end date must be after the start date."])

        return self


class InsightsConfig(CommonConfigMixin):
    model_config = ConfigDict(
        extra="allow",
    )

    name: str = Field(
        title="Name",
        description="The name value of insight",
    )
    level: Literal["ad", "adset", "campaign"] = Field(
        title="Level",
        description="Chosen level for API",
        default="ad",
    )
    fields: list[str] = Field(
        title="Fields",
        description="A list of chosen fields for fields parameter",
        default=[],
    )
    breakdowns: list[str] = Field(
        title="Breakdowns",
        description="A list of chosen breakdowns for breakdowns",
        default=[],
    )
    action_breakdowns: list[str] = Field(
        title="Action Breakdowns",
        description="A list of chosen action_breakdowns for action_breakdowns",
        default=[],
    )
    time_increment: int = Field(
        title="Time Increment",
        description=(
            "Time window in days by which to aggregate statistics. The sync will be chunked into N day intervals, where N is the number of days you specified. "
            "For example, if you set this value to 7, then all statistics will be reported as 7-day aggregates by starting from the start_date. If the start and end dates are October 1st and October 30th, then the connector will output 5 records: 01 - 06, 07 - 13, 14 - 20, 21 - 27, and 28 - 30 (3 days only)."
        ),
        default=1,
        gt=0,
        lt=90,
    )

    @field_validator("fields")
    @classmethod
    def validate_fields(cls, v: list[str]) -> list[str]:
        """Validate that all specified fields are valid Facebook Insights API fields."""
        invalid = [f for f in v if f not in AD_INSIGHTS_VALID_FIELDS]
        if invalid:
            raise ValueError(
                f"Invalid fields: {', '.join(invalid)}. "
                f"See https://developers.facebook.com/docs/marketing-api/insights/parameters for valid fields."
            )
        return v

    @field_validator("breakdowns")
    @classmethod
    def validate_breakdowns(cls, v: list[str]) -> list[str]:
        invalid = [b for b in v if b not in AD_INSIGHTS_VALID_BREAKDOWNS]
        if invalid:
            raise ValueError(
                f"Invalid breakdowns: {', '.join(invalid)}. Must be one of: {', '.join(sorted(AD_INSIGHTS_VALID_BREAKDOWNS))}"
            )
        return v

    @field_validator("action_breakdowns")
    @classmethod
    def validate_action_breakdowns(cls, v: list[str]) -> list[str]:
        invalid = [ab for ab in v if ab not in AD_INSIGHTS_VALID_ACTION_BREAKDOWNS]
        if invalid:
            raise ValueError(
                f"Invalid action_breakdowns: {', '.join(invalid)}. Must be one of: {', '.join(sorted(AD_INSIGHTS_VALID_ACTION_BREAKDOWNS))}"
            )
        return v


class EndpointConfig(CommonConfigMixin):
    account_ids: str = Field(
        title="Account IDs",
        description="Comma-separated list of Facebook Ad Account IDs (e.g., 123456789,987654321)",
    )
    credentials: OAuth2Credentials | AccessToken = Field(
        title="Authentication",
        discriminator="credentials_title",
    )
    include_deleted: bool = Field(
        title="Include Deleted Records",
        description="Set to active if you want to include data from deleted Activities",
        default=False,
    )
    fetch_thumbnail_images: bool = Field(
        title="Fetch Thumbnail Images from Ad Creative",
        description="Set to active if you want to fetch the thumbnail_url and store the result in thumbnail_data_url for each Ad Creative.",
        default=False,
    )
    custom_insights: list[InsightsConfig] | None = Field(
        title="Custom Insights",
        description=(
            "A list which contains ad statistics entries, each entry must have a name and can contains fields, "
            'breakdowns or action_breakdowns. Click on "add" to fill this field.'
        ),
        default=[],
    )

    @property
    def accounts(self) -> list[str]:
        """Parse account IDs from comma-separated string."""
        return [
            account_id.strip()
            for account_id in self.account_ids.split(",")
            if account_id.strip()
        ]


class FacebookResource(BaseDocument, extra="allow"):
    name: ClassVar[str]
    primary_keys: ClassVar[list[str]] = ["/id"]
    endpoint: ClassVar[str]
    interval: ClassVar[timedelta]
    is_paginated: ClassVar[bool] = True
    requires_account_id: ClassVar[bool] = True
    cursor_field: ClassVar[str] = "updated_time"
    entity_prefix: ClassVar[str]  # Used for API filtering (e.g., "campaign", "ad")
    fields: ClassVar[list[str]] = []

    id: str | None = Field(
        default=None,
        # Don't schematize the default value.
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )

    @classmethod
    def resource_name(cls) -> str:
        return cls.name if hasattr(cls, "name") else cls.__name__

    @staticmethod
    def _coerce_to_type(field_name: str, value: Any, target_type: type) -> Any:
        logger.debug(
            f"Coercing value '{value}' ({type(value).__name__}) to {target_type.__name__}"
        )

        if value is None:
            return None

        match target_type:
            case type() if target_type is bool:
                if isinstance(value, str):
                    lower = value.lower().strip()
                    if lower in ("true", "1", "yes", "on"):
                        logger.debug(f"Coercing string '{value}' to bool True")
                        return True
                    if lower in ("false", "0", "no", "off", ""):
                        logger.debug(f"Coercing string '{value}' to bool False")
                        return False
                    logger.debug(
                        f"String '{value}' not recognized as boolean, using default bool coercion"
                    )
                return bool(value)

            case type() if target_type is int:
                if isinstance(value, str):
                    try:
                        result = int(float(value))  # Handle "123.0" -> 123
                        if value != str(result):
                            logger.debug(f"Coerced string '{value}' to int {result}")
                        return result
                    except (ValueError, TypeError) as e:
                        logger.debug(
                            f"Failed to coerce string '{value}' to int: {e}, returning 0"
                        )
                        return 0
                return int(value)

            case type() if target_type is float:
                if isinstance(value, str):
                    try:
                        float_result = float(value)
                        logger.debug(
                            f"Coerced string '{value}' to float {float_result}"
                        )
                        return float_result
                    except (ValueError, TypeError) as e:
                        logger.debug(
                            f"Failed to coerce string '{value}' to float: {e}, returning 0.0"
                        )
                        return 0.0
                return float(value)

            case type() if target_type is str:
                return str(value)

            case type() if target_type is list:
                if not isinstance(value, list):
                    logger.debug(
                        f"Coercing non-list value {type(value).__name__} to list"
                    )
                    return [value] if value else []
                return value

            case type() if target_type is dict:
                if not isinstance(value, dict):
                    logger.debug(
                        f"Coercing non-dict value {type(value).__name__} to empty dict"
                    )
                    return {}
                return value

            case _:
                logger.debug(
                    f"Unknown target type {target_type}, returning value as-is"
                )
                return value

    @staticmethod
    def _get_field_type(
        field_name: str, resource_name: str | None = None
    ) -> type | None:
        if resource_name and resource_name in RESOURCE_FIELD_TYPES:
            if field_name in RESOURCE_FIELD_TYPES[resource_name]:
                return RESOURCE_FIELD_TYPES[resource_name][field_name]

        return COMMON_FIELD_TYPES.get(field_name)

    @staticmethod
    def _coerce_nested_object(obj: dict, schema: dict[str, type]) -> dict:
        if not isinstance(obj, dict):
            logger.debug(
                f"Expected dict for nested object coercion, got {type(obj).__name__}, returning empty dict"
            )
            return {}

        coerced: dict[str, Any] = {}
        for field_name, value in obj.items():
            if value is None:
                coerced[field_name] = None
                continue

            expected_type = schema.get(field_name)
            if expected_type is None:
                logger.debug(
                    f"Nested field '{field_name}' not in schema, keeping as-is"
                )
                coerced[field_name] = value
                continue

            if isinstance(value, expected_type):
                coerced[field_name] = value
                continue

            try:
                logger.debug(
                    f"Coercing nested field '{field_name}' from {type(value).__name__} to {expected_type.__name__}"
                )
                coerced[field_name] = FacebookResource._coerce_to_type(
                    field_name,
                    value,
                    expected_type,
                )
            except Exception as e:
                logger.debug(
                    f"Failed to coerce nested field '{field_name}': {e}, keeping original value"
                )
                coerced[field_name] = value

        return coerced

    @model_validator(mode="before")
    @classmethod
    def _normalize_values(cls, data: dict) -> dict:
        """
        Normalize ALL field values to their expected types based on prior connector implementations.

        Facebook API may return numbers as strings, booleans as strings, etc.
        This ensures serialized output has correct types for schema inference.
        """
        if not isinstance(data, dict):
            logger.debug(
                f"Expected dict for validation, got {type(data).__name__}, returning as-is"
            )
            return data

        resource_name = cls.resource_name()
        logger.debug(
            f"Normalizing values for resource: {resource_name or 'FacebookResource (base)'}"
        )

        # Coerce each field to its expected type
        for field_name, value in list(data.items()):
            if value is None:
                continue

            expected_type = cls._get_field_type(field_name, resource_name)
            if expected_type is None:
                logger.debug(
                    f"Unknown field '{field_name}' (type: {type(value).__name__}), no type mapping available"
                )
                continue

            if expected_type is dict:
                nested_schema = NESTED_OBJECT_SCHEMAS.get(field_name)
                if nested_schema and isinstance(value, dict):
                    logger.debug(
                        f"Recursively coercing nested object field '{field_name}'"
                    )
                    data[field_name] = cls._coerce_nested_object(value, nested_schema)
                elif not isinstance(value, dict):
                    logger.debug(
                        f"Field '{field_name}' expected dict but got {type(value).__name__}, converting to empty dict"
                    )
                    data[field_name] = {}
                else:
                    logger.debug(
                        f"Field '{field_name}' is dict without nested schema, keeping as-is"
                    )
                continue

            if isinstance(value, expected_type):
                continue

            try:
                logger.debug(
                    f"Coercing field '{field_name}' from {type(value).__name__} to {expected_type.__name__}"
                )
                data[field_name] = cls._coerce_to_type(field_name, value, expected_type)
            except Exception as e:
                logger.debug(
                    f"Failed to coerce field '{field_name}': {e}, keeping original value"
                )
                pass

        return data


FullRefreshFetchFn = Callable[
    ["FacebookAPIClient", type[FacebookResource], list[str], Logger],
    AsyncGenerator[FacebookResource, None],
]

IncrementalFetchPageFn = Callable[
    [
        "FacebookAPIClient",
        type[FacebookResource],
        str,
        datetime,
        Logger,
        PageCursor,
        LogCursor,
    ],
    AsyncGenerator[FacebookResource | PageCursor, None],
]

IncrementalFetchChangesFn = Callable[
    ["FacebookAPIClient", type[FacebookResource], str, Logger, LogCursor],
    AsyncGenerator[FacebookResource | LogCursor, None],
]

InsightsFetchPageFn = Callable[
    [
        "FacebookInsightsJobManager",
        type[FacebookResource],
        list[str],
        datetime,
        datetime | None,
        Logger,
        PageCursor,
    ],
    AsyncGenerator[FacebookResource | PageCursor, None],
]

InsightsFetchChangesFn = Callable[
    [
        "FacebookInsightsJobManager",
        type[FacebookResource],
        list[str],
        datetime,
        datetime | None,
        Logger,
        LogCursor,
    ],
    AsyncGenerator[FacebookResource | LogCursor, None],
]


class AdAccount(FacebookResource):
    name: ClassVar[str] = "ad_account"
    endpoint: ClassVar[str] = "/"
    interval: ClassVar[timedelta] = timedelta(hours=1)
    is_paginated: ClassVar[bool] = False
    requires_account_id: ClassVar[bool] = False
    fields: ClassVar[list[str]] = [
        "id",
        "account_id",
        "account_status",
        "age",
        "amount_spent",
        "balance",
        "business",
        "business_city",
        "business_country_code",
        "business_name",
        "business_state",
        "business_street",
        "business_street2",
        "business_zip",
        "can_create_brand_lift_study",
        "capabilities",
        "created_time",
        "currency",
        "disable_reason",
        "end_advertiser",
        "end_advertiser_name",
        "extended_credit_invoice_group",
        "failed_delivery_checks",
        "fb_entity",
        "funding_source",
        "funding_source_details",
        "has_migrated_permissions",
        "io_number",
        "is_attribution_spec_system_default",
        "is_direct_deals_enabled",
        "is_in_3ds_authorization_enabled_market",
        "is_notifications_enabled",
        "is_personal",
        "is_prepay_account",
        "is_tax_id_required",
        "line_numbers",
        "media_agency",
        "min_campaign_group_spend_cap",
        "min_daily_budget",
        "name",
        "offsite_pixels_tos_accepted",
        "owner",
        "partner",
        "rf_spec",
        "spend_cap",
        "tax_id",
        "tax_id_status",
        "tax_id_type",
        "timezone_id",
        "timezone_name",
        "timezone_offset_hours_utc",
        "tos_accepted",
        "user_tasks",
        "user_tos_accepted",
    ]


class AdCreative(FacebookResource):
    name: ClassVar[str] = "ad_creative"
    endpoint: ClassVar[str] = "adcreatives"
    interval: ClassVar[timedelta] = timedelta(hours=1)
    fields: ClassVar[list[str]] = [
        "id",
        "account_id",
        "actor_id",
        "adlabels",
        "applink_treatment",
        "asset_feed_spec",
        "body",
        "call_to_action_type",
        "effective_instagram_story_id",
        "effective_object_story_id",
        "image_crops",
        "image_hash",
        "image_url",
        "instagram_actor_id",
        "instagram_permalink_url",
        "instagram_story_id",
        "link_og_id",
        "link_url",
        "name",
        "object_id",
        "object_story_id",
        "object_story_spec",
        "object_type",
        "object_url",
        "product_set_id",
        "status",
        "template_url",
        "template_url_spec",
        "thumbnail_data_url",
        "thumbnail_url",
        "title",
        "url_tags",
        "video_id",
    ]


class CustomConversions(FacebookResource):
    name: ClassVar[str] = "custom_conversions"
    endpoint: ClassVar[str] = "customconversions"
    interval: ClassVar[timedelta] = timedelta(hours=1)
    fields: ClassVar[list[str]] = [
        "id",
        "account_id",
        "business",
        "creation_time",
        "custom_event_type",
        "data_sources",
        "default_conversion_value",
        "description",
        "event_source_type",
        "first_fired_time",
        "is_archived",
        "is_unavailable",
        "last_fired_time",
        "name",
        "offline_conversion_data_set",
        "retention_days",
        "rule",
    ]


class Campaigns(FacebookResource):
    name: ClassVar[str] = "campaigns"
    endpoint: ClassVar[str] = "campaigns"
    interval: ClassVar[timedelta] = timedelta(hours=1)
    cursor_field: ClassVar[str] = "updated_time"
    entity_prefix: ClassVar[str] = "campaign"
    fields: ClassVar[list[str]] = [
        "id",
        "account_id",
        "adlabels",
        "bid_strategy",
        "budget_rebalance_flag",
        "budget_remaining",
        "buying_type",
        "created_time",
        "daily_budget",
        "effective_status",
        "issues_info",
        "lifetime_budget",
        "name",
        "objective",
        "smart_promotion_type",
        "source_campaign_id",
        "special_ad_category",
        "special_ad_category_country",
        "spend_cap",
        "start_time",
        "stop_time",
        "updated_time",
    ]


class AdSets(FacebookResource):
    """Facebook Ad Sets resource.

    Ad Sets define the targeting, budget, and schedule for a group of ads.
    Schema matches Airbyte source-facebook-marketing/schemas/ad_sets.json
    """

    name: ClassVar[str] = "ad_sets"
    endpoint: ClassVar[str] = "adsets"
    interval: ClassVar[timedelta] = timedelta(hours=1)
    cursor_field: ClassVar[str] = "updated_time"
    entity_prefix: ClassVar[str] = "adset"
    fields: ClassVar[list[str]] = [
        "id",
        "account_id",
        "adlabels",
        "bid_amount",
        "bid_constraints",
        "bid_info",
        "bid_strategy",
        "budget_remaining",
        "campaign_id",
        "created_time",
        "daily_budget",
        "effective_status",
        "end_time",
        "lifetime_budget",
        "name",
        "promoted_object",
        "start_time",
        "targeting",
        "updated_time",
    ]


class Ads(FacebookResource):
    """Facebook Ads resource.

    Ads are the individual ad units within an ad set.
    Schema matches Airbyte source-facebook-marketing/schemas/ads.json
    """

    name: ClassVar[str] = "ads"
    endpoint: ClassVar[str] = "ads"
    interval: ClassVar[timedelta] = timedelta(hours=1)
    cursor_field: ClassVar[str] = "updated_time"
    entity_prefix: ClassVar[str] = "ad"
    fields: ClassVar[list[str]] = [
        "id",
        "account_id",
        "adlabels",
        "adset_id",
        "bid_amount",
        "bid_info",
        "bid_type",
        "campaign_id",
        "conversion_specs",
        "created_time",
        "creative",
        "effective_status",
        "last_updated_by_app_id",
        "name",
        "recommendations",
        "source_ad_id",
        "status",
        "targeting",
        "tracking_specs",
        "updated_time",
    ]


class Activities(FacebookResource):
    """Facebook Activities resource.

    Activity log for account, showing changes made to campaigns, ad sets, and ads.
    Schema matches Airbyte source-facebook-marketing/schemas/activities.json

    Note: Activities do not have an 'id' field, so we use a composite primary key.
    """

    name: ClassVar[str] = "activities"
    endpoint: ClassVar[str] = "activities"
    interval: ClassVar[timedelta] = timedelta(hours=1)
    cursor_field: ClassVar[str] = "event_time"
    primary_keys: ClassVar[list[str]] = [
        "/object_id",
        "/actor_id",
        "/application_id",
        "/event_time",
        "/event_type",
    ]
    fields: ClassVar[list[str]] = [
        "account_id",
        "actor_id",
        "actor_name",
        "application_id",
        "application_name",
        "date_time_in_timezone",
        "event_time",
        "event_type",
        "extra_data",
        "object_id",
        "object_name",
        "object_type",
        "translated_event_type",
    ]

    object_id: str
    actor_id: str
    application_id: str
    event_time: str
    event_type: str


class Images(FacebookResource):
    """Facebook Ad Images resource.

    Images used in ad creatives.
    Schema matches Airbyte source-facebook-marketing/schemas/images.json
    """

    name: ClassVar[str] = "images"
    endpoint: ClassVar[str] = "adimages"
    interval: ClassVar[timedelta] = timedelta(hours=1)
    cursor_field: ClassVar[str] = "updated_time"
    fields: ClassVar[list[str]] = [
        "id",
        "account_id",
        "created_time",
        "creatives",
        "filename",
        "hash",
        "height",
        "is_associated_creatives_in_adgroups",
        "name",
        "original_height",
        "original_width",
        "permalink_url",
        "status",
        "updated_time",
        "url",
        "url_128",
        "width",
    ]


class Videos(FacebookResource):
    """Facebook Ad Videos resource.

    Videos used in ad creatives.
    Schema matches Airbyte source-facebook-marketing/schemas/videos.json
    """

    name: ClassVar[str] = "videos"
    endpoint: ClassVar[str] = "advideos"
    interval: ClassVar[timedelta] = timedelta(hours=1)
    cursor_field: ClassVar[str] = "updated_time"
    fields: ClassVar[list[str]] = [
        "id",
        "account_id",
        "ad_breaks",
        "backdated_time",
        "backdated_time_granularity",
        "content_category",
        "content_tags",
        "created_time",
        "custom_labels",
        "description",
        "embed_html",
        "embeddable",
        "format",
        "icon",
        "is_crosspost_video",
        "is_crossposting_eligible",
        "is_episode",
        "is_instagram_eligible",
        "length",
        "live_status",
        "permalink_url",
        "post_views",
        "premiere_living_room_status",
        "published",
        "scheduled_publish_time",
        "source",
        "title",
        "universal_video_id",
        "updated_time",
        "views",
    ]


class FacebookInsightsResource(FacebookResource):
    level: ClassVar[str] = "account"
    breakdowns: ClassVar[list[str]] = []
    action_breakdowns: ClassVar[list[str]] = []
    time_increment: ClassVar[int] = 1
    cursor_field: ClassVar[str] = "date_start"

    @classmethod
    def _get_required_fields_for_level(cls) -> list[str]:
        required = ["account_id", "date_start"]

        if cls.level == "ad":
            required.append("ad_id")
        elif cls.level == "adset":
            required.append("adset_id")
        elif cls.level == "campaign":
            required.append("campaign_id")

        return required


class AdsInsights(FacebookInsightsResource):
    name: ClassVar[str] = "ads_insights"
    endpoint: ClassVar[str] = "insights"
    interval: ClassVar[timedelta] = timedelta(hours=1)
    requires_account_id: ClassVar[bool] = True

    fields: ClassVar[list[str]] = [
        # Required fields
        "account_id",
        "account_name",
        "date_start",
        "date_stop",
        # Level-specific ID fields (for ad level - default)
        "ad_id",
        "ad_name",
        # Core performance metrics
        "impressions",
        "clicks",
        "spend",
        "reach",
        "frequency",
        # Cost metrics
        "cpm",
        "cpp",
        "cpc",
        "ctr",
        "cost_per_unique_click",
        "cost_per_inline_link_click",
        "cost_per_inline_post_engagement",
        # Engagement metrics
        "unique_clicks",
        "unique_ctr",
        "inline_link_clicks",
        "inline_link_click_ctr",
        "inline_post_engagement",
        "unique_inline_link_clicks",
        "unique_inline_link_click_ctr",
        # Conversion and action metrics (arrays)
        "actions",
        "conversions",
        "action_values",
        "conversion_values",
        "cost_per_action_type",
        "cost_per_conversion",
        # Video metrics (arrays)
        "video_30_sec_watched_actions",
        "video_p25_watched_actions",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
        "video_p95_watched_actions",
        "video_p100_watched_actions",
        # Additional useful metrics
        "objective",
        "optimization_goal",
        "buying_type",
        "attribution_setting",
        # Quality/ranking metrics
        "quality_ranking",
        "engagement_rate_ranking",
        "conversion_rate_ranking",
    ]

    primary_keys: ClassVar[list[str]] = [
        "/account_id",
        "/date_start",
        "/ad_id",
    ]

    level: ClassVar[str] = "ad"
    breakdowns: ClassVar[list[str]] = []
    action_breakdowns: ClassVar[list[str]] = [
        "action_type",
        "action_target_id",
        "action_destination",
    ]
    time_increment: ClassVar[int] = 1
    action_attribution_windows: ClassVar[list[str]] = [
        "1d_click",
        "7d_click",
        "28d_click",
        "1d_view",
        "7d_view",
        "28d_view",
    ]
    # Facebook store metrics maximum of 37 months old. Any time range that
    # older that 37 months from current date would result in 400 Bad request
    # HTTP response.
    # https://developers.facebook.com/docs/marketing-api/reference/ad-account/insights/#overview
    insights_retention_period: ClassVar[relativedelta] = relativedelta(months=37)

    account_id: str
    date_start: str
    ad_id: str

    @classmethod
    def resource_name(cls) -> str:
        return "ads_insights"

    @property
    def minimum_date_start(self) -> datetime:
        """Calculate the minimum allowed start date based on the retention period."""
        now = datetime.now(tz=UTC)
        return now - self.insights_retention_period

    _ACCOUNT_FIELDS = ["account_id", "account_name"]
    _CAMPAIGN_FIELDS = ["campaign_id", "campaign_name"]
    _ADSET_FIELDS = ["adset_id", "adset_name"]
    _AD_FIELDS = ["ad_id", "ad_name"]

    @property
    def level_id_field(self) -> str:
        if self.level == "ad":
            return "ad_id"
        elif self.level == "adset":
            return "adset_id"
        elif self.level == "campaign":
            return "campaign_id"
        else:
            raise RuntimeError(
                f"Unexpected level {self.level} for AdsInsights stream. "
                f"Expected one of: campaign, adset, ad. "
                f"Please check your configuration."
            )

    def level_specific_fields(self) -> list[str]:
        common_fields = self._ACCOUNT_FIELDS + self._CAMPAIGN_FIELDS
        if self.level == "campaign":
            return common_fields
        elif self.level == "adset":
            return common_fields + self._ADSET_FIELDS
        elif self.level == "ad":
            return common_fields + self._ADSET_FIELDS + self._AD_FIELDS
        else:
            raise RuntimeError(
                f"Unexpected level {self.level} for AdsInsights stream. "
                f"Expected one of: campaign, adset, ad. "
                f"Please check your configuration."
            )


class AdsInsightsAgeAndGender(AdsInsights):
    name: ClassVar[str] = "ads_insights_age_and_gender"
    breakdowns: ClassVar[list[str]] = ["age", "gender"]
    primary_keys: ClassVar[list[str]] = AdsInsights.primary_keys + ["/age", "/gender"]
    age: str
    gender: str


class AdsInsightsCountry(AdsInsights):
    name: ClassVar[str] = "ads_insights_country"
    breakdowns = ["country"]
    primary_keys: ClassVar[list[str]] = AdsInsights.primary_keys + ["/country"]
    country: str


class AdsInsightsRegion(AdsInsights):
    name: ClassVar[str] = "ads_insights_region"
    breakdowns = ["region"]
    primary_keys: ClassVar[list[str]] = AdsInsights.primary_keys + ["/region"]
    region: str


class AdsInsightsDma(AdsInsights):
    name: ClassVar[str] = "ads_insights_dma"
    breakdowns = ["dma"]
    primary_keys: ClassVar[list[str]] = AdsInsights.primary_keys + ["/dma"]
    dma: str


class AdsInsightsPlatformAndDevice(AdsInsights):
    name: ClassVar[str] = "ads_insights_platform_and_device"
    breakdowns = ["publisher_platform", "platform_position", "impression_device"]
    action_breakdowns = ["action_type"]
    primary_keys: ClassVar[list[str]] = AdsInsights.primary_keys + [
        "/publisher_platform",
        "/platform_position",
        "/impression_device",
    ]
    publisher_platform: str
    platform_position: str
    impression_device: str


class AdsInsightsActionType(AdsInsights):
    name: ClassVar[str] = "ads_insights_action_type"
    breakdowns = []
    action_breakdowns = ["action_type"]


def build_custom_ads_insights_model(
    config: InsightsConfig,
) -> type[AdsInsights]:
    """Build a dynamic model for custom insights.

    Key considerations:
    - Breakdown fields are optional (Facebook may not return them if no data exists)
    - Only base required fields (account_id, date_start, level-specific ID) are mandatory
    - Primary keys include breakdowns, but the fields themselves can be None
    - This matches Airbyte's behavior where breakdowns are part of the composite key
    """
    # Build field definitions for create_model
    # Format: field_name: (type, default_value)
    field_defs: dict[str, Any] = {}

    # Ensure fields list includes base requirements
    fields = config.fields.copy() if config.fields else []

    # Determine level-specific required fields
    level_id_field = None
    if config.level == "ad":
        level_id_field = "ad_id"
    elif config.level == "adset":
        level_id_field = "adset_id"
    elif config.level == "campaign":
        level_id_field = "campaign_id"

    # Base required fields that must always be present
    base_required_fields = {"account_id", "date_start"}
    if level_id_field:
        base_required_fields.add(level_id_field)

    # Build primary keys: base fields + breakdown fields
    primary_keys = ["/account_id", "/date_start"]
    if level_id_field:
        primary_keys.append(f"/{level_id_field}")
    primary_keys.extend([f"/{b}" for b in config.breakdowns])
    primary_keys = sorted(set(primary_keys))

    # Add field definitions for primary key fields
    for pk in primary_keys:
        field_name = pk.lstrip("/")

        # Get the field type
        resource_specific_field_types = RESOURCE_FIELD_TYPES.get("ads_insights", {})
        field_type = resource_specific_field_types.get(
            field_name,
            COMMON_FIELD_TYPES.get(field_name, str),
        )

        if field_type is None:
            field_type = str

        # Only base required fields are mandatory, breakdowns are optional
        if field_name in base_required_fields:
            field_defs[field_name] = (field_type, ...)
        else:
            # Breakdown fields are optional - Facebook may not return them
            field_defs[field_name] = (field_type | None, None)

    camel_case_name = "".join(word.capitalize() for word in config.name.split("_"))
    model = create_model(
        camel_case_name,
        __base__=(AdsInsights,),
        **field_defs,
    )

    # Set ClassVars for the model
    setattr(model, "fields", fields)
    setattr(model, "primary_keys", primary_keys)
    setattr(model, "name", config.name)
    setattr(model, "level", config.level)
    setattr(model, "breakdowns", config.breakdowns)
    setattr(model, "action_breakdowns", config.action_breakdowns)
    setattr(model, "time_increment", config.time_increment)

    return model
