import builtins
from enum import StrEnum
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


from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceState,
    ConnectorState as GenericConnectorState,
    PageCursor,
    LogCursor,
)
from estuary_cdk.flow import (
    OAuth2Spec,
    LongLivedClientCredentialsOAuth2Credentials,
    ValidationError,
)
from .constants import (
    API_VERSION,
    AD_ACCOUNT_SOURCED_SCHEMA,
    AD_CREATIVE_SOURCED_SCHEMA,
)
from .enums import (
    ActionBreakdown,
    ApiLevel,
    AttributionWindow,
    Breakdown,
    CursorField,
    Field as F,
    ResourceName,
)
from .fields import (
    AD_INSIGHTS_VALID_FIELDS,
    AD_INSIGHTS_VALID_BREAKDOWNS,
    AD_INSIGHTS_VALID_ACTION_BREAKDOWNS,
    get_field_type,
)
from .utils import str_to_list

logger: Logger = getLogger(__name__)
ConnectorState = GenericConnectorState[ResourceState]


class FeatureFlag(StrEnum):
    """Available feature flags for the connector."""

    SOURCED_SCHEMAS = "sourced_schemas"


# Facebook store metrics maximum of 37 months old. Any time range that
# older that 37 months from current date would result in 400 Bad request
# HTTP response.
# https://developers.facebook.com/docs/marketing-api/reference/ad-account/insights/#overview
DATA_RETENTION_PERIOD = 37
ACCOUNT_IDS_PATTERN = r"^\d[\d,]+\d$"

# Default lookback window for insights data. Facebook freezes insight data
# after this many days, so we retrieve refreshed insights from this period.
DEFAULT_LOOKBACK_WINDOW = 28

# Type mapping for Facebook API fields (similar to Sage Intacct's DATATYPE_MAP)
# Maps field names to Python types for proper schema generation and validation
# Based on ads_insights.json schema from Airbyte source-facebook-marketing
FACEBOOK_INSIGHTS_TYPE_MAP: dict[str, type] = {
    F.ACCOUNT_CURRENCY: str,
    F.ACCOUNT_ID: str,
    F.ACCOUNT_NAME: str,
    F.AD_ID: str,
    F.AD_NAME: str,
    F.ADSET_ID: str,
    F.ADSET_NAME: str,
    F.ATTRIBUTION_SETTING: str,
    F.BUYING_TYPE: str,
    F.CAMPAIGN_ID: str,
    F.CAMPAIGN_NAME: str,
    F.CONVERSION_RATE_RANKING: str,
    F.CREATED_TIME: str,
    F.DATE_START: str,
    F.DATE_STOP: str,
    F.ENGAGEMENT_RATE_RANKING: str,
    F.OBJECTIVE: str,
    F.OPTIMIZATION_GOAL: str,
    F.QUALITY_RANKING: str,
    F.UPDATED_TIME: str,
    F.CLICKS: int,
    F.IMPRESSIONS: int,
    F.INLINE_LINK_CLICKS: int,
    F.INLINE_POST_ENGAGEMENT: int,
    F.REACH: int,
    F.UNIQUE_CLICKS: int,
    F.UNIQUE_INLINE_LINK_CLICKS: int,
    F.AUCTION_BID: float,
    F.AUCTION_COMPETITIVENESS: float,
    F.AUCTION_MAX_COMPETITOR_BID: float,
    F.CANVAS_AVG_VIEW_PERCENT: float,
    F.CANVAS_AVG_VIEW_TIME: float,
    F.COST_PER_ESTIMATED_AD_RECALLERS: float,
    F.COST_PER_INLINE_LINK_CLICK: float,
    F.COST_PER_INLINE_POST_ENGAGEMENT: float,
    F.COST_PER_UNIQUE_CLICK: float,
    F.COST_PER_UNIQUE_INLINE_LINK_CLICK: float,
    F.CPC: float,
    F.CPM: float,
    F.CPP: float,
    F.CTR: float,
    F.ESTIMATED_AD_RECALL_RATE: float,
    F.ESTIMATED_AD_RECALLERS: float,
    F.FREQUENCY: float,
    F.FULL_VIEW_IMPRESSIONS: float,
    F.FULL_VIEW_REACH: float,
    F.INLINE_LINK_CLICK_CTR: float,
    F.INSTANT_EXPERIENCE_CLICKS_TO_OPEN: float,
    F.INSTANT_EXPERIENCE_CLICKS_TO_START: float,
    F.QUALIFYING_QUESTION_QUALIFY_ANSWER_RATE: float,
    F.SOCIAL_SPEND: float,
    F.SPEND: float,
    F.UNIQUE_CTR: float,
    F.UNIQUE_INLINE_LINK_CLICK_CTR: float,
    F.UNIQUE_LINK_CLICKS_CTR: float,
    F.ACTION_VALUES: list,
    F.ACTIONS: list,
    F.AD_CLICK_ACTIONS: list,
    F.AD_IMPRESSION_ACTIONS: list,
    F.CATALOG_SEGMENT_ACTIONS: list,
    F.CATALOG_SEGMENT_VALUE: list,
    F.CATALOG_SEGMENT_VALUE_MOBILE_PURCHASE_ROAS: list,
    F.CATALOG_SEGMENT_VALUE_OMNI_PURCHASE_ROAS: list,
    F.CATALOG_SEGMENT_VALUE_WEBSITE_PURCHASE_ROAS: list,
    F.CONVERSION_VALUES: list,
    F.CONVERSIONS: list,
    F.CONVERTED_PRODUCT_QUANTITY: list,
    F.CONVERTED_PRODUCT_VALUE: list,
    F.COST_PER_15_SEC_VIDEO_VIEW: list,
    F.COST_PER_2_SEC_CONTINUOUS_VIDEO_VIEW: list,
    F.COST_PER_ACTION_TYPE: list,
    F.COST_PER_AD_CLICK: list,
    F.COST_PER_CONVERSION: list,
    F.COST_PER_OUTBOUND_CLICK: list,
    F.COST_PER_THRUPLAY: list,
    F.COST_PER_UNIQUE_ACTION_TYPE: list,
    F.COST_PER_UNIQUE_OUTBOUND_CLICK: list,
    F.INSTANT_EXPERIENCE_OUTBOUND_CLICKS: list,
    F.MOBILE_APP_PURCHASE_ROAS: list,
    F.OUTBOUND_CLICKS: list,
    F.OUTBOUND_CLICKS_CTR: list,
    F.PURCHASE_ROAS: list,
    F.UNIQUE_ACTIONS: list,
    F.UNIQUE_OUTBOUND_CLICKS: list,
    F.UNIQUE_OUTBOUND_CLICKS_CTR: list,
    F.VIDEO_15_SEC_WATCHED_ACTIONS: list,
    F.VIDEO_30_SEC_WATCHED_ACTIONS: list,
    F.VIDEO_AVG_TIME_WATCHED_ACTIONS: list,
    F.VIDEO_CONTINUOUS_2_SEC_WATCHED_ACTIONS: list,
    F.VIDEO_P100_WATCHED_ACTIONS: list,
    F.VIDEO_P25_WATCHED_ACTIONS: list,
    F.VIDEO_P50_WATCHED_ACTIONS: list,
    F.VIDEO_P75_WATCHED_ACTIONS: list,
    F.VIDEO_P95_WATCHED_ACTIONS: list,
    F.VIDEO_PLAY_ACTIONS: list,
    F.VIDEO_PLAY_CURVE_ACTIONS: list,
    F.VIDEO_PLAY_RETENTION_0_TO_15S_ACTIONS: list,
    F.VIDEO_PLAY_RETENTION_20_TO_60S_ACTIONS: list,
    F.VIDEO_PLAY_RETENTION_GRAPH_ACTIONS: list,
    F.VIDEO_TIME_WATCHED_ACTIONS: list,
    F.WEBSITE_CTR: list,
    F.WEBSITE_PURCHASE_ROAS: list,
    F.AGE: str,
    F.GENDER: str,
    F.COUNTRY: str,
    F.REGION: str,
    F.DMA: str,
    F.PUBLISHER_PLATFORM: str,
    F.PLATFORM_POSITION: str,
    F.DEVICE_PLATFORM: str,
    F.IMPRESSION_DEVICE: str,
    F.PRODUCT_ID: str,
    F.HOURLY_STATS_AGGREGATED_BY_ADVERTISER_TIME_ZONE: str,
    F.HOURLY_STATS_AGGREGATED_BY_AUDIENCE_TIME_ZONE: str,
    F.ACTION_TYPE: str,
    F.ACTION_TARGET_ID: str,
    F.ACTION_DESTINATION: str,
}


OAUTH2_SPEC = OAuth2Spec(
    provider="facebook",
    authUrlTemplate=(
        f"https://www.facebook.com/{API_VERSION}/dialog/oauth"
        "?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        "&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        "&scope=ads_management,ads_read,read_insights,business_management"
        "&state={{#urlencode}}{{{  state }}}{{/urlencode}}"
    ),
    accessTokenResponseMap={"access_token": "/access_token"},
    accessTokenUrlTemplate=(
        f"https://graph.facebook.com/{API_VERSION}/oauth/access_token"
        "?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        "&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        "&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
        "&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
    ),
    accessTokenHeaders={},
    accessTokenBody="",  # Use query arguments
)

if TYPE_CHECKING:
    from .client import FacebookAPIClient
    from .job_manager import FacebookInsightsJobManager

    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials
else:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials.for_provider(
        OAUTH2_SPEC.provider
    )


def default_start_date() -> datetime:
    # returns now minus DATA_RETENTION_PERIOD months
    now = datetime.now(tz=UTC)
    retention_date = now - relativedelta(months=DATA_RETENTION_PERIOD)
    return retention_date


class CommonConfigMixin(BaseModel, extra="allow"):
    start_date: AwareDatetime = Field(
        title="Start Date",
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data before this date will not be replicated.",
        examples=["2017-01-25T00:00:00Z"],
        default_factory=default_start_date,
    )
    insights_lookback_window: PositiveInt = Field(
        title="Insights Lookback Window",
        description=(
            "The attribution window. Facebook freezes insight data 28 days after it was generated, "
            "which means that all data from the past 28 days may have changed since we last emitted it, "
            "so you can retrieve refreshed insights from the past by setting this parameter. "
            "If you set a custom lookback window value in Facebook account, please provide the same value here."
        ),
        default=DEFAULT_LOOKBACK_WINDOW,
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


class InsightsConfig(CommonConfigMixin):
    model_config = ConfigDict(
        use_enum_values=True,
    )

    name: str = Field(
        title="Name",
        description="The name value of insight",
        json_schema_extra={"order": 0},
    )
    level: ApiLevel = Field(
        title="Level",
        description="Chosen level for API",
        default=ApiLevel.AD,
        json_schema_extra={"order": 1},
    )
    fields: str = Field(
        title="Fields",
        description="A comma-separated list of chosen fields",
        examples=["account_id,ad_id,impressions,clicks,spend"],
        json_schema_extra={"order": 2},
    )
    breakdowns: str = Field(
        title="Breakdowns",
        description="A comma-separated list of chosen breakdowns",
        examples=["age,gender,region"],
        json_schema_extra={"order": 3},
    )
    action_breakdowns: str | None = Field(
        title="Action Breakdowns",
        description="A comma-separated list of chosen action breakdowns",
        examples=["action_type"],
        json_schema_extra={"order": 4},
        default=None,
    )

    @field_validator("fields")
    @classmethod
    def validate_fields(cls, v: str) -> str:
        fields = str_to_list(v)
        invalid = [f for f in fields if f not in AD_INSIGHTS_VALID_FIELDS]
        if invalid:
            raise ValueError(
                f"Invalid fields: {', '.join(invalid)}. "
                f"See https://developers.facebook.com/docs/marketing-api/insights/parameters for valid fields."
            )

        return v

    @field_validator("breakdowns")
    @classmethod
    def validate_breakdowns(cls, v: str) -> str:
        fields = str_to_list(v)
        invalid = [b for b in fields if b not in AD_INSIGHTS_VALID_BREAKDOWNS]
        if invalid:
            raise ValueError(
                f"Invalid breakdowns: {', '.join(invalid)}. Must be one of: {', '.join(sorted(AD_INSIGHTS_VALID_BREAKDOWNS))}"
            )

        return v

    @field_validator("action_breakdowns")
    @classmethod
    def validate_action_breakdowns(cls, v: str | None) -> str | None:
        if v is None:
            return v

        fields = str_to_list(v)
        invalid = [ab for ab in fields if ab not in AD_INSIGHTS_VALID_ACTION_BREAKDOWNS]
        if invalid:
            raise ValueError(
                f"Invalid action_breakdowns: {', '.join(invalid)}. Must be one of: {', '.join(sorted(AD_INSIGHTS_VALID_ACTION_BREAKDOWNS))}"
            )

        return v


class EndpointConfig(CommonConfigMixin):
    account_ids: str = Field(
        title="Account IDs",
        description="Comma-separated list of Facebook Ad Account IDs (e.g., 123456789,987654321)",
        json_schema_extra={"order": 0},
        pattern=ACCOUNT_IDS_PATTERN,
    )
    credentials: OAuth2Credentials = Field(
        title="Authentication",
        discriminator="credentials_title",
    )
    custom_insights: list[InsightsConfig] = Field(
        title="Custom Insights",
        description=(
            "A list which contains ad statistics entries, each entry must have a name and can contains fields, "
            'breakdowns or action_breakdowns. Click on "add" to fill this field.'
        ),
        default=[],
    )

    class Advanced(BaseModel):
        include_deleted: bool = Field(
            title="Include Deleted Records",
            description="Set to active if you want to include data from deleted entities for resources that support it.",
            default=False,
        )
        fetch_thumbnail_images: bool = Field(
            title="Fetch Thumbnail Images from Ad Creative",
            description="Set to active if you want to fetch the thumbnail_url and store the result in thumbnail_data_url for each Ad Creative.",
            default=False,
        )
        feature_flags: str = Field(
            title="Feature Flags",
            description="Comma-separated list of experimental feature flags to enable.",
            default="",
        )

        def has_feature_flag(self, flag: FeatureFlag) -> bool:
            """Check if a feature flag is enabled."""
            flags = [f.strip() for f in self.feature_flags.split(",") if f.strip()]
            return flag.value in flags

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
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
    primary_keys: ClassVar[list[str]]
    endpoint: ClassVar[str]
    cursor_field: ClassVar[str] = "updated_time"
    enable_deleted_filter: ClassVar[bool] = False
    entity_prefix: ClassVar[str] = ""  # Used for API filtering (e.g., "campaign", "ad")
    fields: ClassVar[list[str]] = []

    @classmethod
    def resource_name(cls) -> str:
        return cls.name if hasattr(cls, "name") else cls.__name__

    @staticmethod
    def _coerce_to_type(value: Any, target_type: type) -> Any:
        logger.debug(
            f"Coercing value '{value}' ({type(value).__name__}) to {target_type.__name__}"
        )

        if value is None:
            return None

        match target_type:
            case builtins.bool:
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

            case builtins.int:
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

            case builtins.float:
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

            case builtins.str:
                return str(value)

            case builtins.list:
                if not isinstance(value, list):
                    logger.debug(
                        f"Coercing non-list value {type(value).__name__} to list"
                    )
                    return [value] if value else []
                return value

            case builtins.dict:
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
    def _coerce_field_value(
        field_name: str,
        value: Any,
        expected_type: type | dict[str, type | dict],
    ) -> Any:
        """
        Coerce a single field value to its expected type.

        Handles type discrimination and recursive coercion for nested objects.

        Type discrimination:
            - dict (the type) = untyped dict, just ensure it's a dict
            - dict instance = typed nested schema, recursively coerce fields
            - simple type = coerce to that type
        """
        if value is None:
            return None

        # Type discrimination for dict fields
        if expected_type is dict:
            # Untyped dict - just ensure it's a dict
            if not isinstance(value, dict):
                logger.debug(
                    f"Field '{field_name}' expected dict but got {type(value).__name__}, converting to empty dict"
                )
                return {}
            logger.debug(f"Field '{field_name}' is untyped dict, keeping as-is")
            return value

        if isinstance(expected_type, dict):
            if not isinstance(value, dict):
                logger.debug(
                    f"Field '{field_name}' expected nested object but got {type(value).__name__}, converting to empty dict"
                )
                return {}

            logger.debug(f"Recursively coercing nested object field '{field_name}'")
            return FacebookResource._coerce_nested_object(value, expected_type)

        if isinstance(value, expected_type):
            return value

        try:
            logger.debug(
                f"Coercing field '{field_name}' from {type(value).__name__} to {expected_type.__name__}"
            )
            return FacebookResource._coerce_to_type(value, expected_type)
        except Exception as e:
            logger.debug(
                f"Failed to coerce field '{field_name}': {e}, keeping original value"
            )
            return value

    @staticmethod
    def _coerce_nested_object(obj: dict, schema: dict[str, type | dict]) -> dict:
        """Recursively coerce nested object fields according to schema.

        Supports unlimited nesting depth where schema values can be:
        - A type (str, int, float, bool, list, dict) for simple fields
        - A dict instance for nested typed objects (recursive coercion)

        Args:
            obj: The object to coerce
            schema: The nested field schema

        Returns:
            Coerced object with fields typed according to schema
        """
        if not isinstance(obj, dict):
            logger.debug(
                f"Expected dict for nested object coercion, got {type(obj).__name__}, returning empty dict"
            )
            return {}

        coerced: dict[str, Any] = {}
        for field_name, value in obj.items():
            expected_type = schema.get(field_name)
            if expected_type is None:
                logger.debug(
                    f"Nested field '{field_name}' not in schema, keeping as-is"
                )
                coerced[field_name] = value
                continue

            coerced[field_name] = FacebookResource._coerce_field_value(
                field_name, value, expected_type
            )

        return coerced

    @model_validator(mode="before")
    @classmethod
    def _normalize_values(cls, data: dict) -> dict:
        """
        Normalize ALL field values to their expected types based on prior connector implementations.

        This transformation ensures compatibility with documents output by the imported Airbyte
        `source-facebook-marketing` connector by matching its type coercion behavior.

        Facebook API may return numbers as strings, booleans as strings, etc.
        This ensures serialized output has correct types for schema inference.

        Type discrimination:
        - dict (the type) = untyped dict, just ensure it's a dict
        - dict instance = typed nested schema, recursively coerce fields
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
            expected_type = get_field_type(field_name, resource_name)
            if expected_type is None:
                logger.debug(
                    f"Unknown field '{field_name}' (type: {type(value).__name__}), no type mapping available"
                )
                continue

            data[field_name] = cls._coerce_field_value(field_name, value, expected_type)

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
        bool,
        Logger,
        PageCursor,
        LogCursor,
    ],
    AsyncGenerator[FacebookResource | PageCursor, None],
]

IncrementalFetchChangesFn = Callable[
    ["FacebookAPIClient", type[FacebookResource], str, bool, Logger, LogCursor],
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
    name: ClassVar[str] = ResourceName.AD_ACCOUNT
    primary_keys: ClassVar[list[str]] = ["/id"]
    endpoint: ClassVar[str] = ""
    fields: ClassVar[list[str]] = [
        F.ID,
        F.ACCOUNT_ID,
        F.ACCOUNT_STATUS,
        F.AGE,
        F.AMOUNT_SPENT,
        F.BALANCE,
        F.BUSINESS,
        F.BUSINESS_CITY,
        F.BUSINESS_COUNTRY_CODE,
        F.BUSINESS_NAME,
        F.BUSINESS_STATE,
        F.BUSINESS_STREET,
        F.BUSINESS_STREET2,
        F.BUSINESS_ZIP,
        F.CAN_CREATE_BRAND_LIFT_STUDY,
        F.CAPABILITIES,
        F.CREATED_TIME,
        F.CURRENCY,
        F.DISABLE_REASON,
        F.END_ADVERTISER,
        F.END_ADVERTISER_NAME,
        F.EXTENDED_CREDIT_INVOICE_GROUP,
        F.FB_ENTITY,
        F.FUNDING_SOURCE,
        F.FUNDING_SOURCE_DETAILS,
        F.HAS_MIGRATED_PERMISSIONS,
        F.IO_NUMBER,
        F.IS_ATTRIBUTION_SPEC_SYSTEM_DEFAULT,
        F.IS_DIRECT_DEALS_ENABLED,
        F.IS_IN_3DS_AUTHORIZATION_ENABLED_MARKET,
        F.IS_NOTIFICATIONS_ENABLED,
        F.IS_PERSONAL,
        F.IS_PREPAY_ACCOUNT,
        F.IS_TAX_ID_REQUIRED,
        F.LINE_NUMBERS,
        F.MEDIA_AGENCY,
        F.MIN_CAMPAIGN_GROUP_SPEND_CAP,
        F.MIN_DAILY_BUDGET,
        F.NAME,
        F.OFFSITE_PIXELS_TOS_ACCEPTED,
        F.OWNER,
        F.PARTNER,
        F.RF_SPEC,
        F.SPEND_CAP,
        F.TAX_ID,
        F.TAX_ID_STATUS,
        F.TAX_ID_TYPE,
        F.TIMEZONE_ID,
        F.TIMEZONE_NAME,
        F.TIMEZONE_OFFSET_HOURS_UTC,
        F.TOS_ACCEPTED,
        F.USER_TASKS,
        F.USER_TOS_ACCEPTED,
    ]

    id: str

    @classmethod
    def sourced_schema(cls) -> dict[str, Any]:
        """Return explicit sourced schema matching legacy connector format."""
        return AD_ACCOUNT_SOURCED_SCHEMA


class AdCreative(FacebookResource):
    name: ClassVar[str] = ResourceName.AD_CREATIVES
    primary_keys: ClassVar[list[str]] = ["/id"]
    endpoint: ClassVar[str] = "adcreatives"
    fields: ClassVar[list[str]] = [
        F.ID,
        F.ACCOUNT_ID,
        F.ACTOR_ID,
        F.ADLABELS,
        F.APPLINK_TREATMENT,
        F.ASSET_FEED_SPEC,
        F.BODY,
        F.CALL_TO_ACTION_TYPE,
        F.EFFECTIVE_INSTAGRAM_STORY_ID,
        F.EFFECTIVE_OBJECT_STORY_ID,
        F.IMAGE_CROPS,
        F.IMAGE_HASH,
        F.IMAGE_URL,
        F.INSTAGRAM_ACTOR_ID,
        F.INSTAGRAM_PERMALINK_URL,
        F.INSTAGRAM_STORY_ID,
        F.LINK_OG_ID,
        F.LINK_URL,
        F.NAME,
        F.OBJECT_ID,
        F.OBJECT_STORY_ID,
        F.OBJECT_STORY_SPEC,
        F.OBJECT_TYPE,
        F.OBJECT_URL,
        F.PRODUCT_SET_ID,
        F.STATUS,
        F.TEMPLATE_URL,
        F.TEMPLATE_URL_SPEC,
        F.THUMBNAIL_DATA_URL,
        F.THUMBNAIL_URL,
        F.TITLE,
        F.URL_TAGS,
        F.VIDEO_ID,
    ]

    id: str

    @classmethod
    def sourced_schema(cls) -> dict[str, Any]:
        """Return explicit sourced schema matching legacy connector format."""
        return AD_CREATIVE_SOURCED_SCHEMA


class CustomConversions(FacebookResource):
    name: ClassVar[str] = ResourceName.CUSTOM_CONVERSIONS
    primary_keys: ClassVar[list[str]] = ["/id"]
    endpoint: ClassVar[str] = "customconversions"
    fields: ClassVar[list[str]] = [
        F.ID,
        F.ACCOUNT_ID,
        F.BUSINESS,
        F.CREATION_TIME,
        F.CUSTOM_EVENT_TYPE,
        F.DATA_SOURCES,
        F.DEFAULT_CONVERSION_VALUE,
        F.DESCRIPTION,
        F.EVENT_SOURCE_TYPE,
        F.FIRST_FIRED_TIME,
        F.IS_ARCHIVED,
        F.IS_UNAVAILABLE,
        F.LAST_FIRED_TIME,
        F.NAME,
        F.OFFLINE_CONVERSION_DATA_SET,
        F.RETENTION_DAYS,
        F.RULE,
    ]

    id: str


class Campaigns(FacebookResource):
    name: ClassVar[str] = ResourceName.CAMPAIGNS
    primary_keys: ClassVar[list[str]] = ["/id"]
    endpoint: ClassVar[str] = "campaigns"
    cursor_field: ClassVar[str] = CursorField.UPDATED_TIME
    entity_prefix: ClassVar[str] = ApiLevel.CAMPAIGN
    enable_deleted_filter: ClassVar[bool] = True
    fields: ClassVar[list[str]] = [
        F.ID,
        F.ACCOUNT_ID,
        F.ADLABELS,
        F.BID_STRATEGY,
        F.BUDGET_REBALANCE_FLAG,
        F.BUDGET_REMAINING,
        F.BUYING_TYPE,
        F.CREATED_TIME,
        F.DAILY_BUDGET,
        F.EFFECTIVE_STATUS,
        F.ISSUES_INFO,
        F.LIFETIME_BUDGET,
        F.NAME,
        F.OBJECTIVE,
        F.SMART_PROMOTION_TYPE,
        F.SOURCE_CAMPAIGN_ID,
        F.SPECIAL_AD_CATEGORY,
        F.SPECIAL_AD_CATEGORY_COUNTRY,
        F.SPEND_CAP,
        F.START_TIME,
        F.STOP_TIME,
        F.UPDATED_TIME,
    ]

    id: str


class AdSets(FacebookResource):
    """Facebook Ad Sets resource.

    Ad Sets define the targeting, budget, and schedule for a group of ads.
    Schema matches Airbyte source-facebook-marketing/schemas/ad_sets.json
    """

    name: ClassVar[str] = ResourceName.AD_SETS
    primary_keys: ClassVar[list[str]] = ["/id"]
    endpoint: ClassVar[str] = "adsets"
    cursor_field: ClassVar[str] = CursorField.UPDATED_TIME
    entity_prefix: ClassVar[str] = ApiLevel.ADSET
    enable_deleted_filter: ClassVar[bool] = True
    fields: ClassVar[list[str]] = [
        F.ID,
        F.ACCOUNT_ID,
        F.ADLABELS,
        F.BID_AMOUNT,
        F.BID_CONSTRAINTS,
        F.BID_INFO,
        F.BID_STRATEGY,
        F.BUDGET_REMAINING,
        F.CAMPAIGN_ID,
        F.CREATED_TIME,
        F.DAILY_BUDGET,
        F.EFFECTIVE_STATUS,
        F.END_TIME,
        F.LIFETIME_BUDGET,
        F.NAME,
        F.PROMOTED_OBJECT,
        F.START_TIME,
        F.TARGETING,
        F.UPDATED_TIME,
    ]

    id: str


class Ads(FacebookResource):
    """Facebook Ads resource.

    Ads are the individual ad units within an ad set.
    Schema matches Airbyte source-facebook-marketing/schemas/ads.json
    """

    name: ClassVar[str] = ResourceName.ADS
    primary_keys: ClassVar[list[str]] = ["/id"]
    endpoint: ClassVar[str] = "ads"
    cursor_field: ClassVar[str] = CursorField.UPDATED_TIME
    entity_prefix: ClassVar[str] = ApiLevel.AD
    enable_deleted_filter: ClassVar[bool] = True
    fields: ClassVar[list[str]] = [
        F.ID,
        F.ACCOUNT_ID,
        F.ADLABELS,
        F.ADSET_ID,
        F.BID_AMOUNT,
        F.BID_INFO,
        F.BID_TYPE,
        F.CAMPAIGN_ID,
        F.CONVERSION_SPECS,
        F.CREATED_TIME,
        F.CREATIVE,
        F.EFFECTIVE_STATUS,
        F.LAST_UPDATED_BY_APP_ID,
        F.NAME,
        F.RECOMMENDATIONS,
        F.SOURCE_AD_ID,
        F.STATUS,
        F.TARGETING,
        F.TRACKING_SPECS,
        F.UPDATED_TIME,
    ]

    id: str


class Activities(FacebookResource):
    """Facebook Activities resource.

    Activity log for account, showing changes made to campaigns, ad sets, and ads.
    Schema matches Airbyte source-facebook-marketing/schemas/activities.json

    Note: Activities do not have an 'id' field, so we use a composite primary key.
    """

    name: ClassVar[str] = ResourceName.ACTIVITIES
    endpoint: ClassVar[str] = "activities"
    cursor_field: ClassVar[str] = CursorField.EVENT_TIME
    primary_keys: ClassVar[list[str]] = [
        "/object_id",
        "/actor_id",
        "/application_id",
        "/event_time",
        "/event_type",
    ]
    fields: ClassVar[list[str]] = [
        F.ACCOUNT_ID,
        F.ACTOR_ID,
        F.ACTOR_NAME,
        F.APPLICATION_ID,
        F.APPLICATION_NAME,
        F.DATE_TIME_IN_TIMEZONE,
        F.EVENT_TIME,
        F.EVENT_TYPE,
        F.EXTRA_DATA,
        F.OBJECT_ID,
        F.OBJECT_NAME,
        F.OBJECT_TYPE,
        F.TRANSLATED_EVENT_TYPE,
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

    name: ClassVar[str] = ResourceName.IMAGES
    primary_keys: ClassVar[list[str]] = ["/id"]
    endpoint: ClassVar[str] = "adimages"
    cursor_field: ClassVar[str] = CursorField.UPDATED_TIME
    fields: ClassVar[list[str]] = [
        F.ID,
        F.ACCOUNT_ID,
        F.CREATED_TIME,
        F.CREATIVES,
        F.FILENAME,
        F.HASH,
        F.HEIGHT,
        F.IS_ASSOCIATED_CREATIVES_IN_ADGROUPS,
        F.NAME,
        F.ORIGINAL_HEIGHT,
        F.ORIGINAL_WIDTH,
        F.PERMALINK_URL,
        F.STATUS,
        F.UPDATED_TIME,
        F.URL,
        F.URL_128,
        F.WIDTH,
    ]

    id: str


class Videos(FacebookResource):
    """Facebook Ad Videos resource.

    Videos used in ad creatives.
    Schema matches Airbyte source-facebook-marketing/schemas/videos.json
    """

    name: ClassVar[str] = ResourceName.VIDEOS
    primary_keys: ClassVar[list[str]] = ["/id"]
    endpoint: ClassVar[str] = "advideos"
    cursor_field: ClassVar[str] = CursorField.UPDATED_TIME
    fields: ClassVar[list[str]] = [
        F.ID,
        F.ACCOUNT_ID,
        F.AD_BREAKS,
        F.BACKDATED_TIME,
        F.BACKDATED_TIME_GRANULARITY,
        F.CONTENT_CATEGORY,
        F.CONTENT_TAGS,
        F.CREATED_TIME,
        F.CUSTOM_LABELS,
        F.DESCRIPTION,
        F.EMBED_HTML,
        F.EMBEDDABLE,
        F.FORMAT,
        F.ICON,
        F.IS_CROSSPOST_VIDEO,
        F.IS_CROSSPOSTING_ELIGIBLE,
        F.IS_EPISODE,
        F.IS_INSTAGRAM_ELIGIBLE,
        F.LENGTH,
        F.LIVE_STATUS,
        F.PERMALINK_URL,
        F.POST_VIEWS,
        F.PREMIERE_LIVING_ROOM_STATUS,
        F.PUBLISHED,
        F.SCHEDULED_PUBLISH_TIME,
        F.SOURCE,
        F.TITLE,
        F.UNIVERSAL_VIDEO_ID,
        F.UPDATED_TIME,
        F.VIEWS,
    ]

    id: str


class FacebookInsightsResource(FacebookResource):
    level: ClassVar[ApiLevel] = ApiLevel.ACCOUNT
    breakdowns: ClassVar[list[Breakdown]] = []
    action_breakdowns: ClassVar[list[ActionBreakdown]] = []
    cursor_field: ClassVar[str] = CursorField.DATE_START

    @classmethod
    def _get_required_fields_for_level(cls) -> list[str]:
        required: list[str] = [F.ACCOUNT_ID, F.DATE_START]

        if cls.level == ApiLevel.AD:
            required.append(F.AD_ID)
        elif cls.level == ApiLevel.ADSET:
            required.append(F.ADSET_ID)
        elif cls.level == ApiLevel.CAMPAIGN:
            required.append(F.CAMPAIGN_ID)

        return required


class AdsInsights(FacebookInsightsResource):
    name: ClassVar[str] = ResourceName.ADS_INSIGHTS
    endpoint: ClassVar[str] = "insights"

    fields: ClassVar[list[str]] = [
        # Required fields
        F.ACCOUNT_ID,
        F.ACCOUNT_NAME,
        F.DATE_START,
        F.DATE_STOP,
        # Level-specific ID fields (for ad level - default)
        F.AD_ID,
        F.AD_NAME,
        # Core performance metrics
        F.IMPRESSIONS,
        F.CLICKS,
        F.SPEND,
        F.REACH,
        F.FREQUENCY,
        # Cost metrics
        F.CPM,
        F.CPP,
        F.CPC,
        F.CTR,
        F.COST_PER_UNIQUE_CLICK,
        F.COST_PER_INLINE_LINK_CLICK,
        F.COST_PER_INLINE_POST_ENGAGEMENT,
        # Engagement metrics
        F.UNIQUE_CLICKS,
        F.UNIQUE_CTR,
        F.INLINE_LINK_CLICKS,
        F.INLINE_LINK_CLICK_CTR,
        F.INLINE_POST_ENGAGEMENT,
        F.UNIQUE_INLINE_LINK_CLICKS,
        F.UNIQUE_INLINE_LINK_CLICK_CTR,
        # Conversion and action metrics (arrays)
        F.ACTIONS,
        F.CONVERSIONS,
        F.ACTION_VALUES,
        F.CONVERSION_VALUES,
        F.COST_PER_ACTION_TYPE,
        F.COST_PER_CONVERSION,
        # Video metrics (arrays)
        F.VIDEO_30_SEC_WATCHED_ACTIONS,
        F.VIDEO_P25_WATCHED_ACTIONS,
        F.VIDEO_P50_WATCHED_ACTIONS,
        F.VIDEO_P75_WATCHED_ACTIONS,
        F.VIDEO_P95_WATCHED_ACTIONS,
        F.VIDEO_P100_WATCHED_ACTIONS,
        # Additional useful metrics
        F.OBJECTIVE,
        F.OPTIMIZATION_GOAL,
        F.BUYING_TYPE,
        F.ATTRIBUTION_SETTING,
        # Quality/ranking metrics
        F.QUALITY_RANKING,
        F.ENGAGEMENT_RATE_RANKING,
        F.CONVERSION_RATE_RANKING,
    ]

    primary_keys: ClassVar[list[str]] = sorted(
        [
            "/account_id",
            "/date_start",
            "/ad_id",
        ]
    )

    level: ClassVar[ApiLevel] = ApiLevel.AD
    breakdowns: ClassVar[list[Breakdown]] = []
    action_breakdowns: ClassVar[list[ActionBreakdown]] = [
        ActionBreakdown.ACTION_TYPE,
        ActionBreakdown.ACTION_TARGET_ID,
        ActionBreakdown.ACTION_DESTINATION,
    ]
    action_attribution_windows: ClassVar[list[AttributionWindow]] = [
        AttributionWindow.ONE_DAY_CLICK,
        AttributionWindow.SEVEN_DAY_CLICK,
        AttributionWindow.TWENTY_EIGHT_DAY_CLICK,
        AttributionWindow.ONE_DAY_VIEW,
        AttributionWindow.SEVEN_DAY_VIEW,
        AttributionWindow.TWENTY_EIGHT_DAY_VIEW,
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

    _ACCOUNT_FIELDS: list[str] = [F.ACCOUNT_ID, F.ACCOUNT_NAME]
    _CAMPAIGN_FIELDS: list[str] = [F.CAMPAIGN_ID, F.CAMPAIGN_NAME]
    _ADSET_FIELDS: list[str] = [F.ADSET_ID, F.ADSET_NAME]
    _AD_FIELDS: list[str] = [F.AD_ID, F.AD_NAME]

    @property
    def level_id_field(self) -> str:
        if self.level == ApiLevel.AD:
            return F.AD_ID
        elif self.level == ApiLevel.ADSET:
            return F.ADSET_ID
        elif self.level == ApiLevel.CAMPAIGN:
            return F.CAMPAIGN_ID
        else:
            raise RuntimeError(
                f"Unexpected level {self.level} for AdsInsights stream. "
                f"Expected one of: campaign, adset, ad. "
                f"Please check your configuration."
            )

    def level_specific_fields(self) -> list[str]:
        common_fields = self._ACCOUNT_FIELDS + self._CAMPAIGN_FIELDS
        if self.level == ApiLevel.CAMPAIGN:
            return common_fields
        elif self.level == ApiLevel.ADSET:
            return common_fields + self._ADSET_FIELDS
        elif self.level == ApiLevel.AD:
            return common_fields + self._ADSET_FIELDS + self._AD_FIELDS
        else:
            raise RuntimeError(
                f"Unexpected level {self.level} for AdsInsights stream. "
                f"Expected one of: campaign, adset, ad. "
                f"Please check your configuration."
            )


class AdsInsightsAgeAndGender(AdsInsights):
    name: ClassVar[str] = ResourceName.ADS_INSIGHTS_AGE_AND_GENDER
    breakdowns: ClassVar[list[Breakdown]] = [Breakdown.AGE, Breakdown.GENDER]
    primary_keys: ClassVar[list[str]] = sorted(
        AdsInsights.primary_keys + ["/age", "/gender"]
    )
    age: str
    gender: str


class AdsInsightsCountry(AdsInsights):
    name: ClassVar[str] = ResourceName.ADS_INSIGHTS_COUNTRY
    breakdowns: ClassVar[list[Breakdown]] = [Breakdown.COUNTRY]
    primary_keys: ClassVar[list[str]] = sorted(AdsInsights.primary_keys + ["/country"])
    country: str


class AdsInsightsRegion(AdsInsights):
    name: ClassVar[str] = ResourceName.ADS_INSIGHTS_REGION
    breakdowns: ClassVar[list[Breakdown]] = [Breakdown.REGION]
    primary_keys: ClassVar[list[str]] = sorted(AdsInsights.primary_keys + ["/region"])
    region: str


class AdsInsightsDma(AdsInsights):
    name: ClassVar[str] = ResourceName.ADS_INSIGHTS_DMA
    breakdowns: ClassVar[list[Breakdown]] = [Breakdown.DMA]
    primary_keys: ClassVar[list[str]] = sorted(AdsInsights.primary_keys + ["/dma"])
    dma: str


class AdsInsightsPlatformAndDevice(AdsInsights):
    name: ClassVar[str] = ResourceName.ADS_INSIGHTS_PLATFORM_AND_DEVICE
    breakdowns: ClassVar[list[Breakdown]] = [
        Breakdown.PUBLISHER_PLATFORM,
        Breakdown.PLATFORM_POSITION,
        Breakdown.IMPRESSION_DEVICE,
    ]
    action_breakdowns: ClassVar[list[ActionBreakdown]] = [ActionBreakdown.ACTION_TYPE]
    primary_keys: ClassVar[list[str]] = sorted(
        AdsInsights.primary_keys
        + ["/publisher_platform", "/platform_position", "/impression_device"]
    )
    publisher_platform: str
    platform_position: str
    impression_device: str


class AdsInsightsActionType(AdsInsights):
    name: ClassVar[str] = ResourceName.ADS_INSIGHTS_ACTION_TYPE
    breakdowns: ClassVar[list[Breakdown]] = []
    action_breakdowns: ClassVar[list[ActionBreakdown]] = [ActionBreakdown.ACTION_TYPE]


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
    field_defs: dict[str, Any] = {}
    fields = str_to_list(config.fields) if config.fields else []
    breakdowns = str_to_list(config.breakdowns) if config.breakdowns else []
    action_breakdowns = (
        str_to_list(config.action_breakdowns) if config.action_breakdowns else []
    )

    level_id_field: str | None = None
    if config.level == ApiLevel.AD:
        level_id_field = F.AD_ID
    elif config.level == ApiLevel.ADSET:
        level_id_field = F.ADSET_ID
    elif config.level == ApiLevel.CAMPAIGN:
        level_id_field = F.CAMPAIGN_ID

    # Base required fields that must always be present
    base_required_fields: set[str] = {F.ACCOUNT_ID, F.DATE_START}
    if level_id_field:
        base_required_fields.add(level_id_field)

    # Build primary keys: base fields + breakdown fields
    primary_keys = ["/account_id", "/date_start"]
    if level_id_field:
        primary_keys.append(f"/{level_id_field}")
    primary_keys.extend([f"/{b}" for b in breakdowns])
    primary_keys = sorted(set(primary_keys))

    for pk in primary_keys:
        field_name_str = pk.lstrip("/")

        try:
            field = F(field_name_str)
        except ValueError:
            raise ValidationError(
                [f"Field '{field_name_str}' is not a valid Ads Insights field."]
            )

        field_type = get_field_type(field, "ads_insights")
        if field_type is None:
            field_type = str

        # Only base required fields are mandatory, breakdowns are optional
        if field_name_str in base_required_fields:
            field_defs[field_name_str] = (field_type, ...)
        else:
            # Breakdown fields are optional - Facebook may not return them
            # For nested schemas (dict instances), we can't use | None directly,
            # so we handle it separately
            if isinstance(field_type, dict):
                # For nested typed schemas, the field can be the nested dict or None
                field_defs[field_name_str] = (dict | None, None)
            elif field_type is dict:
                # For untyped dicts, the field can be dict or None
                field_defs[field_name_str] = (dict | None, None)
            else:
                # For simple types, use | None
                field_defs[field_name_str] = (field_type | None, None)

    camel_case_name = "".join(word.capitalize() for word in config.name.split("_"))
    model = create_model(
        camel_case_name,
        __base__=(AdsInsights,),
        **field_defs,
    )

    # Set ClassVars for the model
    setattr(model, "fields", fields)
    setattr(model, "primary_keys", primary_keys)
    setattr(model, "name", "custom" + config.name)
    setattr(model, "level", config.level)
    setattr(model, "breakdowns", breakdowns)
    setattr(model, "action_breakdowns", action_breakdowns)

    return model
