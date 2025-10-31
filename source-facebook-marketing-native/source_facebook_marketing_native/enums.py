from enum import StrEnum


class ApiLevel(StrEnum):
    """API levels for insights queries."""

    AD = "ad"
    ADSET = "adset"
    CAMPAIGN = "campaign"
    ACCOUNT = "account"


class CursorField(StrEnum):
    """Cursor field names used for incremental sync."""

    UPDATED_TIME = "updated_time"
    EVENT_TIME = "event_time"
    DATE_START = "date_start"


class ResourceName(StrEnum):
    """Resource names for Facebook Marketing API."""

    AD_ACCOUNT = "ad_account"
    AD_CREATIVES = "ad_creatives"
    CUSTOM_CONVERSIONS = "custom_conversions"
    CAMPAIGNS = "campaigns"
    AD_SETS = "ad_sets"
    ADS = "ads"
    ACTIVITIES = "activities"
    IMAGES = "images"
    VIDEOS = "videos"
    ADS_INSIGHTS = "ads_insights"
    ADS_INSIGHTS_AGE_AND_GENDER = "ads_insights_age_and_gender"
    ADS_INSIGHTS_COUNTRY = "ads_insights_country"
    ADS_INSIGHTS_REGION = "ads_insights_region"
    ADS_INSIGHTS_DMA = "ads_insights_dma"
    ADS_INSIGHTS_PLATFORM_AND_DEVICE = "ads_insights_platform_and_device"
    ADS_INSIGHTS_ACTION_TYPE = "ads_insights_action_type"


class AttributionWindow(StrEnum):
    """Attribution windows for action metrics (action_attribution_windows parameter)."""

    ONE_DAY_CLICK = "1d_click"
    ONE_DAY_EV = "1d_ev"
    ONE_DAY_VIEW = "1d_view"
    SEVEN_DAY_CLICK = "7d_click"
    SEVEN_DAY_VIEW = "7d_view"
    SEVEN_DAY_VIEW_ALL_CONVERSIONS = "7d_view_all_conversions"
    SEVEN_DAY_VIEW_FIRST_CONVERSION = "7d_view_first_conversion"
    TWENTY_EIGHT_DAY_CLICK = "28d_click"
    TWENTY_EIGHT_DAY_VIEW = "28d_view"
    TWENTY_EIGHT_DAY_VIEW_ALL_CONVERSIONS = "28d_view_all_conversions"
    TWENTY_EIGHT_DAY_VIEW_FIRST_CONVERSION = "28d_view_first_conversion"
    DDA = "dda"
    DEFAULT = "default"
    SKAN_CLICK = "skan_click"
    SKAN_CLICK_SECOND_POSTBACK = "skan_click_second_postback"
    SKAN_CLICK_THIRD_POSTBACK = "skan_click_third_postback"
    SKAN_VIEW = "skan_view"
    SKAN_VIEW_SECOND_POSTBACK = "skan_view_second_postback"
    SKAN_VIEW_THIRD_POSTBACK = "skan_view_third_postback"


class DeliveryStatus(StrEnum):
    """Delivery status values for ads and campaigns."""

    ACTIVE = "active"
    ARCHIVED = "archived"
    COMPLETED = "completed"
    LIMITED = "limited"
    NOT_DELIVERING = "not_delivering"
    DELETED = "deleted"
    NOT_PUBLISHED = "not_published"
    PENDING_REVIEW = "pending_review"
    PERMANENTLY_DELETED = "permanently_deleted"
    RECENTLY_COMPLETED = "recently_completed"
    RECENTLY_REJECTED = "recently_rejected"
    REJECTED = "rejected"
    SCHEDULED = "scheduled"
    INACTIVE = "inactive"


class Breakdown(StrEnum):
    """Valid breakdown parameters for insights queries."""

    AD_EXTENSION_DOMAIN = "ad_extension_domain"
    AD_EXTENSION_URL = "ad_extension_url"
    AD_FORMAT_ASSET = "ad_format_asset"
    AGE = "age"
    APP_ID = "app_id"
    BODY_ASSET = "body_asset"
    BREAKDOWN_AD_OBJECTIVE = "breakdown_ad_objective"
    BREAKDOWN_REPORTING_AD_ID = "breakdown_reporting_ad_id"
    CALL_TO_ACTION_ASSET = "call_to_action_asset"
    COARSE_CONVERSION_VALUE = "coarse_conversion_value"
    COMSCORE_MARKET = "comscore_market"
    COMSCORE_MARKET_CODE = "comscore_market_code"
    CONVERSION_DESTINATION = "conversion_destination"
    COUNTRY = "country"
    CREATIVE_RELAXATION_ASSET_TYPE = "creative_relaxation_asset_type"
    DESCRIPTION_ASSET = "description_asset"
    DEVICE_PLATFORM = "device_platform"
    DMA = "dma"
    FIDELITY_TYPE = "fidelity_type"
    FLEXIBLE_FORMAT_ASSET_TYPE = "flexible_format_asset_type"
    FREQUENCY_VALUE = "frequency_value"
    GEN_AI_ASSET_TYPE = "gen_ai_asset_type"
    GENDER = "gender"
    HOURLY_STATS_AGGREGATED_BY_ADVERTISER_TIME_ZONE = "hourly_stats_aggregated_by_advertiser_time_zone"
    HOURLY_STATS_AGGREGATED_BY_AUDIENCE_TIME_ZONE = "hourly_stats_aggregated_by_audience_time_zone"
    HSID = "hsid"
    IMAGE_ASSET = "image_asset"
    IMPRESSION_DEVICE = "impression_device"
    IMPRESSION_VIEW_TIME_ADVERTISER_HOUR_V2 = "impression_view_time_advertiser_hour_v2"
    IS_AUTO_ADVANCE = "is_auto_advance"
    IS_CONVERSION_ID_MODELED = "is_conversion_id_modeled"
    IS_RENDERED_AS_DELAYED_SKIP_AD = "is_rendered_as_delayed_skip_ad"
    LANDING_DESTINATION = "landing_destination"
    LINK_URL_ASSET = "link_url_asset"
    MARKETING_MESSAGES_BTN_NAME = "marketing_messages_btn_name"
    MDSA_LANDING_DESTINATION = "mdsa_landing_destination"
    MEDIA_ASSET_URL = "media_asset_url"
    MEDIA_CREATOR = "media_creator"
    MEDIA_DESTINATION_URL = "media_destination_url"
    MEDIA_FORMAT = "media_format"
    MEDIA_ORIGIN_URL = "media_origin_url"
    MEDIA_TEXT_CONTENT = "media_text_content"
    MEDIA_TYPE = "media_type"
    MMM = "mmm"
    PLACE_PAGE_ID = "place_page_id"
    PLATFORM_POSITION = "platform_position"
    POSTBACK_SEQUENCE_INDEX = "postback_sequence_index"
    PRODUCT_ID = "product_id"
    PUBLISHER_PLATFORM = "publisher_platform"
    REDOWNLOAD = "redownload"
    REGION = "region"
    SIGNAL_SOURCE_BUCKET = "signal_source_bucket"
    SKAN_CAMPAIGN_ID = "skan_campaign_id"
    SKAN_CONVERSION_ID = "skan_conversion_id"
    SKAN_VERSION = "skan_version"
    SOT_ATTRIBUTION_MODEL_TYPE = "sot_attribution_model_type"
    SOT_ATTRIBUTION_WINDOW = "sot_attribution_window"
    SOT_CHANNEL = "sot_channel"
    SOT_EVENT_TYPE = "sot_event_type"
    SOT_SOURCE = "sot_source"
    STANDARD_EVENT_CONTENT_TYPE = "standard_event_content_type"
    TITLE_ASSET = "title_asset"
    USER_PERSONA_ID = "user_persona_id"
    USER_PERSONA_NAME = "user_persona_name"
    VIDEO_ASSET = "video_asset"


class ActionBreakdown(StrEnum):
    """Valid action breakdown parameters for insights queries (action_breakdowns parameter)."""

    ACTION_CANVAS_COMPONENT_NAME = "action_canvas_component_name"
    ACTION_CAROUSEL_CARD_ID = "action_carousel_card_id"
    ACTION_CAROUSEL_CARD_NAME = "action_carousel_card_name"
    ACTION_DESTINATION = "action_destination"
    ACTION_DEVICE = "action_device"
    ACTION_REACTION = "action_reaction"
    ACTION_TARGET_ID = "action_target_id"
    ACTION_TYPE = "action_type"
    ACTION_VIDEO_SOUND = "action_video_sound"
    ACTION_VIDEO_TYPE = "action_video_type"
    CONVERSION_DESTINATION = "conversion_destination"
    MATCHED_PERSONA_ID = "matched_persona_id"
    MATCHED_PERSONA_NAME = "matched_persona_name"
    SIGNAL_SOURCE_BUCKET = "signal_source_bucket"
    STANDARD_EVENT_CONTENT_TYPE = "standard_event_content_type"


class Field(StrEnum):
    # Core identity and account fields
    ACCOUNT_CURRENCY = "account_currency"
    ACCOUNT_ID = "account_id"
    ACCOUNT_NAME = "account_name"
    ACCOUNT_STATUS = "account_status"
    ACTOR_ID = "actor_id"
    ACTOR_NAME = "actor_name"
    AD_BREAKS = "ad_breaks"
    AD_ID = "ad_id"
    AD_NAME = "ad_name"
    ADSET_END = "adset_end"
    ADSET_ID = "adset_id"
    ADSET_NAME = "adset_name"
    ADSET_START = "adset_start"
    AGE = "age"
    AGE_TARGETING = "age_targeting"
    APPLICATION_ID = "application_id"
    APPLICATION_NAME = "application_name"
    CAMPAIGN_ID = "campaign_id"
    CAMPAIGN_NAME = "campaign_name"

    # Action and conversion fields
    ACTION_DESTINATION = "action_destination"
    ACTION_TARGET_ID = "action_target_id"
    ACTION_TYPE = "action_type"
    ACTION_VALUES = "action_values"
    ACTIONS = "actions"
    AD_CLICK_ACTIONS = "ad_click_actions"
    AD_IMPRESSION_ACTIONS = "ad_impression_actions"

    # Financial fields
    AMOUNT_SPENT = "amount_spent"
    ATTRIBUTION_SETTING = "attribution_setting"
    AUCTION_BID = "auction_bid"
    AUCTION_COMPETITIVENESS = "auction_competitiveness"
    AUCTION_MAX_COMPETITOR_BID = "auction_max_competitor_bid"
    AVERAGE_PURCHASES_CONVERSION_VALUE = "average_purchases_conversion_value"
    BALANCE = "balance"
    BID_AMOUNT = "bid_amount"
    BID_CONSTRAINTS = "bid_constraints"
    BID_INFO = "bid_info"
    BID_STRATEGY = "bid_strategy"
    BID_TYPE = "bid_type"
    BUDGET_REBALANCE_FLAG = "budget_rebalance_flag"
    BUDGET_REMAINING = "budget_remaining"
    BUYING_TYPE = "buying_type"

    # Business information fields
    BUSINESS = "business"
    BUSINESS_CITY = "business_city"
    BUSINESS_COUNTRY_CODE = "business_country_code"
    BUSINESS_NAME = "business_name"
    BUSINESS_STATE = "business_state"
    BUSINESS_STREET = "business_street"
    BUSINESS_STREET2 = "business_street2"
    BUSINESS_ZIP = "business_zip"

    # Capabilities and settings
    CAN_CREATE_BRAND_LIFT_STUDY = "can_create_brand_lift_study"
    CAPABILITIES = "capabilities"

    # Catalog fields
    CATALOG_SEGMENT_ACTIONS = "catalog_segment_actions"
    CATALOG_SEGMENT_VALUE = "catalog_segment_value"
    CATALOG_SEGMENT_VALUE_MOBILE_PURCHASE_ROAS = "catalog_segment_value_mobile_purchase_roas"
    CATALOG_SEGMENT_VALUE_OMNI_PURCHASE_ROAS = "catalog_segment_value_omni_purchase_roas"
    CATALOG_SEGMENT_VALUE_WEBSITE_PURCHASE_ROAS = "catalog_segment_value_website_purchase_roas"

    # Performance metrics
    CANVAS_AVG_VIEW_PERCENT = "canvas_avg_view_percent"
    CANVAS_AVG_VIEW_TIME = "canvas_avg_view_time"
    CLICKS = "clicks"
    CONVERSION_LEAD_RATE = "conversion_lead_rate"
    CONVERSION_LEADS = "conversion_leads"
    CONVERSION_RATE_RANKING = "conversion_rate_ranking"
    CONVERSION_SPECS = "conversion_specs"
    CONVERSION_VALUES = "conversion_values"
    CONVERSIONS = "conversions"

    # Converted product fields
    CONVERTED_PRODUCT_APP_CUSTOM_EVENT_FB_MOBILE_PURCHASE = "converted_product_app_custom_event_fb_mobile_purchase"
    CONVERTED_PRODUCT_APP_CUSTOM_EVENT_FB_MOBILE_PURCHASE_VALUE = "converted_product_app_custom_event_fb_mobile_purchase_value"
    CONVERTED_PRODUCT_OFFLINE_PURCHASE = "converted_product_offline_purchase"
    CONVERTED_PRODUCT_OFFLINE_PURCHASE_VALUE = "converted_product_offline_purchase_value"
    CONVERTED_PRODUCT_OMNI_PURCHASE = "converted_product_omni_purchase"
    CONVERTED_PRODUCT_OMNI_PURCHASE_VALUES = "converted_product_omni_purchase_values"
    CONVERTED_PRODUCT_QUANTITY = "converted_product_quantity"
    CONVERTED_PRODUCT_VALUE = "converted_product_value"
    CONVERTED_PRODUCT_WEBSITE_PIXEL_PURCHASE = "converted_product_website_pixel_purchase"
    CONVERTED_PRODUCT_WEBSITE_PIXEL_PURCHASE_VALUE = "converted_product_website_pixel_purchase_value"

    # Converted promoted product fields
    CONVERTED_PROMOTED_PRODUCT_APP_CUSTOM_EVENT_FB_MOBILE_PURCHASE = "converted_promoted_product_app_custom_event_fb_mobile_purchase"
    CONVERTED_PROMOTED_PRODUCT_APP_CUSTOM_EVENT_FB_MOBILE_PURCHASE_VALUE = "converted_promoted_product_app_custom_event_fb_mobile_purchase_value"
    CONVERTED_PROMOTED_PRODUCT_OFFLINE_PURCHASE = "converted_promoted_product_offline_purchase"
    CONVERTED_PROMOTED_PRODUCT_OFFLINE_PURCHASE_VALUE = "converted_promoted_product_offline_purchase_value"
    CONVERTED_PROMOTED_PRODUCT_OMNI_PURCHASE = "converted_promoted_product_omni_purchase"
    CONVERTED_PROMOTED_PRODUCT_OMNI_PURCHASE_VALUES = "converted_promoted_product_omni_purchase_values"
    CONVERTED_PROMOTED_PRODUCT_QUANTITY = "converted_promoted_product_quantity"
    CONVERTED_PROMOTED_PRODUCT_VALUE = "converted_promoted_product_value"
    CONVERTED_PROMOTED_PRODUCT_WEBSITE_PIXEL_PURCHASE = "converted_promoted_product_website_pixel_purchase"
    CONVERTED_PROMOTED_PRODUCT_WEBSITE_PIXEL_PURCHASE_VALUE = "converted_promoted_product_website_pixel_purchase_value"

    # Cost per action fields
    COST_PER_15_SEC_VIDEO_VIEW = "cost_per_15_sec_video_view"
    COST_PER_2_SEC_CONTINUOUS_VIDEO_VIEW = "cost_per_2_sec_continuous_video_view"
    COST_PER_ACTION_TYPE = "cost_per_action_type"
    COST_PER_AD_CLICK = "cost_per_ad_click"
    COST_PER_CONVERSION = "cost_per_conversion"
    COST_PER_CONVERSION_LEAD = "cost_per_conversion_lead"
    COST_PER_DDA_COUNTBY_CONVS = "cost_per_dda_countby_convs"
    COST_PER_ESTIMATED_AD_RECALLERS = "cost_per_estimated_ad_recallers"
    COST_PER_INLINE_LINK_CLICK = "cost_per_inline_link_click"
    COST_PER_INLINE_POST_ENGAGEMENT = "cost_per_inline_post_engagement"
    COST_PER_OBJECTIVE_RESULT = "cost_per_objective_result"
    COST_PER_ONE_THOUSAND_AD_IMPRESSION = "cost_per_one_thousand_ad_impression"
    COST_PER_OUTBOUND_CLICK = "cost_per_outbound_click"
    COST_PER_RESULT = "cost_per_result"
    COST_PER_THRUPLAY = "cost_per_thruplay"
    COST_PER_UNIQUE_ACTION_TYPE = "cost_per_unique_action_type"
    COST_PER_UNIQUE_CLICK = "cost_per_unique_click"
    COST_PER_UNIQUE_CONVERSION = "cost_per_unique_conversion"
    COST_PER_UNIQUE_INLINE_LINK_CLICK = "cost_per_unique_inline_link_click"
    COST_PER_UNIQUE_OUTBOUND_CLICK = "cost_per_unique_outbound_click"

    # Standard cost metrics
    CPC = "cpc"
    CPM = "cpm"
    CPP = "cpp"
    CTR = "ctr"

    # Creative fields
    ADLABELS = "adlabels"
    APPLINK_TREATMENT = "applink_treatment"
    ASSET_FEED_SPEC = "asset_feed_spec"
    BODY = "body"
    CALL_TO_ACTION_TYPE = "call_to_action_type"
    CONTENT_CATEGORY = "content_category"
    CONTENT_TAGS = "content_tags"
    CREATIVE = "creative"
    CREATIVE_MEDIA_TYPE = "creative_media_type"
    CREATIVES = "creatives"
    CURRENCY = "currency"
    CUSTOM_EVENT_TYPE = "custom_event_type"
    CUSTOM_LABELS = "custom_labels"

    # Date and time fields
    BACKDATED_TIME = "backdated_time"
    BACKDATED_TIME_GRANULARITY = "backdated_time_granularity"
    CREATED_TIME = "created_time"
    CREATION_TIME = "creation_time"
    DATE_START = "date_start"
    DATE_STOP = "date_stop"
    DATE_TIME_IN_TIMEZONE = "date_time_in_timezone"
    END_TIME = "end_time"
    EVENT_TIME = "event_time"
    FIRST_FIRED_TIME = "first_fired_time"
    LAST_FIRED_TIME = "last_fired_time"
    START_TIME = "start_time"
    STOP_TIME = "stop_time"
    UPDATED_TIME = "updated_time"
    SCHEDULED_PUBLISH_TIME = "scheduled_publish_time"

    # Budget fields
    DAILY_BUDGET = "daily_budget"
    LIFETIME_BUDGET = "lifetime_budget"
    MIN_CAMPAIGN_GROUP_SPEND_CAP = "min_campaign_group_spend_cap"
    MIN_DAILY_BUDGET = "min_daily_budget"
    SPEND_CAP = "spend_cap"

    # Data sources and conversions
    DATA_SOURCES = "data_sources"
    DDA_COUNTBY_CONVS = "dda_countby_convs"
    DDA_RESULTS = "dda_results"
    DEFAULT_CONVERSION_VALUE = "default_conversion_value"
    DESCRIPTION = "description"

    # Status and delivery fields
    DISABLE_REASON = "disable_reason"
    EFFECTIVE_INSTAGRAM_STORY_ID = "effective_instagram_story_id"
    EFFECTIVE_OBJECT_STORY_ID = "effective_object_story_id"
    EFFECTIVE_STATUS = "effective_status"
    EMBED_HTML = "embed_html"
    EMBEDDABLE = "embeddable"
    ENGAGEMENT_RATE_RANKING = "engagement_rate_ranking"
    EVENT_SOURCE_TYPE = "event_source_type"
    EVENT_TYPE = "event_type"
    EXTRA_DATA = "extra_data"

    # Extended fields
    END_ADVERTISER = "end_advertiser"
    END_ADVERTISER_NAME = "end_advertiser_name"
    EXTENDED_CREDIT_INVOICE_GROUP = "extended_credit_invoice_group"

    # Ad recall and estimation fields
    ESTIMATED_AD_RECALL_RATE = "estimated_ad_recall_rate"
    ESTIMATED_AD_RECALL_RATE_LOWER_BOUND = "estimated_ad_recall_rate_lower_bound"
    ESTIMATED_AD_RECALL_RATE_UPPER_BOUND = "estimated_ad_recall_rate_upper_bound"
    ESTIMATED_AD_RECALLERS = "estimated_ad_recallers"
    ESTIMATED_AD_RECALLERS_LOWER_BOUND = "estimated_ad_recallers_lower_bound"
    ESTIMATED_AD_RECALLERS_UPPER_BOUND = "estimated_ad_recallers_upper_bound"

    # Failed delivery and funding
    FAILED_DELIVERY_CHECKS = "failed_delivery_checks"
    FB_ENTITY = "fb_entity"
    FILENAME = "filename"
    FORMAT = "format"
    FREQUENCY = "frequency"
    FULL_VIEW_IMPRESSIONS = "full_view_impressions"
    FULL_VIEW_REACH = "full_view_reach"
    FUNDING_SOURCE = "funding_source"
    FUNDING_SOURCE_DETAILS = "funding_source_details"

    # Gender and targeting
    GENDER = "gender"
    GENDER_TARGETING = "gender_targeting"

    # Hash and height
    HASH = "hash"
    HAS_MIGRATED_PERMISSIONS = "has_migrated_permissions"
    HEIGHT = "height"
    HOURLY_STATS_AGGREGATED_BY_ADVERTISER_TIME_ZONE = "hourly_stats_aggregated_by_advertiser_time_zone"
    HOURLY_STATS_AGGREGATED_BY_AUDIENCE_TIME_ZONE = "hourly_stats_aggregated_by_audience_time_zone"

    # ID field
    ID = "id"

    # Image fields
    ICON = "icon"
    IMAGE_CROPS = "image_crops"
    IMAGE_HASH = "image_hash"
    IMAGE_URL = "image_url"

    # Impressions and engagement
    IMPRESSIONS = "impressions"
    IMPRESSION_DEVICE = "impression_device"
    INLINE_LINK_CLICK_CTR = "inline_link_click_ctr"
    INLINE_LINK_CLICKS = "inline_link_clicks"
    INLINE_POST_ENGAGEMENT = "inline_post_engagement"

    # Instagram fields
    INSTAGRAM_ACTOR_ID = "instagram_actor_id"
    INSTAGRAM_PERMALINK_URL = "instagram_permalink_url"
    INSTAGRAM_STORY_ID = "instagram_story_id"
    INSTAGRAM_UPCOMING_EVENT_REMINDERS_SET = "instagram_upcoming_event_reminders_set"

    # Instant experience fields
    INSTANT_EXPERIENCE_CLICKS_TO_OPEN = "instant_experience_clicks_to_open"
    INSTANT_EXPERIENCE_CLICKS_TO_START = "instant_experience_clicks_to_start"
    INSTANT_EXPERIENCE_OUTBOUND_CLICKS = "instant_experience_outbound_clicks"
    INTERACTIVE_COMPONENT_TAP = "interactive_component_tap"

    # IO number and flags
    IO_NUMBER = "io_number"
    IS_ARCHIVED = "is_archived"
    IS_ASSOCIATED_CREATIVES_IN_ADGROUPS = "is_associated_creatives_in_adgroups"
    IS_ATTRIBUTION_SPEC_SYSTEM_DEFAULT = "is_attribution_spec_system_default"
    IS_CROSSPOST_VIDEO = "is_crosspost_video"
    IS_CROSSPOSTING_ELIGIBLE = "is_crossposting_eligible"
    IS_DIRECT_DEALS_ENABLED = "is_direct_deals_enabled"
    IS_EPISODE = "is_episode"
    IS_IN_3DS_AUTHORIZATION_ENABLED_MARKET = "is_in_3ds_authorization_enabled_market"
    IS_INSTAGRAM_ELIGIBLE = "is_instagram_eligible"
    IS_NOTIFICATIONS_ENABLED = "is_notifications_enabled"
    IS_PERSONAL = "is_personal"
    IS_PREPAY_ACCOUNT = "is_prepay_account"
    IS_TAX_ID_REQUIRED = "is_tax_id_required"
    IS_UNAVAILABLE = "is_unavailable"
    ISSUES_INFO = "issues_info"

    # Labels and landing page
    LABELS = "labels"
    LANDING_PAGE_VIEW_ACTIONS_PER_LINK_CLICK = "landing_page_view_actions_per_link_click"
    LANDING_PAGE_VIEW_PER_LINK_CLICK = "landing_page_view_per_link_click"
    LANDING_PAGE_VIEW_PER_PURCHASE_RATE = "landing_page_view_per_purchase_rate"
    LAST_UPDATED_BY_APP_ID = "last_updated_by_app_id"
    LENGTH = "length"
    LINE_NUMBERS = "line_numbers"

    # Link fields
    LINK_OG_ID = "link_og_id"
    LINK_URL = "link_url"
    LIVE_STATUS = "live_status"
    LOCATION = "location"

    # Marketing messages fields
    MARKETING_MESSAGES_CLICK_RATE_BENCHMARK = "marketing_messages_click_rate_benchmark"
    MARKETING_MESSAGES_COST_PER_DELIVERED = "marketing_messages_cost_per_delivered"
    MARKETING_MESSAGES_COST_PER_LINK_BTN_CLICK = "marketing_messages_cost_per_link_btn_click"
    MARKETING_MESSAGES_DELIVERED = "marketing_messages_delivered"
    MARKETING_MESSAGES_DELIVERY_RATE = "marketing_messages_delivery_rate"
    MARKETING_MESSAGES_LINK_BTN_CLICK = "marketing_messages_link_btn_click"
    MARKETING_MESSAGES_LINK_BTN_CLICK_RATE = "marketing_messages_link_btn_click_rate"
    MARKETING_MESSAGES_MEDIA_VIEW_RATE = "marketing_messages_media_view_rate"
    MARKETING_MESSAGES_PHONE_CALL_BTN_CLICK_RATE = "marketing_messages_phone_call_btn_click_rate"
    MARKETING_MESSAGES_QUICK_REPLY_BTN_CLICK = "marketing_messages_quick_reply_btn_click"
    MARKETING_MESSAGES_QUICK_REPLY_BTN_CLICK_RATE = "marketing_messages_quick_reply_btn_click_rate"
    MARKETING_MESSAGES_READ = "marketing_messages_read"
    MARKETING_MESSAGES_READ_RATE = "marketing_messages_read_rate"
    MARKETING_MESSAGES_READ_RATE_BENCHMARK = "marketing_messages_read_rate_benchmark"
    MARKETING_MESSAGES_SENT = "marketing_messages_sent"
    MARKETING_MESSAGES_SPEND = "marketing_messages_spend"
    MARKETING_MESSAGES_SPEND_CURRENCY = "marketing_messages_spend_currency"
    MARKETING_MESSAGES_WEBSITE_ADD_TO_CART = "marketing_messages_website_add_to_cart"
    MARKETING_MESSAGES_WEBSITE_INITIATE_CHECKOUT = "marketing_messages_website_initiate_checkout"
    MARKETING_MESSAGES_WEBSITE_PURCHASE = "marketing_messages_website_purchase"
    MARKETING_MESSAGES_WEBSITE_PURCHASE_VALUES = "marketing_messages_website_purchase_values"

    # Media agency and mobile
    MEDIA_AGENCY = "media_agency"
    MOBILE_APP_PURCHASE_ROAS = "mobile_app_purchase_roas"

    # Name fields
    NAME = "name"

    # Object fields
    OBJECTIVE = "objective"
    OBJECTIVE_RESULT_RATE = "objective_result_rate"
    OBJECTIVE_RESULTS = "objective_results"
    OBJECT_ID = "object_id"
    OBJECT_NAME = "object_name"
    OBJECT_STORY_ID = "object_story_id"
    OBJECT_STORY_SPEC = "object_story_spec"
    OBJECT_TYPE = "object_type"
    OBJECT_URL = "object_url"

    # Offline conversion and offsite pixels
    OFFLINE_CONVERSION_DATA_SET = "offline_conversion_data_set"
    OFFSITE_PIXELS_TOS_ACCEPTED = "offsite_pixels_tos_accepted"
    ONSITE_CONVERSION_MESSAGING_DETECTED_PURCHASE_DEDUPED = "onsite_conversion_messaging_detected_purchase_deduped"
    OPTIMIZATION_GOAL = "optimization_goal"
    ORIGINAL_HEIGHT = "original_height"
    ORIGINAL_WIDTH = "original_width"

    # Outbound clicks
    OUTBOUND_CLICKS = "outbound_clicks"
    OUTBOUND_CLICKS_CTR = "outbound_clicks_ctr"

    # Owner and partner
    OWNER = "owner"
    PARTNER = "partner"

    # Permalink and place page
    PERMALINK_URL = "permalink_url"
    PLACE_PAGE_NAME = "place_page_name"
    PLATFORM_POSITION = "platform_position"
    POST_VIEWS = "post_views"
    PREMIERE_LIVING_ROOM_STATUS = "premiere_living_room_status"

    # Product fields
    PRODUCT_BRAND = "product_brand"
    PRODUCT_CATEGORY = "product_category"
    PRODUCT_CONTENT_ID = "product_content_id"
    PRODUCT_CUSTOM_LABEL_0 = "product_custom_label_0"
    PRODUCT_CUSTOM_LABEL_1 = "product_custom_label_1"
    PRODUCT_CUSTOM_LABEL_2 = "product_custom_label_2"
    PRODUCT_CUSTOM_LABEL_3 = "product_custom_label_3"
    PRODUCT_CUSTOM_LABEL_4 = "product_custom_label_4"
    PRODUCT_GROUP_CONTENT_ID = "product_group_content_id"
    PRODUCT_GROUP_RETAILER_ID = "product_group_retailer_id"
    PRODUCT_ID = "product_id"
    PRODUCT_NAME = "product_name"
    PRODUCT_RETAILER_ID = "product_retailer_id"
    PRODUCT_SET_ID = "product_set_id"

    # Promoted object
    PROMOTED_OBJECT = "promoted_object"

    # Publisher platform
    PUBLISHER_PLATFORM = "publisher_platform"
    PUBLISHED = "published"

    # Purchase fields
    PURCHASE_PER_LANDING_PAGE_VIEW = "purchase_per_landing_page_view"
    PURCHASE_ROAS = "purchase_roas"
    PURCHASES_PER_LINK_CLICK = "purchases_per_link_click"

    # Qualifying question
    QUALIFYING_QUESTION_QUALIFY_ANSWER_RATE = "qualifying_question_qualify_answer_rate"
    QUALITY_RANKING = "quality_ranking"

    # Reach and recommendations
    REACH = "reach"
    RECOMMENDATIONS = "recommendations"
    RETENTION_DAYS = "retention_days"

    # Result fields
    RESULT_RATE = "result_rate"
    RESULT_VALUES_PERFORMANCE_INDICATOR = "result_values_performance_indicator"
    RESULTS = "results"

    # RF spec and rule
    RF_SPEC = "rf_spec"
    RULE = "rule"

    # Shops
    SHOPS_ASSISTED_PURCHASES = "shops_assisted_purchases"

    # Smart promotion and social
    SMART_PROMOTION_TYPE = "smart_promotion_type"
    SOCIAL_SPEND = "social_spend"
    SOURCE = "source"
    SOURCE_AD_ID = "source_ad_id"
    SOURCE_CAMPAIGN_ID = "source_campaign_id"

    # Special ad category
    SPECIAL_AD_CATEGORY = "special_ad_category"
    SPECIAL_AD_CATEGORY_COUNTRY = "special_ad_category_country"

    # Spend
    SPEND = "spend"

    # Status
    STATUS = "status"

    # Targeting and tax
    TARGETING = "targeting"
    TAX_ID = "tax_id"
    TAX_ID_STATUS = "tax_id_status"
    TAX_ID_TYPE = "tax_id_type"

    # Template fields
    TEMPLATE_URL = "template_url"
    TEMPLATE_URL_SPEC = "template_url_spec"

    # Thumbnail fields
    THUMBNAIL_DATA_URL = "thumbnail_data_url"
    THUMBNAIL_URL = "thumbnail_url"

    # Timezone fields
    TIMEZONE_ID = "timezone_id"
    TIMEZONE_NAME = "timezone_name"
    TIMEZONE_OFFSET_HOURS_UTC = "timezone_offset_hours_utc"

    # Title
    TITLE = "title"

    # TOS fields
    TOS_ACCEPTED = "tos_accepted"

    # Postback fields
    TOTAL_POSTBACKS = "total_postbacks"
    TOTAL_POSTBACKS_DETAILED = "total_postbacks_detailed"
    TOTAL_POSTBACKS_DETAILED_V4 = "total_postbacks_detailed_v4"

    # Tracking fields
    TRACKING_SPECS = "tracking_specs"
    TRANSLATED_EVENT_TYPE = "translated_event_type"

    # Unique action fields
    UNIQUE_ACTIONS = "unique_actions"
    UNIQUE_CLICKS = "unique_clicks"
    UNIQUE_CONVERSIONS = "unique_conversions"
    UNIQUE_CTR = "unique_ctr"
    UNIQUE_INLINE_LINK_CLICK_CTR = "unique_inline_link_click_ctr"
    UNIQUE_INLINE_LINK_CLICKS = "unique_inline_link_clicks"
    UNIQUE_LINK_CLICKS_CTR = "unique_link_clicks_ctr"
    UNIQUE_OUTBOUND_CLICKS = "unique_outbound_clicks"
    UNIQUE_OUTBOUND_CLICKS_CTR = "unique_outbound_clicks_ctr"

    # Universal video
    UNIVERSAL_VIDEO_ID = "universal_video_id"

    # URL fields
    URL = "url"
    URL_128 = "url_128"
    URL_TAGS = "url_tags"

    # User fields
    USER_TASKS = "user_tasks"
    USER_TOS_ACCEPTED = "user_tos_accepted"

    # Video fields
    VIDEO_ID = "video_id"
    VIDEO_15_SEC_WATCHED_ACTIONS = "video_15_sec_watched_actions"
    VIDEO_30_SEC_WATCHED_ACTIONS = "video_30_sec_watched_actions"
    VIDEO_AVG_TIME_WATCHED_ACTIONS = "video_avg_time_watched_actions"
    VIDEO_CONTINUOUS_2_SEC_WATCHED_ACTIONS = "video_continuous_2_sec_watched_actions"
    VIDEO_P100_WATCHED_ACTIONS = "video_p100_watched_actions"
    VIDEO_P25_WATCHED_ACTIONS = "video_p25_watched_actions"
    VIDEO_P50_WATCHED_ACTIONS = "video_p50_watched_actions"
    VIDEO_P75_WATCHED_ACTIONS = "video_p75_watched_actions"
    VIDEO_P95_WATCHED_ACTIONS = "video_p95_watched_actions"
    VIDEO_PLAY_ACTIONS = "video_play_actions"
    VIDEO_PLAY_CURVE_ACTIONS = "video_play_curve_actions"
    VIDEO_PLAY_RETENTION_0_TO_15S_ACTIONS = "video_play_retention_0_to_15s_actions"
    VIDEO_PLAY_RETENTION_20_TO_60S_ACTIONS = "video_play_retention_20_to_60s_actions"
    VIDEO_PLAY_RETENTION_GRAPH_ACTIONS = "video_play_retention_graph_actions"
    VIDEO_THRUPLAY_WATCHED_ACTIONS = "video_thruplay_watched_actions"
    VIDEO_TIME_WATCHED_ACTIONS = "video_time_watched_actions"
    VIDEO_VIEW_PER_IMPRESSION = "video_view_per_impression"
    UNIQUE_VIDEO_CONTINUOUS_2_SEC_WATCHED_ACTIONS = "unique_video_continuous_2_sec_watched_actions"
    UNIQUE_VIDEO_VIEW_15_SEC = "unique_video_view_15_sec"

    # Views and width
    VIEWS = "views"
    WEBSITE_CTR = "website_ctr"
    WEBSITE_PURCHASE_ROAS = "website_purchase_roas"
    WIDTH = "width"
    WISH_BID = "wish_bid"

    # Breakdown dimension fields (used in breakdowns parameter)
    COUNTRY = "country"
    REGION = "region"
    DMA = "dma"
    DEVICE_PLATFORM = "device_platform"
