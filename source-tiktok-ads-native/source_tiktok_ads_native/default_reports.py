"""The report streams the connector exposes out of the box.

These mirror the prebuilt report tables Fivetran's TikTok Ads connector delivers, so a
migrating customer finds the same grains. Reservation-level reports are deliberately absent:
TikTok removed the Reservation report type from the API in 2026, and TikTok's guidance is to
read auction reports, which cover reservation ad accounts too.

Each entry is validated against `models.Report`, so the shapes here are checked at startup
rather than trusted.
"""

# Metric bundles. TikTok composes reports from named groups rather than per-report field
# lists, and these follow the groups Fivetran's prebuilt tables carry.
AUCTION_ADS_BASIC_DATA = [
    "spend",
    "cpc",
    "cpm",
    "impressions",
    "clicks",
    "ctr",
    "reach",
    "cost_per_1000_reached",
    "frequency",
    "conversion",
    "cost_per_conversion",
    "conversion_rate",
    "conversion_rate_v2",
    "real_time_conversion",
    "real_time_cost_per_conversion",
    "real_time_conversion_rate",
    "real_time_conversion_rate_v2",
    "result",
    "cost_per_result",
    "result_rate",
    "real_time_result",
    "real_time_cost_per_result",
    "real_time_result_rate",
    "secondary_goal_result",
    "cost_per_secondary_goal_result",
    "secondary_goal_result_rate",
]

VIDEO_PLAY_DATA = [
    "average_video_play",
    "average_video_play_per_user",
    "video_play_actions",
    "video_watched_2s",
    "video_watched_6s",
    "video_views_p25",
    "video_views_p50",
    "video_views_p75",
    "video_views_p100",
]

ENGAGEMENT_DATA = [
    "comments",
    "likes",
    "shares",
    "follows",
    "profile_visits",
    "profile_visits_rate",
]

# Audience reports support a narrower set than basic reports; TikTok rejects the request
# outright when an unsupported metric is asked for at this report type.
AUDIENCE_BASIC_DATA = [
    "spend",
    "impressions",
    "clicks",
    "cpc",
    "cpm",
    "ctr",
    "reach",
    "frequency",
    "conversion",
    "cost_per_conversion",
    "conversion_rate",
    "conversion_rate_v2",
    "real_time_conversion",
    "real_time_cost_per_conversion",
    "real_time_conversion_rate",
    "real_time_conversion_rate_v2",
    "result",
    "cost_per_result",
    "result_rate",
    "real_time_result",
    "real_time_cost_per_result",
    "real_time_result_rate",
]

BASIC_METRICS = AUCTION_ADS_BASIC_DATA + VIDEO_PLAY_DATA + ENGAGEMENT_DATA

# GMV reports carry only spend figures.
GMV_METRICS = ["spend", "billed_cost"]

_BASIC_LEVELS = [
    ("campaign", "AUCTION_CAMPAIGN"),
    ("adgroup", "AUCTION_ADGROUP"),
    ("ad", "AUCTION_AD"),
]

_BASIC_GRANULARITIES = ["daily", "hourly", "lifetime"]

# Audience breakdowns exist at campaign and ad level only, matching Fivetran. TikTok's API
# would also serve an ad-group-level audience report, which is left to a custom report.
_AUDIENCE_LEVELS = [
    ("campaign", "AUCTION_CAMPAIGN"),
    ("ad", "AUCTION_AD"),
]

_AUDIENCE_BREAKDOWNS = [
    ("age_gender", ["age", "gender"]),
    ("country", ["country_code"]),
    ("language", ["language"]),
    ("platform", ["platform"]),
]


def _basic_reports() -> list[dict]:
    return [
        {
            "name": f"{label}_report_{granularity}",
            "report_type": "BASIC",
            "data_level": data_level,
            "granularity": granularity,
            "dimensions": [],
            "metrics": BASIC_METRICS,
        }
        for label, data_level in _BASIC_LEVELS
        for granularity in _BASIC_GRANULARITIES
    ]


def _audience_reports() -> list[dict]:
    return [
        {
            "name": f"{label}_{breakdown}_report",
            "report_type": "AUDIENCE",
            "data_level": data_level,
            "granularity": "daily",
            "dimensions": dimensions,
            "metrics": AUDIENCE_BASIC_DATA,
        }
        for label, data_level in _AUDIENCE_LEVELS
        for breakdown, dimensions in _AUDIENCE_BREAKDOWNS
    ]


def _gmv_reports() -> list[dict]:
    return [
        {
            "name": "gmv_advertiser_country_report_daily",
            "report_type": "TT_SHOP",
            "data_level": "AUCTION_ADVERTISER",
            "granularity": "daily",
            "dimensions": ["country_code"],
            "metrics": GMV_METRICS,
        },
        {
            "name": "gmv_campaign_country_report_daily",
            "report_type": "TT_SHOP",
            "data_level": "AUCTION_CAMPAIGN",
            "granularity": "daily",
            "dimensions": ["country_code"],
            "metrics": GMV_METRICS,
        },
    ]


DEFAULT_REPORTS: list[dict] = _basic_reports() + _audience_reports() + _gmv_reports()
