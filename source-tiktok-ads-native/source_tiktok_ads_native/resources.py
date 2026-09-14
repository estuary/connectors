from datetime import datetime, timedelta, UTC
import functools
import json
from logging import Logger
from typing import Any

from estuary_cdk.flow import AccessToken, CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.capture.common import BaseDocument
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError

from pydantic import ValidationError as ModelValidationError

from .api import (
    ADVERTISER_INFO_BATCH_SIZE,
    AdvertiserRegistry,
    resolve_base_url,
    TikTokApiError,
    backfill_ad_groups,
    backfill_ads,
    backfill_campaigns,
    fetch_ad_groups,
    fetch_ads,
    fetch_advertisers,
    fetch_campaigns,
    fetch_images,
    fetch_videos,
    snapshot_advertisers,
    snapshot_locations,
)
from .default_reports import DEFAULT_REPORTS
from .models import (
    Ad,
    AdGroup,
    AUDIENCE_DIMENSIONS,
    Campaign,
    DataLevel,
    EndpointConfig,
    Granularity,
    Image,
    MAX_METRICS_PER_REQUEST,
    OAUTH2_SPEC,
    Report,
    ReportDocument,
    ReportType,
    ResourceConfig,
    ResourceState,
    TIME_INCOMPATIBLE_DIMENSIONS,
    TimestampedResource,
    Video,
    create_report_doc_model,
)
from .reports import backfill_report, fetch_report


# TikTok expects the bare token under its own header rather than `Authorization: Bearer`.
AUTHORIZATION_HEADER = "Access-Token"

REPORT_INTERVAL = timedelta(hours=12)
ENTITY_INTERVAL = timedelta(minutes=30)
SNAPSHOT_INTERVAL = timedelta(hours=12)

DOCS_URL = "https://go.estuary.dev/source-tiktok-ads-native"


def _token_source(config: EndpointConfig) -> TokenSource:
    return TokenSource(
        oauth_spec=OAUTH2_SPEC,
        credentials=config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
    )


def _registry(
    http: HTTPMixin, config: EndpointConfig, base_url: str
) -> AdvertiserRegistry:
    """Build the advertiser registry, supplying app credentials when OAuth provides them."""
    if isinstance(config.credentials, AccessToken):
        app_id, secret = None, None
    else:
        app_id = config.credentials.client_id
        secret = config.credentials.client_secret

    return AdvertiserRegistry(http, base_url, config.advertiser_ids, app_id, secret)


# --- Report configuration ---------------------------------------------------------------


def _validate_report(report: Report) -> list[str]:
    """Check one report against TikTok's dimension and metric rules.

    TikTok rejects an illegal combination with a generic `40002` whose message is the only
    way to tell what went wrong, so these are caught at configuration time instead.
    """
    errors: list[str] = []
    name = report.name

    if not report.metrics:
        errors.append(f'Report "{name}" requests no metrics.')
    elif len(report.metrics) > MAX_METRICS_PER_REQUEST:
        errors.append(
            f'Report "{name}" requests {len(report.metrics)} metrics. TikTok accepts at '
            f"most {MAX_METRICS_PER_REQUEST} per request."
        )

    breakdowns = set(report.dimensions)

    if report.id_dimension in breakdowns:
        errors.append(
            f'Report "{name}" lists "{report.id_dimension}" as a dimension, but that is '
            f'already implied by the report level "{report.data_level.value}".'
        )

    if report.time_dimension in breakdowns:
        errors.append(
            f'Report "{name}" lists "{report.time_dimension}" as a dimension, but that is '
            f'already implied by the "{report.granularity.value}" aggregation.'
        )

    audience_breakdowns = breakdowns & AUDIENCE_DIMENSIONS
    unknown = breakdowns - AUDIENCE_DIMENSIONS

    if unknown:
        errors.append(
            f'Report "{name}" requests unsupported dimensions: {sorted(unknown)}.'
        )

    if report.report_type is ReportType.AUDIENCE:
        # TikTok requires exactly one audience dimension, and permits age with gender as its
        # only pairing.
        allowed_pair = audience_breakdowns == {"age", "gender"}
        if not audience_breakdowns:
            errors.append(
                f'Report "{name}" is an audience report but requests no audience dimension.'
            )
        elif len(audience_breakdowns) > 1 and not allowed_pair:
            errors.append(
                f'Report "{name}" requests {sorted(audience_breakdowns)}. An audience report '
                'accepts one audience dimension, except that "age" and "gender" may be paired.'
            )
    elif len(breakdowns) > 1:
        errors.append(
            f'Report "{name}" requests {sorted(breakdowns)}. A basic report accepts at most '
            "one breakdown dimension alongside its level and aggregation."
        )

    time_incompatible = breakdowns & TIME_INCOMPATIBLE_DIMENSIONS
    if time_incompatible and report.granularity is not Granularity.LIFETIME:
        errors.append(
            f'Report "{name}" combines {sorted(time_incompatible)} with '
            f'"{report.granularity.value}" aggregation. TikTok serves no time series for '
            "those dimensions, so they require lifetime aggregation."
        )

    if "country_code" in breakdowns and report.granularity is Granularity.HOURLY:
        errors.append(
            f'Report "{name}" combines "country_code" with hourly aggregation, which TikTok '
            "rejects. Use daily or lifetime aggregation instead."
        )

    if "ad_type" in breakdowns and report.data_level is DataLevel.AUCTION_ADVERTISER:
        errors.append(
            f'Report "{name}" combines "ad_type" with the advertiser report level, which '
            "TikTok rejects."
        )

    return errors


def _parse_reports(config: EndpointConfig) -> list[Report]:
    """Build the full report list, raising a ValidationError describing every problem."""
    entries: list[dict[str, Any]] = list(DEFAULT_REPORTS)

    if config.custom_reports:
        try:
            custom = json.loads(config.custom_reports)
        except json.JSONDecodeError:
            raise ValidationError(["Custom reports input is not valid JSON."])

        if not isinstance(custom, list):
            raise ValidationError(["Custom reports JSON input is not an array."])

        entries.extend(custom)

    errors: list[str] = []
    reports: list[Report] = []
    seen: set[str] = set()

    for entry in entries:
        if not isinstance(entry, dict):
            raise ValidationError(
                ["Custom reports JSON input array must only contain objects."]
            )

        try:
            report = Report.model_validate(entry)
        except ModelValidationError:
            name = entry.get("name", "UNKNOWN NAME")
            errors.append(
                f'Could not read custom report "{name}". Ensure it follows the format '
                f"described in this connector's documentation: {DOCS_URL}"
            )
            continue

        if report.name in seen:
            errors.append(
                f'Report name "{report.name}" is used more than once. Report names become '
                "stream names and must be unique."
            )
            continue

        seen.add(report.name)
        errors.extend(_validate_report(report))
        reports.append(report)

    if errors:
        raise ValidationError(errors)

    return reports


# --- Resources ----------------------------------------------------------------------------


def _incremental_resource(
    model: type[TimestampedResource],
    fetch_changes,
    fetch_page,
    registry: AdvertiserRegistry,
    http: HTTPMixin,
    base_url: str,
    start_date: datetime,
) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_changes, http, base_url, registry),
            fetch_page=functools.partial(
                fetch_page, http, base_url, registry, start_date
            ),
        )

    started_at = datetime.now(tz=UTC)

    return common.Resource(
        name=model.NAME,
        key=[model.PRIMARY_KEY],
        model=model,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=model.NAME, interval=ENTITY_INTERVAL),
        schema_inference=True,
    )


def _creative_asset_resource(
    model: type[Video] | type[Image],
    fetch_changes,
    registry: AdvertiserRegistry,
    http: HTTPMixin,
    base_url: str,
    start_date: datetime,
) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_changes, http, base_url, registry),
        )

    return common.Resource(
        name=model.NAME,
        key=[model.PRIMARY_KEY],
        model=model,
        open=open,
        # Seeded at the start date rather than now: the first sweep walks back through every
        # reachable asset, which is what stands in for a backfill here.
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=start_date),
        ),
        initial_config=ResourceConfig(name=model.NAME, interval=ENTITY_INTERVAL),
        schema_inference=True,
    )


def _snapshot_resource(
    name: str,
    fetch_snapshot,
    registry: AdvertiserRegistry,
    http: HTTPMixin,
    base_url: str,
) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(fetch_snapshot, http, base_url, registry),
            tombstone=BaseDocument(_meta=BaseDocument.Meta(op="d")),
        )

    return common.Resource(
        name=name,
        # Snapshot deletions are addressed positionally, so the row id is the key and the
        # document schema must require nothing the tombstone cannot supply.
        key=["/_meta/row_id"],
        model=BaseDocument,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name=name, interval=SNAPSHOT_INTERVAL),
        schema_inference=True,
    )


def _report_resources(
    http: HTTPMixin,
    base_url: str,
    config: EndpointConfig,
    registry: AdvertiserRegistry,
    reports: list[Report],
) -> list[common.Resource]:
    def open(
        report: Report,
        doc_model: type[ReportDocument],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_report,
                http,
                base_url,
                report,
                doc_model,
                registry,
                config.start_date,
                config.advanced.lookback_window_days,
            ),
            fetch_page=functools.partial(
                backfill_report,
                http,
                base_url,
                report,
                doc_model,
                registry,
                config.start_date,
            ),
        )

    started_at = datetime.now(tz=UTC)
    resources: list[common.Resource] = []

    for report in reports:
        doc_model = create_report_doc_model(report)

        resources.append(
            common.Resource(
                name=report.name,
                # The advertiser is part of the key because TikTok does not return it, and
                # every dimension is, because together they identify one report row. An
                # advertiser-level report already carries it as its id dimension.
                key=["/advertiser_id"]
                + [
                    f"/{dimension}"
                    for dimension in report.all_dimensions()
                    if dimension != "advertiser_id"
                ],
                model=doc_model,
                open=functools.partial(open, report, doc_model),
                initial_state=ResourceState(
                    inc=ResourceState.Incremental(cursor=started_at),
                    backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
                ),
                initial_config=ResourceConfig(
                    name=report.name, interval=REPORT_INTERVAL
                ),
                schema_inference=True,
            )
        )

    return resources


# --- Entry points -------------------------------------------------------------------------


async def resolve_advertiser_ids(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[str]:
    """Determine which advertiser accounts to capture.

    An explicit list always wins. Otherwise the accounts are discovered from the API, which
    is only possible under OAuth: `/oauth2/advertiser/get/` authenticates with the app id and
    secret, and an access token on its own carries neither.
    """
    if config.advertiser_ids:
        return config.advertiser_ids

    if isinstance(config.credentials, AccessToken):
        raise ValidationError(
            [
                "Advertiser IDs are required when authenticating with an access token. "
                "Discovering them automatically needs the app id and secret that only OAuth "
                "provides. Please list the advertiser accounts to capture, or switch to OAuth."
            ]
        )

    base_url = resolve_base_url(config.advanced.is_sandbox)

    return await _registry(http, config, base_url).ids(log)


async def validate_credentials(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> None:
    http.token_source = _token_source(config)

    # Report configuration is checked first: it needs no network access, and a bad report
    # spec should be reported even when the credentials are also wrong.
    _parse_reports(config)

    try:
        advertiser_ids = await resolve_advertiser_ids(log, http, config)

        if not advertiser_ids:
            raise ValidationError(
                [
                    "These credentials do not grant access to any advertiser accounts. "
                    "Please confirm the TikTok account has been authorized for this app."
                ]
            )

        # Reading account details proves the token authenticates and that the configured
        # advertisers are actually readable, which listing authorized ids alone does not.
        await fetch_advertisers(
            http,
            resolve_base_url(config.advanced.is_sandbox),
            advertiser_ids[:ADVERTISER_INFO_BATCH_SIZE],
            log,
        )
    except TikTokApiError as err:
        if err.is_auth_error:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err}"
        elif err.is_permission_error:
            msg = f"The provided credentials lack permission to read these advertiser accounts.\n\n{err}"
        elif err.is_advertiser_gone:
            msg = f"A configured advertiser account does not exist or has been deleted.\n\n{err}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err}"

        raise ValidationError([msg])
    except HTTPError as err:
        raise ValidationError(
            [f"Encountered error validating credentials.\n\n{err.message}"]
        )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes.

    No network call happens here, so discovery works without reaching TikTok; the advertiser
    accounts are resolved lazily when a stream first reads.
    """
    http.token_source = _token_source(config)

    base_url = resolve_base_url(config.advanced.is_sandbox)
    registry = _registry(http, config, base_url)
    reports = _parse_reports(config)

    return [
        _snapshot_resource("advertisers", snapshot_advertisers, registry, http, base_url),
        _snapshot_resource("locations", snapshot_locations, registry, http, base_url),
        _incremental_resource(
            Campaign,
            fetch_campaigns,
            backfill_campaigns,
            registry,
            http,
            base_url,
            config.start_date,
        ),
        _incremental_resource(
            AdGroup,
            fetch_ad_groups,
            backfill_ad_groups,
            registry,
            http,
            base_url,
            config.start_date,
        ),
        _incremental_resource(
            Ad, fetch_ads, backfill_ads, registry, http, base_url, config.start_date
        ),
        _creative_asset_resource(
            Video, fetch_videos, registry, http, base_url, config.start_date
        ),
        _creative_asset_resource(
            Image, fetch_images, registry, http, base_url, config.start_date
        ),
        *_report_resources(http, base_url, config, registry, reports),
    ]
