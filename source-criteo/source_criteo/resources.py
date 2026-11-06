import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin
from pydantic import ValidationError as PydanticValidationError

from .api import (
    backfill_report,
    fetch_report,
    floor_to_day,
    probe_report,
    resolve_advertiser_ids,
    snapshot_ad_sets,
    snapshot_advertisers,
    snapshot_audiences,
    snapshot_campaigns,
)
from .models import (
    OAUTH2_SPEC,
    CriteoTokenSource,
    EndpointConfig,
    ReportConfig,
    ResourceConfig,
    ResourceState,
    report_document_model,
    report_key,
    report_stream_name,
)


# How many accessible advertiser IDs to name when rejecting an unknown one.
# Enough to fix a typo against a normal portfolio, short of dumping a large one
# into an error message.
ADVERTISER_ID_SUGGESTIONS = 20


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured client credentials reach the Criteo API."""
    http.token_source = CriteoTokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    try:
        # An empty list asks for the whole portfolio regardless of what the user
        # configured, so this exercises the credentials rather than a filter.
        await resolve_advertiser_ids(http, [], log)
    except HTTPError as err:
        if err.code in (400, 401, 403):
            msg = (
                "Could not authenticate with the provided Criteo client credentials. "
                f"Please confirm the client ID and secret are correct and have Marketing Solutions access.\n\n{err.message}"
            )
        else:
            msg = f"Encountered error validating Criteo credentials.\n\n{err.message}"

        raise ValidationError([msg]) from err
    except RuntimeError as err:
        # A 200 carrying `errors`, which is how a client that authenticates but
        # lacks a Marketing Solutions grant presents.
        raise ValidationError([str(err)]) from err


async def validate_advertiser_ids(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[str]:
    """Report configured advertiser IDs the API client cannot see.

    Criteo's search endpoints return nothing for an unknown advertiser rather
    than complaining, so a typo would otherwise surface as an empty capture.

    Returns user-facing messages rather than raising, so that `Connector.validate`
    can report every problem it finds in one go.
    """
    if not config.advertiser_ids:
        return []

    # Free: `validate_credentials` has already primed the portfolio cache.
    portfolio = await resolve_advertiser_ids(http, [], log)

    unknown = [
        advertiser_id
        for advertiser_id in config.advertiser_ids
        if advertiser_id not in set(portfolio)
    ]
    if not unknown:
        return []

    accessible = ", ".join(sorted(portfolio)[:ADVERTISER_ID_SUGGESTIONS]) or "none"
    if len(portfolio) > ADVERTISER_ID_SUGGESTIONS:
        accessible += f", and {len(portfolio) - ADVERTISER_ID_SUGGESTIONS} more"

    return [
        f"Advertiser ID(s) not in this API client's portfolio: {', '.join(unknown)}. "
        f"Accessible advertiser IDs: {accessible}."
    ]


async def validate_reports(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[str]:
    """Ask Criteo to serve one day of each configured report.

    The report fields we do not check locally — dimensions, metrics, currency,
    timezone, and whether Criteo will serve that combination together — are all
    Criteo's vocabulary, and any list hard-coded here would eventually reject
    something valid. Asking the API instead keeps it authoritative and surfaces
    the problem in Criteo's own words, at publish time rather than mid-capture.

    Every report is probed even after one fails, and messages are returned rather
    than raised, so a user with several broken reports learns about all of them
    at once instead of one per publish attempt.
    """
    if not config.reports:
        return []

    portfolio = await resolve_advertiser_ids(http, [], log)

    # Probed against advertisers known to exist. A bad `advertiser_ids` entry is
    # already reported by `validate_advertiser_ids`, and reusing it here would
    # make every report fail too, burying the report's own problems under
    # derivative noise.
    scope = [
        advertiser_id
        for advertiser_id in config.advertiser_ids
        if advertiser_id in set(portfolio)
    ] or portfolio

    if not scope:
        return [
            "This API client's advertiser portfolio is empty, and Criteo's statistics "
            "report requires at least one advertiser. Grant the client access to an "
            "advertiser before capturing reports."
        ]

    errors: list[str] = []
    for report in config.reports:
        try:
            await probe_report(
                http, report, report_document_model(report), scope, log
            )
        except HTTPError as err:
            errors.append(
                f"Criteo rejected report {report.name}. Confirm its dimensions, metrics, "
                f"currency and timezone are ones Criteo accepts together.\n\n{err.message}"
            )
        except RuntimeError as err:
            errors.append(f"Report {report.name}: {err}")
        except PydanticValidationError as err:
            errors.append(
                f"Criteo's response for report {report.name} does not match what the "
                f"collection is keyed on. Every dimension ({', '.join(report.all_dimensions)}) "
                f"must be present on each row.\n\n{err}"
            )

    return errors


def snapshot_resources(
    http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    def open(
        fetch_snapshot,
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
            fetch_snapshot=fetch_snapshot,
        )

    snapshots = {
        "advertisers": functools.partial(snapshot_advertisers, http),
        "audiences": functools.partial(snapshot_audiences, http, config.advertiser_ids),
        "ad_sets": functools.partial(snapshot_ad_sets, http, config.advertiser_ids),
        "campaigns": functools.partial(snapshot_campaigns, http, config.advertiser_ids),
    }

    return [
        common.SnapshotResource(
            name=name,
            open=functools.partial(open, fetch_snapshot),
            initial_config=ResourceConfig(name=name, interval=timedelta(hours=1)),
        )
        for name, fetch_snapshot in snapshots.items()
    ]


def report_resources(
    http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """One collection per configured statistics report, keyed by its dimensions."""

    def open(
        report: ReportConfig,
        document_model,
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
                report,
                document_model,
                config.advertiser_ids,
                config.start_date,
                config.advanced.report_window_size,
                config.advanced.report_lookback_days,
            ),
            fetch_page=functools.partial(
                backfill_report,
                http,
                report,
                document_model,
                config.advertiser_ids,
                config.start_date,
                config.advanced.report_window_size,
            ),
        )

    # Report days are whole UTC days, so the backfill/incremental seam is a day
    # boundary: the backfill owns every day before today, and the incremental
    # task owns today onwards, starting from a cursor that has not yet reached it.
    cutoff = floor_to_day(datetime.now(tz=UTC))

    resources: list[common.Resource] = []
    for report in config.reports:
        document_model = report_document_model(report)
        name = report_stream_name(report)
        resources.append(
            common.Resource(
                name=name,
                key=report_key(report),
                model=document_model,
                open=functools.partial(open, report, document_model),
                initial_state=ResourceState(
                    inc=ResourceState.Incremental(cursor=cutoff),
                    backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
                ),
                initial_config=ResourceConfig(name=name, interval=timedelta(hours=1)),
                schema_inference=True,
            )
        )

    return resources


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = CriteoTokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    return [
        *snapshot_resources(http, config),
        *report_resources(http, config),
    ]
