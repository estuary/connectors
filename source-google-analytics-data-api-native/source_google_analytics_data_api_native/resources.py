from datetime import datetime, timedelta
import functools
import json
from logging import Logger
from typing import Any
from zoneinfo import ZoneInfo

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError

from pydantic import ValidationError as ModelValidationError
from .api import (
    fetch_property_timezone,
    fetch_report,
    backfill_report,
)
from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    OAUTH2_SPEC,
    create_report_doc_model,
    Report,
    ReportDocument,
)
from .utils import (
    dt_to_str,
)

from .default_reports import DEFAULT_REPORTS


async def validate_credentials(
    log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    try:
        await fetch_property_timezone(http, config.property_id, log)
    except HTTPError as err:
        msg = 'Unknown error occurred.'
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def validate_custom_reports_json(
    custom_reports_json: str | None,
):
    if not custom_reports_json:
        return

    custom_reports: Any

    try:
        custom_reports = json.loads(custom_reports_json)
        assert isinstance(custom_reports, list)
    except (json.decoder.JSONDecodeError, AssertionError) as err:
        if isinstance(err, json.decoder.JSONDecodeError):
            raise ValidationError(["Custom reports input is not valid JSON."])
        elif isinstance(err, AssertionError):
            raise ValidationError(["Custom reports JSON input is not an array."])

    errors: list[str] = []

    default_report_names = [report.get('name') for report in DEFAULT_REPORTS]

    for custom_report_details in custom_reports:
        try:
            assert isinstance(custom_report_details, dict)
            model = Report.model_validate(custom_report_details)
            if model.name in default_report_names:
                errors.append(f'Custom report name "{model.name}" already exists as a default report. Please rename the custom report.')
        except (AssertionError, ModelValidationError) as err:
            if isinstance(err, AssertionError):
                raise ValidationError(["Custom reports JSON input array must only contain objects."])
            elif isinstance(err, ModelValidationError):
                name = custom_report_details.get('name', 'UNKNOWN NAME')
                msg = f"Error when validating custom report \"{name}\". Ensure \"{name}\" follows the custom report format described in this connector's documentation."
                errors.append(msg)

    if errors:
        raise ValidationError(errors)


def reports(
        log: Logger, http: HTTPMixin, config: EndpointConfig, timezone: ZoneInfo, available_reports: list[dict[str, Any]]
) -> list[common.Resource]:

    def open(
        model: type[ReportDocument],
        report: Report,
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
                config.property_id,
                timezone,
                model,
                report,
            ),
            fetch_page=functools.partial(
                backfill_report,
                http,
                config.property_id,
                model,
                report,
            )
        )

    start = config.start_date.astimezone(tz=timezone)
    cutoff = datetime.now(tz=timezone)
    resources: list[common.Resource] = []

    for report_details in available_reports:
        report = Report.model_validate(report_details)
        model = create_report_doc_model(report)

        resource = common.Resource(
            name=report.name,
            key=["/property_id", "/report_date"] + [f"/{d}" for d in report.dimensions],
            model=model,
            open=functools.partial(open, model, report),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=dt_to_str(start))
            ),
            initial_config=ResourceConfig(
                name=report.name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )

        resources.append(resource)

    return resources


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)
    timezone = ZoneInfo(await fetch_property_timezone(http, config.property_id, log))

    available_reports = DEFAULT_REPORTS
    if config.custom_reports:
        custom_reports = json.loads(config.custom_reports)
        available_reports += custom_reports

    return [
        *reports(log, http, config, timezone, available_reports),
    ]
