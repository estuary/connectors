import functools
from collections.abc import AsyncGenerator, Callable
from datetime import timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, HTTPSession, TokenSource

from .api import (
    API,
    SANDBOX_API,
    list_employee_ids,
    snapshot_bulk_table,
    snapshot_employee_fields,
    snapshot_employees,
    snapshot_named_lists,
)
from .models import (
    BULK_TABLE_STREAMS,
    EndpointConfig,
    ResourceConfig,
    ResourceState,
)

SNAPSHOT_INTERVAL = timedelta(hours=1)

type SnapshotFn = Callable[[HTTPSession, str, Logger], AsyncGenerator]


def base_url(config: EndpointConfig) -> str:
    return SANDBOX_API if config.advanced.sandbox else API


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured service user authenticates against HiBob and can see employees."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    url = base_url(config)

    try:
        # The fields endpoint needs no permissions, so it only proves authentication.
        await http.request(log, f"{url}/company/people/fields")
        employee_ids = await list_employee_ids(http, url, log)
    except HTTPError as err:
        if err.code == 401:
            msg = f"Invalid service user credentials. Please confirm the service user ID and token are correct.\n\n{err.message}"
        elif err.code == 403:
            msg = f"The service user is not permitted to read employee data. Please confirm it belongs to a permission group with access to People's data.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])

    # HiBob returns an empty result rather than an error when the service user has no access.
    if not employee_ids:
        raise ValidationError(
            [
                "The service user can't see any employees. Add it to a permission group with access "
                "to People's data for all employees (remove the 'Lifecycle status equals Employed' "
                "condition to include terminated employees) and View permission on the categories to capture."
            ]
        )


def _snapshot_resource(
    name: str,
    fetch_snapshot: functools.partial,
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
            fetch_snapshot=fetch_snapshot,
        )

    return common.SnapshotResource(
        name=name,
        open=open,
        initial_config=ResourceConfig(name=name, interval=SNAPSHOT_INTERVAL),
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    url = base_url(config)

    snapshots: list[tuple[str, SnapshotFn]] = [
        ("employees", snapshot_employees),
        ("employee_fields", snapshot_employee_fields),
        ("named_lists", snapshot_named_lists),
    ]

    resources = [
        _snapshot_resource(name, functools.partial(fn, http, url))
        for name, fn in snapshots
    ]
    resources.extend(
        _snapshot_resource(
            table.name, functools.partial(snapshot_bulk_table, http, url, table)
        )
        for table in BULK_TABLE_STREAMS
    )

    return resources
