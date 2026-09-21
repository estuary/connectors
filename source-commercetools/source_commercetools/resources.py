from datetime import UTC, datetime, timedelta
import functools
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    SETTLE_DELAY,
    SMALLEST_DATETIME_GRAIN,
    floor_tick,
    backfill_resources,
    fetch_resources,
    granted_permissions,
    introspect_token,
    is_accessible,
)
from .models import (
    STREAMS,
    CommercetoolsResource,
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    oauth2_spec,
)


# Discovery's permission check needs a live token, which the snapshot test has no
# account for. This key is the escape hatch it recognises. A Project key may only
# contain [A-Za-z0-9_-], so the dots keep it out of reach of any real configuration.
TEST_CONFIG_PROJECT_KEY = "dontchangethis.notareal.commercetools.project"


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the API Client authenticates and covers the configured Project."""
    http.token_source = TokenSource(
        oauth_spec=oauth2_spec(config.region), credentials=config.credentials
    )

    try:
        _, token = await http.token_source.fetch_token(log, http)
        introspection = await introspect_token(
            http,
            config.region,
            config.credentials.client_id,
            config.credentials.client_secret,
            token,
            log,
        )
    except HTTPError as err:
        if err.code in (400, 401):
            msg = (
                "Invalid credentials. Please confirm the provided client ID and secret are correct, "
                f"and that the API Client belongs to a Project in the {config.region} region."
                f"\n\n{err.message}"
            )
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])

    if not introspection.active:
        raise ValidationError(
            [
                "The API Client's credentials are no longer active. Please confirm the API Client "
                "has not been deleted in the Merchant Center under Settings > Developer settings."
            ]
        )

    # An empty scope means "cannot tell", not "no access" — the checks below would
    # otherwise reject every configuration if introspection ever omitted it.
    if not introspection.scope:
        log.warning(
            "Token introspection returned no scopes; skipping Project and permission checks.",
            {"project_key": config.project_key},
        )
        return

    permissions = granted_permissions(introspection.scope, config.project_key)

    if not permissions:
        raise ValidationError(
            [
                f"The API Client has no permissions for Project '{config.project_key}'. Please confirm the "
                "Project key is correct and that the API Client was created within that Project."
            ]
        )

    inaccessible = [model.PATH for model in STREAMS if not is_accessible(permissions, model.PATH)]
    if inaccessible:
        log.warning(
            "The API Client cannot read every supported resource. Any binding for these will fail.",
            {
                "inaccessible": inaccessible,
                "missing_scopes": [f"view_{path}:{config.project_key}" for path in inaccessible],
            },
        )


async def _accessible_streams(
    log: Logger, http: HTTPMixin, config: EndpointConfig, token: str
) -> list[type[CommercetoolsResource]]:
    """Returns the streams this API Client is able to read.
    An API Client whose scopes cannot be determined gets everything.
    """
    introspection = await introspect_token(
        http,
        config.region,
        config.credentials.client_id,
        config.credentials.client_secret,
        token,
        log,
    )

    if not introspection.scope:
        return STREAMS

    permissions = granted_permissions(introspection.scope, config.project_key)

    return [model for model in STREAMS if is_accessible(permissions, model.PATH)]


def incremental_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    streams: list[type[CommercetoolsResource]],
) -> list[common.Resource]:
    def open(
        model: type[CommercetoolsResource],
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
                fetch_resources,
                http,
                config.region,
                config.project_key,
                model,
            ),
            fetch_page=functools.partial(
                backfill_resources,
                http,
                config.region,
                config.project_key,
                config.start_date,
                model,
            ),
        )

    cutoff = floor_tick(datetime.now(tz=UTC)) - SETTLE_DELAY

    return [
        common.Resource(
            name=model.PATH,
            key=["/id"],
            model=model,
            open=functools.partial(open, model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff - SMALLEST_DATETIME_GRAIN),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
            ),
            initial_config=ResourceConfig(
                name=model.PATH, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for model in streams
    ]


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    should_check_permissions: bool = False,
) -> list[common.Resource]:
    http.token_source = TokenSource(
        oauth_spec=oauth2_spec(config.region), credentials=config.credentials
    )

    streams = STREAMS

    if should_check_permissions and config.project_key != TEST_CONFIG_PROJECT_KEY:
        _, token = await http.token_source.fetch_token(log, http)
        streams = await _accessible_streams(log, http, config, token)

    return incremental_resources(log, http, config, streams)
