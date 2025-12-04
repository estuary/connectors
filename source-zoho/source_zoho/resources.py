import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture.common import (
    ReductionStrategy,
    Resource,
    ResourceState,
    open_binding,
)
from estuary_cdk.capture.task import Task
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    FieldListingUnavailable,
    fetch_available_modules,
    fetch_module,
    fetch_supported_fields,
)
from .bulk_api import BulkJobManager, backfill_module
from .models import (
    OAUTH2_SPEC,
    EndpointConfig,
    ModuleFieldListing,
    ZohoModule,
    ZohoResourceConfigWithSchedule,
)


async def validate_credentials(http: HTTPMixin, config: EndpointConfig, log: Logger):
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    try:
        _ = await anext(
            fetch_available_modules(config.credentials.api_domain, log, http)
        )
    except HTTPError as err:
        msg = f"Encountered error validating credentials.\n\n{err.message}"
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"

        raise ValidationError([msg])
    except StopAsyncIteration:
        pass


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[Resource]:
    if http.token_source is None:
        http.token_source = TokenSource(
            oauth_spec=OAUTH2_SPEC, credentials=config.credentials
        )

    # API works with second precision
    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    bulk_job_manager = BulkJobManager(config.credentials.api_domain, http, log)

    def open(
        module: type[ZohoModule],
        binding: CaptureBinding[ZohoResourceConfigWithSchedule],
        binding_index: int,
        resource_state: ResourceState,
        task: Task,
        _all_bindings,
    ) -> None:
        open_binding(
            binding,
            binding_index,
            resource_state,
            task,
            fetch_changes=functools.partial(
                fetch_module,
                config.credentials.api_domain,
                module,
                http,
            ),
            fetch_page=functools.partial(
                backfill_module,
                config.start_date,
                module,
                bulk_job_manager,
            ),
        )

    def make_resource(module: type[ZohoModule]) -> Resource:
        return Resource(
            name=module.api_name,
            key=["/id"],
            model=module,
            open=functools.partial(open, module),
            initial_state=ResourceState(
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
                inc=ResourceState.Incremental(cursor=cutoff),
            ),
            initial_config=ZohoResourceConfigWithSchedule(
                name=module.api_name,
                interval=timedelta(minutes=5),
                schedule="55 23 * * *",  # Formula refresh at 23:55 UTC daily
            ),
            schema_inference=True,
            reduction_strategy=ReductionStrategy.MERGE,
        )

    resources = []

    async for listing in fetch_available_modules(
        config.credentials.api_domain, log, http
    ):
        try:
            known_fields: list[ModuleFieldListing] = []

            async for field in fetch_supported_fields(
                config.credentials.api_domain, listing.api_name, log, http
            ):
                known_fields.append(field)
        except FieldListingUnavailable:
            log.info(
                f"Skipping module with unavailable field listing: {listing.api_name}"
            )
            continue

        module = type(
            listing.api_name,
            (ZohoModule,),
            {
                "api_name": listing.api_name,
                "generated_type": listing.generated_type,
                "field_metadata": {f.api_name: f for f in known_fields},
            },
        )

        if not module.is_api_supported():
            log.info(f"Skipping unsupported module: {listing.api_name}")
            continue

        resources.append(make_resource(module))

    return resources
