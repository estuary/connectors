from datetime import datetime, timedelta, UTC
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource

from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    GOOGLE_SPEC,
    GooglePlayRow,
    RESOURCES,
)
from .api import (
    fetch_resources,
    backfill_resources,
)

from .gcs import GCSClient
from .shared import start_of_month, dt_to_str


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(
        google_spec=GOOGLE_SPEC,
        credentials=config.credentials,
        oauth_spec=None
    )
    gcs_client = GCSClient(
        log=log,
        bucket=config.bucket,
        http=http,
    )

    try:
        async for _ in gcs_client.list_all_files():
            break
    except Exception as e:
        msg = f"Encountered error while validating credentials. Please confirm provided credentials are correct. Error: {e}"
        raise ValidationError([msg])


def resources(
        log: Logger, config: EndpointConfig, gcs_client: GCSClient,
) -> list[common.Resource]:

    def open(
        model: type[GooglePlayRow],
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
                gcs_client,
                model,
            ),
            fetch_page=functools.partial(
                backfill_resources,
                gcs_client,
                model,
            )
        )

    # Round the cutoff down to the current month to avoid the backfill &
    # incremental tasks from overlapping which CSVs they process.
    start = dt_to_str(start_of_month(config.start_date))
    cutoff = start_of_month(datetime.now(tz=UTC))

    return [
        common.Resource(
            name=model.name,
            key=sorted(model.primary_keys),
            model=model,
            open=functools.partial(open, model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=start)
            ),
            initial_config=ResourceConfig(
                name=model.name, interval=timedelta(minutes=60)
            ),
            schema_inference=True,
        )
        for model in RESOURCES
    ]


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(
        google_spec=GOOGLE_SPEC,
        credentials=config.credentials,
        oauth_spec=None
    )
    gcs_client = GCSClient(
        log=log,
        bucket=config.bucket,
        http=http,
    )

    return resources(log, config, gcs_client)
