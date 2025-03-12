from datetime import datetime, timedelta, UTC
import functools
from logging import Logger
from typing import Any

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource

from .supported_standard_objects import SUPPORTED_STANDARD_OBJECTS, COMMON_CUSTOM_OBJECT_DETAILS

from .bulk_job_manager import BulkJobManager
from .rest_query_manager import RestQueryManager
from .shared import dt_to_str, VERSION

from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    AccessTokenResponse,
    GlobalDescribeObjectsResponse,
    FieldDetailsDict,
    SObject,
    OAUTH2_SPEC,
    FullRefreshResource,
    SalesforceResource,
    update_oauth_spec,
)
from .api import (
    snapshot_resources,
    backfill_incremental_resources,
    fetch_incremental_resources,
)


CUSTOM_OBJECT_SUFFIX = '__c'


async def _fetch_instance_url(log: Logger, http: HTTPMixin, config: EndpointConfig) -> str:
    url = OAUTH2_SPEC.accessTokenUrlTemplate
    body = {
        "grant_type": "refresh_token",
        "client_id": config.credentials.client_id,
        "client_secret": config.credentials.client_secret,
        "refresh_token": config.credentials.refresh_token,
    }

    response = AccessTokenResponse.model_validate_json(
        await http.request(log, url, method="POST", form=body, _with_token=False)
    )

    return response.instance_url


async def _fetch_queryable_objects(log: Logger, http: HTTPMixin, instance_url: str) -> list[str]:
    url = f"{instance_url}/services/data/v{VERSION}/sobjects"

    response = GlobalDescribeObjectsResponse.model_validate_json(
        await http.request(log, url)
    )

    return [o.name for o in response.sobjects if o.queryable]


async def _fetch_object_fields(log: Logger, http: HTTPMixin, instance_url: str, name: str) -> FieldDetailsDict:
    url = f"{instance_url}/services/data/v{VERSION}/sobjects/{name}/describe"

    response = SObject.model_validate_json(
        await http.request(log, url)
    )

    fields = {}

    for field in response.fields:
        fields[field.name] = {
            "soapType": field.soapType,
            "calculated": field.calculated,
        }

    return FieldDetailsDict.model_validate(fields)


def full_refresh_resource(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    bulk_job_manager: BulkJobManager,
    instance_url: str,
    name: str,
    fields: FieldDetailsDict | None,
    enable: bool,
) -> common.Resource:

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        # When `open` is called, we need to ensure there are actually fields provided. This avoids
        # using API requests during discovery, but we need to make sure the fields are present when validating & opening.
        if fields is None:
            raise RuntimeError(f"Missing fields for {name}. Contact Estuary Support for help resolving this error.")

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_resources,
                http,
                bulk_job_manager,
                instance_url,
                name,
                fields,
            ),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )

    resource = common.Resource(
        name=name,
        key=["/_meta/row_id"],
        model=FullRefreshResource,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(
            name=name, interval=timedelta(minutes=5)
        ),
        schema_inference=True,
        disable=not enable
    )

    return resource


def incremental_resource(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    bulk_job_manager: BulkJobManager,
    rest_query_manager: RestQueryManager,
    instance_url: str,
    name: str,
    fields: FieldDetailsDict | None,
    enable: bool,
) -> common.Resource:

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        # When `open` is called, we need to ensure there are actually fields provided. This avoids
        # using API requests during discovery, but we need to make sure the fields are present when validating & opening.
        if fields is None:
            raise RuntimeError(f"Missing fields for {name}. Contact Estuary Support for help resolving this error.")

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(
                backfill_incremental_resources,
                http,
                bulk_job_manager,
                rest_query_manager,
                instance_url,
                name,
                fields,
                config.advanced.window_size
            ),
            fetch_changes=functools.partial(
                fetch_incremental_resources,
                http,
                bulk_job_manager,
                rest_query_manager,
                instance_url,
                name,
                fields,
                config.advanced.window_size,
            ),
        )

    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    resource = common.Resource(
        name=name,
        key=["/Id"],
        model=SalesforceResource,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=dt_to_str(config.start_date), cutoff=cutoff)
        ),
        initial_config=ResourceConfig(
            name=name, interval=timedelta(minutes=5)
        ),
        schema_inference=True,
        disable=not enable,
    )

    return resource


async def _object_to_resource(
        log: Logger,
        http: HTTPMixin,
        config: EndpointConfig,
        bulk_job_manager: BulkJobManager,
        rest_query_manager: RestQueryManager,
        instance_url: str,
        name: str,
        should_fetch_fields: bool = False,
    ) -> common.Resource | None:
    is_custom_object = name.endswith(CUSTOM_OBJECT_SUFFIX)
    details = COMMON_CUSTOM_OBJECT_DETAILS if is_custom_object else SUPPORTED_STANDARD_OBJECTS.get(name, None)

    if details is None:
        return

    cursor_field = details.get('cursor_field', None)
    enable = details.get('enabled_by_default', False)
    assert isinstance(enable, bool)
    fields = await _fetch_object_fields(log, http, instance_url, name) if should_fetch_fields else None

    if is_custom_object or cursor_field is not None:
        return incremental_resource(
            log,
            http,
            config,
            bulk_job_manager,
            rest_query_manager,
            instance_url,
            name,
            fields,
            enable,
        )
    else:
        return full_refresh_resource(
            log,
            http,
            config,
            bulk_job_manager,
            instance_url,
            name,
            fields,
            enable,
        )


# enabled_resources returns resources for only enabled bindings.
async def enabled_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig, bindings: list[common._ResolvableBinding]
) -> list[common.Resource]:
    update_oauth_spec(config.is_sandbox)
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    instance_url = await _fetch_instance_url(log, http, config)

    enabled_binding_names: list[str] = []

    for binding in bindings:
        path: list[str] = binding.resourceConfig.path()
        enabled_binding_names.append(path[0])

    bulk_job_manager = BulkJobManager(http, log, instance_url)
    rest_query_manager = RestQueryManager(http, log, instance_url)
    resources: list[common.Resource] = []

    for name in enabled_binding_names:
        r = await _object_to_resource(log, http, config, bulk_job_manager, rest_query_manager, instance_url, name, True)
        if r:
            resources.append(r)

    return resources


# all_resources returns resources for all possible supported bindings.
async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    update_oauth_spec(config.is_sandbox)
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    instance_url = await _fetch_instance_url(log, http, config)

    queryable_object_names = await _fetch_queryable_objects(log, http, instance_url)

    bulk_job_manager = BulkJobManager(http, log, instance_url)
    rest_query_manager = RestQueryManager(http, log, instance_url)
    resources: list[common.Resource] = []

    for name in queryable_object_names:
        r = await _object_to_resource(log, http, config, bulk_job_manager, rest_query_manager, instance_url, name)
        if r:
            resources.append(r)

    return resources
