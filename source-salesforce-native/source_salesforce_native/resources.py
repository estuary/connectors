import asyncio
from datetime import datetime, timedelta, UTC
import functools
from logging import Logger
from typing import Any

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.capture.common import ReductionStrategy
from estuary_cdk.http import HTTPMixin

from .supported_standard_objects import (
    CUSTOM_OBJECT_WITH_SYSTEM_MODSTAMP_DETAILS,
    CUSTOM_OBJECT_WITH_CREATED_DATE_DETAILS,
    CUSTOM_OBJECT_WITH_LAST_MODIFIED_DATE_DETAILS,
    SUPPORTED_STANDARD_OBJECTS,

)

from .bulk_job_manager import BulkJobManager
from .rest_query_manager import RestQueryManager
from .shared import dt_to_str, now, VERSION

from .models import (
    EndpointConfig,
    SalesforceResourceConfigWithSchedule,
    ResourceState,
    SalesforceTokenSource,
    GlobalDescribeObjectsResponse,
    SOAP_TYPES_NOT_SUPPORTED_BY_BULK_API,
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
CUSTOM_OBJECT_FEED_SUFFIX = '__Feed'
CUSTOM_OBJECT_METADATA_SUFFIX = '__mdt'
CUSTOM_OBJECT_HISTORY_SUFFIX = '__History'
CUSTOM_OBJECT_SHARE_SUFFIX = '__Share'
BUILD_RESOURCE_SEMAPHORE_LIMIT = 15


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
        binding: CaptureBinding[SalesforceResourceConfigWithSchedule],
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
        initial_config=SalesforceResourceConfigWithSchedule(
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
    is_supported_by_bulk_api: bool,
    bulk_job_manager: BulkJobManager,
    rest_query_manager: RestQueryManager,
    instance_url: str,
    name: str,
    fields: FieldDetailsDict | None,
    enable: bool,
) -> common.Resource:

    def open(
        binding: CaptureBinding[SalesforceResourceConfigWithSchedule],
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
                is_supported_by_bulk_api,
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
                is_supported_by_bulk_api,
                bulk_job_manager,
                rest_query_manager,
                instance_url,
                name,
                fields,
                config.advanced.window_size,
            ),
        )

    cutoff = now()

    resource = common.Resource(
        name=name,
        key=["/Id"],
        model=SalesforceResource,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=dt_to_str(config.start_date), cutoff=cutoff)
        ),
        initial_config=SalesforceResourceConfigWithSchedule(
            name=name,
            interval=timedelta(minutes=5),
            # Default to performing a formula field refresh at 23:55 UTC every day for every enabled binding.
            schedule="55 23 * * *"
        ),
        schema_inference=True,
        disable=not enable,
        reduction_strategy=ReductionStrategy.MERGE,
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

    is_custom_object_with_system_modstamp = (
        name.endswith(CUSTOM_OBJECT_SUFFIX)
        or name.endswith(CUSTOM_OBJECT_FEED_SUFFIX)
        or name.endswith(CUSTOM_OBJECT_METADATA_SUFFIX)
    )

    is_custom_object_with_last_modified_date = (
        name.endswith(CUSTOM_OBJECT_SHARE_SUFFIX)
    )

    is_custom_object_with_created_date = (
        name.endswith(CUSTOM_OBJECT_HISTORY_SUFFIX)
    )

    if is_custom_object_with_system_modstamp:
        details = CUSTOM_OBJECT_WITH_SYSTEM_MODSTAMP_DETAILS
    elif is_custom_object_with_last_modified_date:
        details = CUSTOM_OBJECT_WITH_LAST_MODIFIED_DATE_DETAILS
    elif is_custom_object_with_created_date:
        details = CUSTOM_OBJECT_WITH_CREATED_DATE_DETAILS
    else:
        details = SUPPORTED_STANDARD_OBJECTS.get(name, None)

    if details is None:
        return

    cursor_field = details.get('cursor_field', None)
    enable = details.get('enabled_by_default', False)
    is_supported_by_bulk_api = details.get('is_supported_by_bulk_api', True)
    assert isinstance(enable, bool)
    assert isinstance(is_supported_by_bulk_api, bool)
    fields = await _fetch_object_fields(log, http, instance_url, name) if should_fetch_fields else None

    if fields:
        field_soap_types = {fields[field].soapType for field in fields}
        for soap_type in field_soap_types:
            if soap_type in SOAP_TYPES_NOT_SUPPORTED_BY_BULK_API:
                is_supported_by_bulk_api = False


    if cursor_field is not None:
        return incremental_resource(
            log,
            http,
            config,
            is_supported_by_bulk_api,
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
    http.token_source = SalesforceTokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    enabled_binding_names: list[str] = []

    for binding in bindings:
        path: list[str] = binding.resourceConfig.path()
        enabled_binding_names.append(path[0])

    bulk_job_manager = BulkJobManager(http, log, config.credentials.instance_url)
    rest_query_manager = RestQueryManager(http, log, config.credentials.instance_url)

    # If we concurrently send multiple requests that exchange the same refresh token for an access token,
    # some of those requests intermittently fail. 
    # https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_oauth_refresh_token_flow.htm&type=5#:~:text=Avoid%20sending%20simultaneous%20requests%20that%20contain%20the%20same%20refresh%20token.%20If%20your%20client%20sends%20identical%20requests%20at%20the%20same%20time%2C%20some%20of%20the%20requests%20fail%20intermittently%20and%20the%20Status%20column%20in%20the%20Login%20History%20displays%20Failed%3A%20Token%20request%20is%20already%20being%20processed.
    # To avoid this, we make a noop request to set the token_source's access token before using the scatter-gather
    # technique to make multiple requests concurrently. This prevents the first BUILD_RESOURCE_SEMAPHORE_LIMIT
    # requests from all exchanging the same access token and encountering that intermittent error.
    await _fetch_queryable_objects(log, http, config.credentials.instance_url)

    semaphore = asyncio.Semaphore(BUILD_RESOURCE_SEMAPHORE_LIMIT)
    async def build_resource(name: str) -> common.Resource | None:
        async with semaphore:
            return await _object_to_resource(
                log, http, config, bulk_job_manager, rest_query_manager, config.credentials.instance_url, name, True
            )

    task_results = await asyncio.gather(
        *(
            build_resource(name) for name in enabled_binding_names
        )
    )

    return [resource for resource in task_results if resource is not None]


# all_resources returns resources for all possible supported bindings.
async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    update_oauth_spec(config.is_sandbox)
    http.token_source = SalesforceTokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    queryable_object_names = await _fetch_queryable_objects(log, http, config.credentials.instance_url)

    bulk_job_manager = BulkJobManager(http, log, config.credentials.instance_url)
    rest_query_manager = RestQueryManager(http, log, config.credentials.instance_url)
    resources: list[common.Resource] = []

    for name in queryable_object_names:
        r = await _object_to_resource(log, http, config, bulk_job_manager, rest_query_manager, config.credentials.instance_url, name)
        if r:
            resources.append(r)

    return resources
