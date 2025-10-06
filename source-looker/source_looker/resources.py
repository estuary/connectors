import asyncio
from datetime import timedelta
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, HTTPError

from .models import (
    LookerTokenSource,
    EndpointConfig,
    FullRefreshResource,
    ResourceConfig,
    ResourceState,
    LookerStream,
    LookerChildStream,
    LookMLModelExplores,
    STREAMS,
    OAUTH2_SPEC,
)
from .api import (
    snapshot_resources,
    snapshot_child_resources,
    snapshot_lookml_model_explores,
    url_base,
)


def update_oauth2spec_with_domain(config: EndpointConfig):
    """
    Updates the OAuth2Spec with the config's domain.
    """
    login_url = OAUTH2_SPEC.accessTokenUrlTemplate.replace("DOMAIN", config.subdomain)
    OAUTH2_SPEC.accessTokenUrlTemplate = login_url


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    update_oauth2spec_with_domain(config)

    http.token_source = LookerTokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)
    url = f"{url_base(config.subdomain)}/users"

    try:
        await http.request(log, url)
    except HTTPError as err:
        msg = 'Unknown error occurred.'
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


async def is_accessible_endpoint(
    http: HTTPMixin,
    log: Logger,
    url: str,
) -> bool:
    """
    Returns a boolean representing whether the provided endpoint is accessible.

    If an endpoint is inaccessible with the current credentials, the Looker API returns a 404 error within a couple seconds.
    If an endpoint is accessible, it can take multiple minutes to completely receive the HTTP request if there's a lot of data.

    If we receive a non-TimeoutError exception within the first few seconds, we assume the endpoint is inaccessible.
    """
    is_accessible = True

    try:
        await asyncio.wait_for(http.request(log, url), timeout=10)
    except asyncio.TimeoutError:
        is_accessible = True
    except HTTPError as err:
        if err.code == 404:
            is_accessible = False
        else:
            raise

    return is_accessible


def full_refresh_resource(
        stream: type[LookerStream], log: Logger, http: HTTPMixin, config: EndpointConfig,
) -> common.Resource:

    def open(
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_resources,
                http,
                config.subdomain,
                stream,
            ),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )


    return common.Resource(
        name=stream.name,
        key=["/_meta/row_id"],
        model=FullRefreshResource,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(
            name=stream.name, interval=timedelta(minutes=30)
        ),
        schema_inference=True,
    )


def full_refresh_child_resource(
        stream: type[LookerChildStream], log: Logger, http: HTTPMixin, config: EndpointConfig
) -> common.Resource:

    def open(
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        if stream is LookMLModelExplores:
            fetch_snapshot = functools.partial(
                snapshot_lookml_model_explores,
                http,
                config.subdomain,
            )
        else:
            fetch_snapshot = functools.partial(
                snapshot_child_resources,
                http,
                config.subdomain,
                stream,
            )

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=fetch_snapshot,
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )

    return common.Resource(
        name=stream.name,
        key=["/_meta/row_id"],
        model=FullRefreshResource,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(
            name=stream.name, interval=timedelta(minutes=30)
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    update_oauth2spec_with_domain(config)

    http.token_source = LookerTokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)
    base_url = url_base(config.subdomain)

    async def build_resource_if_accessible_stream(element: dict) -> list[common.Resource]:
        resources: list[common.Resource] = []
        base_stream: type[LookerStream] = element["stream"]
        url = f"{base_url}/{base_stream.path}"

        if await is_accessible_endpoint(http, log, url):
            resources.append(full_refresh_resource(base_stream, log, http, config))

            for child in element.get("children", []):
                child_stream: type[LookerChildStream] = child["stream"]
                resources.append(full_refresh_child_resource(child_stream, log, http, config))

        return resources

    # Concurrently check accessibility & build resoruces for all streams.
    results = await asyncio.gather(
        *(
            build_resource_if_accessible_stream(element)
            for element in STREAMS
        )
    )

    accessible_streams = [resource for group in results for resource in group]
    return accessible_streams
