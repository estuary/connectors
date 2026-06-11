import asyncio
import functools
from datetime import timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import CaptureBinding, ResourceConfig, ResourceState
from estuary_cdk.flow import OAuth2TokenFlowSpec, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from . import api
from .models import EndpointConfig, OAuthCredentials, ZuoraDocument

# Max concurrent describe calls during discovery.
_DISCOVER_CONCURRENCY = 10


def _setup_http_auth(http: HTTPMixin, config: EndpointConfig) -> None:
    if isinstance(config.credentials, OAuthCredentials):
        http.token_source = TokenSource(
            oauth_spec=OAuth2TokenFlowSpec(
                accessTokenUrlTemplate=f"{config.base_url}/oauth/token",
                accessTokenResponseMap={"access_token": "/access_token"},
            ),
            credentials=config.credentials,
        )
    else:
        # BASIC auth: headers are injected per-request in api.py via auth_headers().
        http.token_source = None


def _open_incremental(
    object_name: str,
    fields: list[str],
    config: EndpointConfig,
    http: HTTPMixin,
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    state: ResourceState,
    task: Task,
    all_bindings: list[CaptureBinding[ResourceConfig]],
) -> None:
    common.open_binding(
        binding,
        binding_index,
        state,
        task,
        fetch_changes=functools.partial(
            api.fetch_changes,
            object_name,
            fields,
            config.base_url,
            http,
            config,
        ),
        fetch_page=functools.partial(
            api.fetch_page,
            object_name,
            fields,
            config.base_url,
            http,
            config,
            config.start_date,
        ),
    )


def _open_snapshot(
    object_name: str,
    fields: list[str],
    config: EndpointConfig,
    http: HTTPMixin,
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    state: ResourceState,
    task: Task,
    all_bindings: list[CaptureBinding[ResourceConfig]],
) -> None:
    common.open_binding(
        binding,
        binding_index,
        state,
        task,
        fetch_snapshot=functools.partial(
            api.fetch_snapshot,
            object_name,
            fields,
            config.base_url,
            http,
            config,
        ),
        tombstone=ZuoraDocument(_meta=ZuoraDocument.Meta(op="d")),
    )


async def _describe_one(
    object_name: str,
    base_url: str,
    http: HTTPMixin,
    log: Logger,
    auth: dict[str, str],
    sem: asyncio.Semaphore,
) -> tuple[str, list[str]] | None:
    async with sem:
        try:
            fields, is_queryable = await api.describe_object(
                base_url, http, log, object_name, auth
            )
        except HTTPError as err:
            log.warning("Skipping object: describe failed", {"object": object_name, "http_code": err.code})
            return None
        except Exception as err:
            log.warning("Skipping object: describe failed", {"object": object_name, "error": str(err)})
            return None

    if not is_queryable:
        log.debug("Skipping object: not queryable via ZOQL", {"object": object_name})
        return None
    if not fields:
        log.warning("Skipping object: no selectable fields", {"object": object_name})
        return None
    return object_name, fields


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    _setup_http_auth(http, config)
    auth = api.auth_headers(config)

    object_names = await api.discover_object_names(config.base_url, http, log, auth)
    log.info("Describing Zuora objects", {"count": len(object_names), "concurrency": _DISCOVER_CONCURRENCY})

    sem = asyncio.Semaphore(_DISCOVER_CONCURRENCY)
    describe_results = await asyncio.gather(*[
        _describe_one(name, config.base_url, http, log, auth, sem)
        for name in object_names
    ])

    resources: list[common.Resource] = []
    for result in describe_results:
        if result is None:
            continue
        object_name, fields = result
        has_updated_date = "UpdatedDate" in fields

        if has_updated_date:
            resource = common.Resource(
                name=object_name,
                key=["/Id"],
                model=ZuoraDocument,
                open=functools.partial(
                    _open_incremental, object_name, fields, config, http
                ),
                initial_state=ResourceState(
                    inc=ResourceState.Incremental(cursor=config.start_date),
                ),
                initial_config=ResourceConfig(
                    name=object_name, interval=timedelta(minutes=5)
                ),
                schema_inference=True,
            )
        else:
            resource = common.Resource(
                name=object_name,
                key=["/_meta/row_id"],
                model=ZuoraDocument,
                open=functools.partial(
                    _open_snapshot, object_name, fields, config, http
                ),
                initial_state=ResourceState(),
                initial_config=ResourceConfig(
                    name=object_name, interval=timedelta(hours=1)
                ),
                schema_inference=True,
            )

        resources.append(resource)

    return resources


async def validate_credentials(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> None:
    """Validate connectivity by describing the Account object."""
    _setup_http_auth(http, config)
    auth = api.auth_headers(config)
    try:
        await api.describe_object(config.base_url, http, log, "Account", auth)  # result unused
    except HTTPError as err:
        if err.code == 401:
            raise ValidationError(
                ["Authentication failed. Please check your credentials."]
            )
        raise ValidationError([f"Failed to connect to Zuora: {err.message}."])
    except Exception as err:
        raise ValidationError([f"Failed to connect to Zuora: {err}."])
