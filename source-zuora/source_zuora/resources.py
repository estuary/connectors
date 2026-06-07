import functools
from datetime import timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import CaptureBinding, ResourceConfig, ResourceState
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource
from estuary_cdk.flow import OAuth2TokenFlowSpec

from . import api
from .models import EndpointConfig, OBJECT_MODELS, ZuoraDocument


def _make_token_source(config: EndpointConfig) -> TokenSource:
    return TokenSource(
        oauth_spec=OAuth2TokenFlowSpec(
            accessTokenUrlTemplate=f"{config.tenant_endpoint}/oauth/token",
            accessTokenResponseMap={"access_token": "/access_token"},
        ),
        credentials=config.credentials,
    )


def _open_binding(
    object_name: str,
    fields: list[str],
    config: EndpointConfig,
    http: HTTPMixin,
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
            api.fetch_changes,
            object_name,
            fields,
            config.tenant_endpoint,
            http,
        ),
        fetch_page=functools.partial(
            api.fetch_page,
            object_name,
            fields,
            config.tenant_endpoint,
            http,
        ),
    )


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    http.token_source = _make_token_source(config)

    resources: list[common.Resource] = []

    for object_name, model_cls in OBJECT_MODELS.items():
        try:
            fields = await api.describe_object(
                config.tenant_endpoint, http, log, object_name
            )
        except HTTPError as err:
            log.warning(
                f"Skipping {object_name}: describe endpoint returned HTTP {err.code}. "
                "This object may not be available in your Zuora subscription."
            )
            continue
        except Exception as err:
            log.warning(f"Skipping {object_name}: failed to describe object: {err}")
            continue

        if not fields:
            log.warning(f"Skipping {object_name}: no selectable fields found.")
            continue

        # Validate field list against ZOQL, removing fields that cause errors
        # (e.g. system fields marked selectable in describe but rejected by ZOQL).
        fields = await api.validate_fields(
            object_name, fields, config.tenant_endpoint, http, log
        )

        # Ensure UpdatedDate is always in the field list for cursor tracking.
        if "UpdatedDate" not in fields:
            log.warning(
                f"Skipping {object_name}: UpdatedDate field not available. "
                "Incremental sync requires this field."
            )
            continue

        resource = common.Resource(
            name=object_name,
            key=["/Id"],
            model=model_cls,
            open=functools.partial(
                _open_binding,
                object_name,
                fields,
                config,
                http,
            ),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.start_date),
            ),
            initial_config=ResourceConfig(
                name=object_name,
                interval=timedelta(minutes=5),
            ),
            schema_inference=True,
        )
        resources.append(resource)

    return resources


async def validate_credentials(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> None:
    """Validate that the provided credentials can authenticate with Zuora."""
    http.token_source = _make_token_source(config)

    try:
        # A lightweight call to verify authentication and endpoint reachability.
        url = f"{config.tenant_endpoint}/v1/describe/Account"
        await http.request(log, url)
    except HTTPError as err:
        if err.code == 401:
            raise ValidationError(
                [
                    "Authentication failed. Please verify your Client ID and Client Secret "
                    f"are correct for the endpoint {config.tenant_endpoint}."
                ]
            )
        raise ValidationError(
            [
                f"Failed to connect to Zuora at {config.tenant_endpoint}. "
                f"HTTP {err.code}: {err.message}"
            ]
        )
