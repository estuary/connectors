import dataclasses
import functools
from datetime import datetime, timedelta
from logging import Logger

from aiohttp import ClientError

from estuary_cdk.capture import Task, common, request
from estuary_cdk.capture.common import ResourceState
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, ShouldRetryProtocol, TokenSource
from pydantic import ValidationError as PydanticValidationError

from .api import (
    HEADERS,
    backfill_records,
    capture_horizon,
    fetch_records,
    list_objects,
    probe_object,
    validation_should_retry,
)
from .models import (
    EndpointConfig,
    ErrorResponse,
    MetadataResponse,
    ObjectMetadata,
    ResourceConfig,
    TwentyRecord,
)


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """
    Validates that the API key works against the configured Twenty server.
    """
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    url = f"{config.base_url.rstrip('/')}{ObjectMetadata.PATH}"

    try:
        MetadataResponse.model_validate_json(
            await http.request(
                log,
                url,
                params={"limit": 1},
                headers=HEADERS,
                should_retry=validation_should_retry,
            )
        )
    except HTTPError as err:
        if err.code == 401:
            msg = f"Invalid API key. Please confirm the provided API key is correct.\n\n{err.message}"
        elif err.code == 403:
            msg = f"The API key can't read Twenty's metadata. Please give the key a role with the Data Model settings permission and read access to every object you want to capture.\n\n{err.message}"
        elif err.code == 404:
            msg = f"Incorrect base URL: Twenty's metadata endpoint was not found at {config.base_url}. Please confirm the base URL of your Twenty API.\n\n{err.message}"
        else:
            msg = f"Encountered error validating API key.\n\n{err.message}"

        raise ValidationError([msg])
    except PydanticValidationError as err:
        raise ValidationError(
            [
                f"This doesn't look like a Twenty API at {config.base_url}. Please confirm the base URL.\n\n"
                f"First problem in the metadata response: {err.errors()[0]['loc']}: {err.errors()[0]['msg']}"
            ]
        ) from err


def _permission_denied(err: HTTPError) -> bool:
    # Twenty answers 400 PERMISSION_DENIED when a role can't read an object.
    if err.code not in (400, 403) or err.body is None:
        return False
    try:
        return ErrorResponse.model_validate_json(err.body).code == "PERMISSION_DENIED"
    except PydanticValidationError:
        return False


async def disable_unreadable_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    resources: list[common.Resource],
) -> list[common.Resource]:
    """
    Discovers objects the key's role can't read as disabled bindings. Leaving them out
    would make re-discovery remove an existing binding, and adding them enabled would
    fail every auto-discover publish.
    """
    discovered: list[common.Resource] = []
    for resource in resources:
        try:
            await probe_object(log, http, config.base_url, resource.name)
        except HTTPError as err:
            if not _permission_denied(err):
                raise
            log.warning(
                "The API key's role can't read this object, so it is discovered disabled.",
                {"object": resource.name},
            )
            resource = dataclasses.replace(resource, disable=True)
        discovered.append(resource)
    return discovered


async def validate_bindings(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    bindings: list[request.ValidateBinding[ResourceConfig]],
    resources: list[common.Resource],
):
    """
    Reads one record of each bound object, so a key that can't read an object fails at publish time.
    """
    names = {resource.name for resource in resources}
    errors: list[str] = []
    for binding in bindings:
        name = binding.resourceConfig.name
        if name not in names:
            errors.append(
                f"Object '{name}': It was removed, deactivated or renamed in Twenty, or it is a system or remote object this connector doesn't capture. Re-discover the capture to update its bindings."
            )
            continue

        try:
            await probe_object(log, http, config.base_url, name)
        except HTTPError as err:
            if _permission_denied(err):
                reason = "The API key's role can't read it. Give the role read access to this object (for workflows, the Workflows permission), or disable its binding."
            elif err.code == 403:
                reason = f"The API key's role can't read it. {err.message}"
            else:
                reason = f"HTTP {err.code}: {err.message}"
            errors.append(f"Object '{name}': {reason}")
        except ValueError as err:
            errors.append(f"Object '{name}': Invalid Twenty API response. {err}")
        except (ClientError, TimeoutError) as err:
            # A streamed response can break mid-read; report it with the other objects.
            errors.append(
                f"Object '{name}': Connection failed while reading it. {err!r}"
            )

    if errors:
        raise ValidationError(errors)


def records_resource(
    http: HTTPMixin,
    base_url: str,
    object_name: str,
    cutoff: datetime,
    is_active: bool = True,
) -> common.Resource:
    """Scheduled backfills also refresh links cleared without changing updatedAt."""

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings: list[tuple[CaptureBinding[ResourceConfig], common.Resource]],
    ) -> None:
        if not is_active:
            task.log.warning(
                "Object is no longer in the Twenty workspace. Remove its binding from the capture.",
                {"object": object_name},
            )
            return

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(backfill_records, http, base_url, object_name),
            fetch_changes=functools.partial(fetch_records, http, base_url, object_name),
        )

    return common.Resource(
        name=object_name,
        key=["/id"],
        model=TwentyRecord,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
        ),
        initial_config=ResourceConfig(name=object_name, interval=timedelta(minutes=5)),
        schema_inference=True,
    )


async def _active_object_names(
    log: Logger,
    http: HTTPMixin,
    base_url: str,
    should_retry: ShouldRetryProtocol | None = None,
) -> set[str]:
    return {
        obj.namePlural
        async for obj in list_objects(log, http, base_url, should_retry)
        if obj.isActive
        and not obj.isRemote
        and (not obj.isSystem or obj.namePlural in {"noteTargets", "taskTargets"})
    }


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    cutoff = capture_horizon()
    names = sorted(
        await _active_object_names(log, http, config.base_url, validation_should_retry)
    )
    return [records_resource(http, config.base_url, name, cutoff) for name in names]


async def enabled_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    bindings: list[CaptureBinding[ResourceConfig]],
) -> list[common.Resource]:
    """Preserve binding order, including objects no longer in the workspace."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    cutoff = capture_horizon()
    # A running capture keeps the CDK's default retries, so a short outage doesn't fail it.
    active_names = await _active_object_names(log, http, config.base_url)
    if bindings and not any(b.resourceConfig.name in active_names for b in bindings):
        raise RuntimeError(
            "None of the capture's bound objects are in the Twenty workspace anymore. "
            "Re-discover the capture and remove the bindings of removed objects."
        )
    return [
        records_resource(
            http,
            config.base_url,
            binding.resourceConfig.name,
            cutoff,
            is_active=binding.resourceConfig.name in active_names,
        )
        for binding in bindings
    ]
