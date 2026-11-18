import asyncio
import functools
from logging import Logger
from urllib.parse import urljoin

from estuary_cdk.capture import Task, common, request
from estuary_cdk.flow import CaptureBinding, ValidationError
from pyiceberg.exceptions import (
    AuthorizationExpiredError, ForbiddenError, NoSuchTableError, OAuthError, UnauthorizedError,
)
from pyiceberg.typedef import Identifier
from requests import ConnectionError as RequestsConnectionError, Timeout

from .api import IcebergCatalog, fetch_table, list_table_identifiers
from .models import (
    AWSIAMConfig, CatalogIAMConfig, EndpointConfig, IAMConfig, ResourceConfig, ResourceState, S3Config, SigV4Config,
)
from .schema import collection_key, collection_schema


def _identifier(config: ResourceConfig) -> Identifier:
    return (*config.namespace.split("."), config.table)


def _open_table(
    properties: dict[str, str],
    file_io_properties: dict[str, str],
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    state: ResourceState,
    task: Task,
    all_bindings: list,
):
    common.open_binding(
        binding,
        binding_index,
        state,
        task,
        fetch_changes=functools.partial(
            fetch_table, properties, file_io_properties, _identifier(binding.resourceConfig)
        ),
    )


def _aws_properties(config: AWSIAMConfig | SigV4Config, prefix: str) -> dict[str, str]:
    if isinstance(config, AWSIAMConfig) and not all((
        config.aws_access_key_id, config.aws_secret_access_key, config.aws_session_token,
    )):
        raise ValidationError(["Missing AWS IAM session credentials. Configure Estuary to assume your AWS role."])
    properties = {
        f"{prefix}.region": config.aws_region,
        f"{prefix}.access-key-id": config.aws_access_key_id,
        f"{prefix}.secret-access-key": config.aws_secret_access_key,
    }
    if config.aws_session_token:
        properties[f"{prefix}.session-token"] = config.aws_session_token
    return properties


def _all_resources(
    log: Logger, config: EndpointConfig, identifiers: list[Identifier] | None = None
) -> list[common.Resource]:
    properties = {"uri": config.catalog_uri}
    for name in ("warehouse", "credential", "token", "scope"):
        if value := getattr(config, name):
            properties[name] = value
    if config.oauth2_server_uri:
        properties["oauth2-server-uri"] = urljoin(config.catalog_uri.rstrip("/") + "/", config.oauth2_server_uri.lstrip("/"))
    if isinstance(auth := config.catalog_auth, (SigV4Config, CatalogIAMConfig)):
        credentials = config.credentials if isinstance(auth, CatalogIAMConfig) else auth
        assert isinstance(credentials, (AWSIAMConfig, SigV4Config))
        properties.update(_aws_properties(credentials, "client"))
        properties.update({
            "rest.sigv4-enabled": "true",
            "rest.signing-region": credentials.aws_region,
            "rest.signing-name": auth.signing_name,
        })

    file_io_properties = {}
    if isinstance(config.s3, IAMConfig):
        assert isinstance(config.credentials, AWSIAMConfig)
        file_io_properties = _aws_properties(config.credentials, "s3")
    elif isinstance(s3 := config.s3, S3Config):
        file_io_properties = {
            "s3.region": s3.region,
            "s3.access-key-id": s3.access_key_id,
            "s3.secret-access-key": s3.secret_access_key,
            "s3.path-style-access": str(s3.path_style_access).lower(),
            "s3.force-virtual-addressing": str(not s3.path_style_access).lower(),
        }
        if s3.endpoint:
            file_io_properties["s3.endpoint"] = s3.endpoint
        if s3.session_token:
            file_io_properties["s3.session-token"] = s3.session_token

    resources = []
    try:
        with IcebergCatalog("estuary", file_io_properties, **properties) as catalog:
            for identifier in identifiers if identifiers is not None else list_table_identifiers(log, catalog, config.namespaces):
                if any("." in level for level in identifier[:-1]):
                    log.warning("skipping Iceberg namespace with literal dots", {"table": ".".join(identifier)})
                    continue
                try:
                    schema = catalog.load_table(identifier).schema()
                    document_schema = collection_schema(schema)
                except (NoSuchTableError, ForbiddenError, ValidationError, ValueError) as err:
                    if identifiers is not None and isinstance(err, ForbiddenError):
                        raise ValidationError([f"Cannot read Iceberg table {'.'.join(identifier)}: {err}"]) from err
                    if identifiers is not None and not isinstance(err, NoSuchTableError):
                        raise
                    log.warning("skipping unreadable Iceberg table", {"table": ".".join(identifier), "error": str(err)})
                    continue
                namespace, table = ".".join(identifier[:-1]), identifier[-1]
                resources.append(
                    common.Resource(
                        name=f"{namespace}_{table}",
                        key=collection_key(schema),
                        model=common.Resource.FixedSchema(document_schema),
                        open=functools.partial(_open_table, properties, file_io_properties),
                        # Snapshot IDs are opaque, not monotonically increasing integers.
                        initial_state=ResourceState(inc=ResourceState.Incremental(cursor=("",))),
                        initial_config=ResourceConfig(namespace=namespace, table=table),
                        schema_inference=False,
                        is_fallback_key=not schema.identifier_field_ids,
                    )
                )
    except (
        UnauthorizedError, AuthorizationExpiredError, OAuthError, ForbiddenError, RequestsConnectionError, Timeout,
    ) as err:
        # Report catalog connection and permission failures as configuration errors, not tracebacks.
        raise ValidationError([
            f"Could not access Iceberg catalog {config.catalog_uri}: {err}. "
            "Check the catalog URI and authentication settings."
        ]) from err
    return resources


async def all_resources(log: Logger, config: EndpointConfig) -> list[common.Resource]:
    return await asyncio.to_thread(_all_resources, log, config)


async def enabled_resources(
    log: Logger,
    config: EndpointConfig,
    bindings: list[CaptureBinding[ResourceConfig]] | list[request.ValidateBinding[ResourceConfig]],
) -> list[common.Resource]:
    identifiers = sorted({_identifier(binding.resourceConfig) for binding in bindings})
    return await asyncio.to_thread(_all_resources, log, config, identifiers)
