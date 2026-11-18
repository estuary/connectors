from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from functools import partial
from logging import Logger

from azure.cosmos.aio import CosmosClient
from azure.cosmos._base import ParsePaths
from azure.cosmos.exceptions import CosmosHttpResponseError
from azure.core.exceptions import ClientAuthenticationError
from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding, ValidationError

from .api import (
    PARTITION_KEY_STATE, fetch_container, has_partition_key_state, list_containers, list_databases,
    sample_partition_key,
)
from .models import (
    AccountKeyCredentials, Container, Document, EndpointConfig, ResourceConfig, ResourceState,
)


@asynccontextmanager
async def open_client(config: EndpointConfig) -> AsyncIterator[CosmosClient]:
    async with AsyncExitStack() as cleanup:
        if isinstance(config.credentials, AccountKeyCredentials):
            credential = config.credentials.key
        else:
            from azure.identity.aio import ClientSecretCredential

            credential = ClientSecretCredential(
                tenant_id=config.credentials.tenant_id,
                client_id=config.credentials.client_id,
                client_secret=config.credentials.client_secret,
            )
            cleanup.push_async_callback(credential.close)

        client = CosmosClient(
            config.endpoint,
            credential=credential,
            enable_endpoint_discovery=not config.advanced.disable_endpoint_discovery,
            throughput_bucket=config.advanced.throughput_bucket,
        )
        # Close the client and credential even if account initialization fails.
        cleanup.push_async_callback(client.close)
        await client.__aenter__()
        yield client


def open_container(
    config: EndpointConfig,
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    state: ResourceState,
    task: Task,
    all_bindings: list[tuple[CaptureBinding[ResourceConfig], common.Resource]],
) -> None:
    async def run(task: Task) -> None:
        async with open_client(config) as client:
            async def capture(task: Task) -> None:
                common.open_binding(
                    binding,
                    binding_index,
                    state,
                    task,
                    fetch_changes=partial(
                        fetch_container, client,
                        binding.resourceConfig.database or config.database, binding.resourceConfig.name,
                        schema=binding.collection.writeSchema,
                        start_time=config.advanced.start_time,
                        priority=config.advanced.priority,
                        max_item_count=config.advanced.max_item_count,
                    ),
                )

            # Wait for the binding's children before closing its shared client.
            await task.spawn_child("capture", capture)

    task.spawn_child("/".join(binding.resourceConfig.path()), run)


def container_schema(container: Container, key_types: dict[str, list[str]]) -> common.Resource.FixedSchema:
    schema = Document.model_json_schema(mode="serialization")
    meta = schema["$defs"]["Meta"]
    if has_partition_key_state(container.partitionKey):
        # The connector always writes the state, so it is a non-null key field.
        meta["properties"][PARTITION_KEY_STATE] = {
            "type": "string", "description": meta["properties"][PARTITION_KEY_STATE]["description"],
        }
        meta["required"] = [PARTITION_KEY_STATE]
        schema["required"].append("_meta")
    else:
        del meta["properties"][PARTITION_KEY_STATE]
    for path in container.partitionKey.paths:
        node = schema
        segments = ParsePaths([path])
        for index, field in enumerate(segments):
            if field not in node["required"]:
                node["required"].append(field)
            if index == len(segments) - 1:
                node["properties"].setdefault(field, {"type": key_types[path]})
            else:
                node = node["properties"].setdefault(field, {
                    "type": "object", "properties": {}, "required": [],
                    "additionalProperties": True,
                })
    return common.Resource.FixedSchema(schema)


async def all_resources(
    log: Logger, config: EndpointConfig, sample_keys: bool = True,
) -> list[common.Resource]:
    """Build resources for the selected databases.

    Only discovery samples partition key values to choose key types. Validate
    and Open match bindings by path, so they use container metadata alone and
    a later sample can't drop a container that is already bound.
    """
    resources = []
    database = config.database
    try:
        async with open_client(config) as client:
            databases = (
                await list_databases(client) if config.all_databases
                else config.databases or [config.database]
            )
            for database in databases:
                containers = await list_containers(client, database)
                for container in containers:
                    paths = {path: ParsePaths([path]) for path in container.partitionKey.paths}
                    if any(segments[0] == "_meta" for segments in paths.values()):
                        log.warning(
                            f"Skipping Cosmos DB container '{database}/{container.id}': partition keys on or under "
                            "the reserved _meta field cannot be captured.",
                        )
                        continue

                    key_types: dict[str, list[str]] = {}
                    skip_container = False
                    for path, segments in paths.items():
                        if not sample_keys:
                            key_types[path] = ["string"] if segments == ["id"] else ["string", "integer", "boolean"]
                            continue
                        # Cosmos DB item IDs are always strings.
                        try:
                            value = None if segments == ["id"] else await sample_partition_key(
                                client, database, container.id, path, config.advanced.priority,
                            )
                        except CosmosHttpResponseError as err:
                            if err.status_code != 400:
                                raise
                            log.warning(
                                f"Skipping Cosmos DB container '{database}/{container.id}': sampling the partition "
                                f"key at '{path}' failed: {err.message}",
                            )
                            skip_container = True
                            break
                        if isinstance(value, bool):
                            key_types[path] = ["boolean"]
                        elif isinstance(value, int) or (isinstance(value, float) and value.is_integer()):
                            key_types[path] = ["integer"]
                        elif isinstance(value, float):
                            log.warning(
                                f"Skipping Cosmos DB container '{database}/{container.id}': non-integer numeric "
                                f"partition key at '{path}' cannot be used as a collection key.",
                            )
                            skip_container = True
                            break
                        else:
                            key_types[path] = ["string"]
                    if skip_container:
                        continue

                    keys = [
                        "/" + "/".join(segment.replace("~", "~0").replace("/", "~1") for segment in segments)
                        for segments in paths.values()
                    ]
                    resource_config = ResourceConfig(container=container.id)
                    if not config.database:
                        resource_config.database = database
                    resources.append(common.Resource(
                        name=container.id if config.database else f"{database}/{container.id}",
                        key=list(dict.fromkeys([
                            *keys,
                            *(["/_meta/" + PARTITION_KEY_STATE] if has_partition_key_state(container.partitionKey) else []),
                            "/id",
                        ])),
                        model=container_schema(container, key_types),
                        open=partial(open_container, config),
                        initial_state=ResourceState(
                            inc=ResourceState.Incremental(cursor=("",)),
                        ),
                        initial_config=resource_config,
                        schema_inference=True,
                    ))
    except ClientAuthenticationError as err:
        raise ValidationError([
            "Microsoft Entra authentication failed. Check your service principal credentials."
        ]) from err
    except CosmosHttpResponseError as err:
        if err.status_code == 401:
            raise ValidationError([
                "Cosmos DB rejected the account key. Check the endpoint and account key."
                if isinstance(config.credentials, AccountKeyCredentials) else
                "Cosmos DB rejected the Microsoft Entra identity. Check the endpoint, tenant, and identity configuration."
            ]) from err
        if err.status_code == 403:
            raise ValidationError([
                "Cosmos DB denied access. Check the account key or Cosmos DB Built-in Data Reader role assignment and account network access settings."
            ]) from err
        if err.status_code == 404:
            raise ValidationError([
                f"Database '{database}' was not found. Check the endpoint and database name."
            ]) from err
        raise

    return resources
