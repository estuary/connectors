from logging import Logger
from typing import AsyncGenerator, Awaitable

from estuary_cdk.buffer_ordered import buffer_ordered
from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import TypeAdapter

from .models import (
    API_VERSION,
    LookerStream,
    LookerSearchStream,
    LookerChildStream,
    FullRefreshResource,
    LookMLModel,
)


def url_base(subdomain: str) -> str:
    return f"https://{subdomain}/api/{API_VERSION}"


def _is_missing_resource(err: HTTPError) -> bool:
    """
    Returns whether the error means Looker cannot serve this specific resource.

    While Looker is unavailable (an outage, scheduled maintenance, or an upgrade) it answers
    with an HTML error page under a 404 status, the same status it uses for a resource that
    genuinely cannot be served. Treating an outage as a missing resource would silently
    truncate a snapshot, and the CDK emits tombstones for the rows a snapshot no longer
    yields, so an outage would delete live documents and recreate them on the next sweep.
    Only Looker's JSON API error envelope is considered a missing resource. Anything else
    propagates and fails the snapshot loudly.
    """
    return err.code == 404 and "documentation_url" in err.message


async def _paginate_through_search(
    http: HTTPSession,
    url: str,
    log: Logger,
    fields: list[str] | None,
    limit: int = 100
) -> AsyncGenerator[FullRefreshResource, None]:
    offset = 0

    params: dict[str, int | str] = {
        "sorts": "id",
        "limit": limit,
        "offset": offset,
    }

    if fields:
        params["fields"] = ",".join(fields)

    while True:
        params["offset"] = offset
        _, body = await http.request_stream(log, url, params=params)

        count = 0
        async for resource in IncrementalJsonProcessor(
            body(),
            "item",
            FullRefreshResource,
        ):
            yield resource
            count += 1

        if count < limit:
            break

        offset += limit


async def snapshot_searchable_resources(
    http: HTTPSession,
    subdomain: str,
    stream: type[LookerSearchStream],
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{url_base(subdomain)}/{stream.path}"

    async for resource in _paginate_through_search(http, url, log, stream.fields, stream.limit):
        yield resource


async def snapshot_resources(
    http: HTTPSession,
    subdomain: str,
    stream: type[LookerStream],
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{url_base(subdomain)}/{stream.path}"

    resources = TypeAdapter(list[FullRefreshResource]).validate_json(await http.request(log, url))

    for resource in resources:
        yield resource


async def snapshot_child_resources(
    http: HTTPSession,
    subdomain: str,
    stream: type[LookerChildStream],
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    parent_url = f"{url_base(subdomain)}/{stream.parent.path}"

    parent_resources = TypeAdapter(list[FullRefreshResource]).validate_json(await http.request(log, parent_url))

    parent_ids: list[str] = []

    for parent in parent_resources:
        parent = parent.model_dump()
        id = parent.get("id")
        can_permissions: dict[str, bool] | None = parent.get("can")

        assert isinstance(id, str)
        should_be_accessible = True
        # If the connector is restricted from accessing the child resource of
        # this parent resource, don't waste an HTTP request trying to access it.
        if (
            can_permissions
            and stream.required_can_permission
            and not can_permissions.get(stream.required_can_permission, False)
        ):
            should_be_accessible = False

        if should_be_accessible:
            parent_ids.append(id)

    # There can be thousands of child resources we have to fetch. The buffered_ordered
    # function is used to fetch multiple batches of child resources concurrently
    # while maintaining the initial ordering. The ordering aspect is important since
    # this is a snapshot function; the CDK relies on documents being yielded in the
    # same order between snapshots to determine if the snapshot has changed between
    # sweeps.
    async def _fetch_child_resources(parent_id: str) -> list[FullRefreshResource]:
        child_url = f"{parent_url}/{parent_id}/{stream.path}"

        try:
            response = await http.request(log, child_url)
        except HTTPError as err:
            # The parent's `can` permissions don't catch every reason children are
            # unavailable, like a parent deleted since it was listed. Looker 404s these,
            # so skip this parent's children rather than fail the entire snapshot.
            if _is_missing_resource(err):
                log.warning(
                    "Skipping inaccessible child resources.",
                    {
                        "stream": stream.name,
                        "parent_id": parent_id,
                        "error": err.message,
                    },
                )
                return []
            raise

        return TypeAdapter(list[FullRefreshResource]).validate_json(response)

    async def _gen() -> AsyncGenerator[Awaitable[list[FullRefreshResource]], None]:
        for id in parent_ids:
            yield _fetch_child_resources(id)

    async for child_batch in buffer_ordered(_gen(), concurrency=25):
        for resource in child_batch:
            yield resource


async def snapshot_lookml_model_explores(
    http: HTTPSession,
    subdomain: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    lookml_models_url = f"{url_base(subdomain)}/lookml_models"

    lookml_models = TypeAdapter(list[LookMLModel]).validate_json(await http.request(log, lookml_models_url))

    # lookml_model_info contains the information necessary to look up the explores
    # associated for each LookML model.
    # The first element is the LookML model name.
    # The second element is a list of explore names for that LookML model.
    lookml_models_info: list[tuple[str, list[str]]] = []

    for lookml_model in lookml_models:
        lookml_models_info.append(
            (
                lookml_model.name,
                [explore.name for explore in lookml_model.explores ]
            )
        )

    for model_name, explores in lookml_models_info:
        for explore_name in explores:
            explore_url = f"{lookml_models_url}/{model_name}/explores/{explore_name}"

            try:
                response = await http.request(log, explore_url)
            except HTTPError as err:
                # A model's nav listing advertises explores this endpoint won't serve: abstract
                # explores (`extension: required`), explores in a model that no longer compiles,
                # and explores the credentials can't see. Looker 404s all of these, so skip the
                # explore rather than fail the entire snapshot.
                if _is_missing_resource(err):
                    log.warning(
                        "Skipping inaccessible LookML model explore.",
                        {
                            "model": model_name,
                            "explore": explore_name,
                            "error": err.message,
                        },
                    )
                    continue
                raise

            yield FullRefreshResource.model_validate_json(response)
