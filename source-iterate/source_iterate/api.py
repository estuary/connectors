from logging import Logger
from typing import AsyncGenerator
from urllib.parse import urlparse, parse_qs

from estuary_cdk.http import HTTPSession

from .models import (
    FullRefreshResource,
    SurveysResponse,
    SurveyResponsesResponse,
)

API = "https://iteratehq.com/api/v1"

# Iterate docs: https://iterate.docs.apiary.io/#introduction/overview
# The docs mention that every request needs a "v" query param that's the date
# of the implementation. It doesn't seem like this is required for API requests
# to succeed, but I assume it's used somehow to avoid breaking changes.
VERSION = 20250130


def _extract_next_page_cursor(url: str) -> str:
    query_params = parse_qs(urlparse(url).query)

    cursor = query_params.get("page[cursor]", None)
    if cursor is None:
        msg = f"Did not find a page[cursor] parameter in URL: {url}"
        raise RuntimeError(msg)

    return cursor[0]


async def snapshot_surveys(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{API}/surveys"
    params = {"v": VERSION}

    response = SurveysResponse.model_validate_json(
        await http.request(log, url, params=params)
    )

    for survey in response.results:
        yield survey


async def snapshot_survey_responses(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    async for survey in snapshot_surveys(http, log):
        survey_id = getattr(survey, "id")
        url = f"{API}/surveys/{survey_id}/responses"
        params: dict[str, str | int] = {"v": VERSION}

        while True:
            response = SurveyResponsesResponse.model_validate_json(
                await http.request(log, url, params=params)
            )

            for record in response.results.list:
                yield record

            if not response.links:
                break

            cursor = _extract_next_page_cursor(response.links.next)

            params["page[cursor]"] = cursor
