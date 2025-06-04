import asyncio
import json
from datetime import datetime
from logging import Logger
from typing import AsyncGenerator, Any
from urllib.parse import urlparse, parse_qsl

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPSession, HTTPError

from .models import (
    EndpointConfig,
    ApiResponse,
    PaginatedResponse,
    QuestionsResponse,
    ExportCreationRequest,
    ExportStartResponse,
    ExportStatusResponse,
    QualtricsResource,
    Survey,
    SurveyQuestion,
    SurveyResponse,
)


PAGE_SIZE = 500  # Max allowed by Qualtrics API


def check_response_status(api_response: ApiResponse, log: Logger) -> None:
    if not api_response.meta or not api_response.meta.httpStatus:
        return

    status_str = api_response.meta.httpStatus
    try:
        status_code = int(status_str.split()[0])
        if status_code >= 400:
            error_msg = f"Qualtrics API error: HTTP {status_code}"
            if api_response.meta.error:
                error_msg = api_response.meta.error.get("errorMessage", error_msg)

            raise HTTPError(code=status_code, message=error_msg)
    except (ValueError, IndexError):
        log.warning(f"Could not parse HTTP status: '{status_str}'")
        raise


async def snapshot_surveys(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[QualtricsResource, None]:
    headers = {
        "X-API-TOKEN": config.credentials.access_token,
        "Content-Type": "application/json",
    }

    url = f"{config.base_url}/surveys"
    offset = 0

    while True:
        params: dict[str, Any] = {"offset": offset}

        api_response = ApiResponse[PaginatedResponse].model_validate(
            await http.request(
                log=log,
                url=url,
                params=params,
                headers=headers,
            )
        )

        check_response_status(api_response, log)

        if api_response.result:
            for item in api_response.result.elements:
                yield Survey.model_validate(item)

            if not api_response.result.nextPage:
                break

            parsed = urlparse(api_response.result.nextPage)
            params = dict(parse_qsl(parsed.query))
            offset = params.get("offset")


async def _fetch_survey_questions(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
    survey_id: str,
) -> AsyncGenerator[SurveyQuestion, None]:
    headers = {
        "X-API-TOKEN": config.credentials.access_token,
        "Content-Type": "application/json",
    }

    url = f"{config.base_url}/surveys/{survey_id}/questions"
    api_response = ApiResponse[QuestionsResponse].model_validate(
        await http.request(log=log, url=url, headers=headers)
    )

    check_response_status(api_response, log)

    if not api_response.result or not api_response.result.questions:
        log.warning(f"No questions found for survey {survey_id}")
        return

    for question_id, question_data in api_response.result.questions.items():
        yield SurveyQuestion.model_validate(question_data)


async def snapshot_survey_questions(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[QualtricsResource, None]:
    async for survey in snapshot_surveys(http, config, log):
        if not isinstance(survey, Survey):
            log.warning(f"Unexpected resource type: {type(survey)}")
            raise TypeError(f"Expected Survey, got {type(survey)}")
        async for question in _fetch_survey_questions(http, config, log, survey.id):
            yield question


async def export_survey_responses(
    http: HTTPSession,
    config: EndpointConfig,
    survey_id: str,
    log: Logger,
    start_date: datetime,
) -> AsyncGenerator[SurveyResponse, None]:
    headers = {
        "X-API-TOKEN": config.credentials.access_token,
        "Content-Type": "application/json",
    }

    export_url = f"{config.base_url}/surveys/{survey_id}/export-responses"
    # Note: Since we are using NDJSON format, we cannot use many parameters that are typically used with CSV exports.
    # This means we may need to make a request to the survey metadata or survey definition endpoints to get user-friendly labels
    # for questions that have multiple choices. By default, we will see the encoded numeric IDs for choices in the responses.
    export_request = ExportCreationRequest(
        format="ndjson",
        startDate=start_date.isoformat() + "Z"
        if start_date.tzinfo is None
        else start_date.isoformat(),
    )

    api_response = ApiResponse[ExportStartResponse].model_validate(
        await http.request(
            log=log,
            url=export_url,
            method="POST",
            json=export_request.model_dump(exclude_none=True),
            headers=headers,
        )
    )

    check_response_status(api_response, log)

    if not api_response.result or not api_response.result.progressId:
        log.warning(f"No progress ID returned for survey {survey_id} export")
        return

    progress_id = api_response.result.progressId
    status_url = f"{export_url}/{progress_id}"

    while True:
        status_response = ApiResponse[ExportStatusResponse].model_validate(
            await http.request(
                log=log,
                url=status_url,
                headers=headers,
            )
        )

        check_response_status(status_response, log)

        if not status_response.result:
            raise RuntimeError(f"No status returned for export {progress_id}")

        status = status_response.result.status
        percent_complete = status_response.result.percentComplete

        if status == "complete":
            break
        elif status == "failed":
            raise RuntimeError(f"Export failed for survey {survey_id}")

        log.debug(f"Export progress for survey {survey_id}: {percent_complete}%")

        await asyncio.sleep(2)

    download_url = f"{status_url}/file"
    response_count = 0

    try:
        _, body = await http.request_lines(
            log=log,
            url=download_url,
            headers=headers,
        )

        # We can potentially have issues with this processing logic if the aiohttp.ClientSession does not automatically handle
        # decompression for the response. If we encounter issues, we may need to handle decompression manually which is more complicated,
        # especially if we want to do this in a streaming fashion rather than loading the entire response into memory. Compression
        # is needed since Qualtrics will reject requests that exceed 1.8GB in size. Qualtrics docs also mentions they may return compressed responses
        # even if the request does not specify compression if the response is large enough.
        async for line in body():
            try:
                # Qualtrics response does not include surveyId, so we add it manually
                response_data = json.loads(line)
                response_data["surveyId"] = survey_id

                response_obj = SurveyResponse.model_validate(response_data)
                response_count += 1

                yield response_obj

                if response_count % 1000 == 0:
                    log.debug(
                        f"Processed {response_count} responses from survey {survey_id}"
                    )

            except Exception as e:
                log.error(f"Failed to parse response line from survey {survey_id}: {e}")
                raise

        log.info(f"Exported {response_count} responses from survey {survey_id}")

    except Exception as e:
        log.error(f"Error downloading responses for survey {survey_id}: {e}")
        raise


async def fetch_survey_responses_incremental(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[QualtricsResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    max_modified_date = log_cursor

    async for survey in snapshot_surveys(http, config, log):
        if not isinstance(survey, Survey):
            log.warning(f"Unexpected resource type: {type(survey)}")
            raise TypeError(f"Expected Survey, got {type(survey)}")
        async for response in export_survey_responses(
            http, config, survey.id, log, log_cursor
        ):
            if response.recordedDate > max_modified_date:
                max_modified_date = response.recordedDate
            yield response

    yield max_modified_date
