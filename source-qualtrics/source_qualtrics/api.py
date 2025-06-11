import asyncio
import json
from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator, Any
from urllib.parse import urlparse, parse_qsl

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPError, HTTPMixin
from estuary_cdk.incremental_zip_processor import IncrementalZipProcessor

from .models import (
    ApiResponse,
    PaginatedResponse,
    ExportCreationRequest,
    ExportStartResponse,
    ExportStatusResponse,
    QualtricsResource,
    Survey,
    SurveyQuestion,
    SurveyResponse,
)


PAGE_SIZE = 500  # Max allowed by Qualtrics API
COMMON_HTTP_HEADERS = {
    "Content-Type": "application/json",
}


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
    http: HTTPMixin,
    data_center: str,
    log: Logger,
) -> AsyncGenerator[QualtricsResource, None]:
    url = f"https://{data_center}.qualtrics.com/API/v3/surveys"
    offset = 0

    while True:
        params: dict[str, Any] = {"offset": offset}

        api_response = ApiResponse[PaginatedResponse].model_validate_json(
            await http.request(
                log=log,
                url=url,
                params=params,
                headers=COMMON_HTTP_HEADERS,
            )
        )

        check_response_status(api_response, log)

        if not api_response.result or not api_response.result.elements:
            log.info("No surveys found.")
            return

        for item in api_response.result.elements:
            yield Survey.model_validate(item)

        if not api_response.result.nextPage:
            return

        parsed = urlparse(api_response.result.nextPage)
        params = dict(parse_qsl(parsed.query))
        offset_str = params.get("offset")

        if offset_str is None:
            log.error(
                f"No offset parameter found in nextPage URL: {api_response.result.nextPage}"
            )
            raise ValueError(
                "The API returned a nextPage URL without an offset, which is unexpected."
            )

        try:
            offset = int(offset_str)
        except ValueError:
            log.error(
                f"Invalid offset value '{offset_str}' in nextPage URL: {api_response.result.nextPage}"
            )
            raise


async def _fetch_survey_questions(
    http: HTTPMixin,
    data_center: str,
    survey_id: str,
    log: Logger,
) -> AsyncGenerator[SurveyQuestion, None]:
    url = f"https://{data_center}.qualtrics.com/API/v3/survey-definitions/{survey_id}/questions"
    api_response = ApiResponse[PaginatedResponse].model_validate_json(
        await http.request(
            log=log,
            url=url,
            headers=COMMON_HTTP_HEADERS,
        )
    )

    check_response_status(api_response, log)

    if api_response.result:
        for item in api_response.result.elements:
            item["SurveyID"] = survey_id
            survey_question = SurveyQuestion.model_validate(item)
            yield survey_question


async def snapshot_survey_questions(
    http: HTTPMixin,
    data_center: str,
    log: Logger,
) -> AsyncGenerator[QualtricsResource, None]:
    async for survey in snapshot_surveys(http, data_center, log):
        if not isinstance(survey, Survey):
            log.warning(f"Unexpected resource type: {type(survey)}")
            raise TypeError(f"Expected Survey, got {type(survey)}")
        async for question in _fetch_survey_questions(
            http,
            data_center,
            survey.id,
            log,
        ):
            yield question


async def export_survey_responses(
    http: HTTPMixin,
    log: Logger,
    data_center: str,
    survey_id: str,
    start_date: datetime,
    end_date: datetime | None,
) -> AsyncGenerator[SurveyResponse, None]:
    start = (
        start_date.isoformat() + "Z"
        if start_date.tzinfo is None
        else start_date.isoformat()
    )
    end = None
    if end_date:
        end = (
            end_date.isoformat() + "Z"
            if end_date.tzinfo is None
            else end_date.isoformat()
        )

    export_url = f"https://{data_center}.qualtrics.com/API/v3/surveys/{survey_id}/export-responses"
    # Note: Since we are using NDJSON format, we cannot use many parameters that are typically used with CSV exports.
    # This means we may need to make a request to the survey metadata or survey definition endpoints to get user-friendly labels
    # for questions that have multiple choices. By default, we will see the encoded numeric IDs for choices in the responses.
    export_request = ExportCreationRequest(
        format="ndjson",
        startDate=start,
        endDate=end,
    )

    api_response = ApiResponse[ExportStartResponse].model_validate_json(
        await http.request(
            log=log,
            url=export_url,
            method="POST",
            json=export_request.model_dump(exclude_none=True),
            headers=COMMON_HTTP_HEADERS,
        )
    )

    check_response_status(api_response, log)

    if not api_response.result or not api_response.result.progressId:
        log.warning(f"No progress ID returned for survey {survey_id} export")
        return

    progress_id = api_response.result.progressId
    status_url = f"{export_url}/{progress_id}"
    file_id = None

    while True:
        status_response = ApiResponse[ExportStatusResponse].model_validate_json(
            await http.request(
                log=log,
                url=status_url,
                headers=COMMON_HTTP_HEADERS,
            )
        )

        check_response_status(status_response, log)

        if not status_response.result:
            raise RuntimeError(f"No status returned for export {progress_id}")

        status = status_response.result.status
        percent_complete = status_response.result.percentComplete
        file_id = status_response.result.fileId

        if status == "complete" or file_id is not None:
            break
        elif status == "failed":
            raise RuntimeError(f"Export failed for survey {survey_id}")

        log.debug(f"Export progress for survey {survey_id}: {percent_complete}%")

        await asyncio.sleep(2)

    download_url = f"{export_url}/{file_id}/file"
    response_count = 0

    try:
        _, body = await http.request_stream(
            log=log,
            url=download_url,
            headers=COMMON_HTTP_HEADERS,
        )
        processor = IncrementalZipProcessor(body(), log=log)

        async for line in processor:
            try:
                # Qualtrics response does not include surveyId, so we add it manually so we can link responses to their survey
                line_json = json.loads(line)
                line_json["surveyId"] = survey_id
                response_obj = SurveyResponse.model_validate(line_json)
                response_count += 1

                yield response_obj

                if response_count % 1000 == 0:
                    log.debug(
                        f"Processed {response_count} responses from survey {survey_id}"
                    )

            except Exception as e:
                log.error(f"Failed to process response from survey {survey_id}: {e}")
                raise

    except Exception as e:
        log.error(f"Error downloading responses for survey {survey_id}: {e}")
        raise


async def fetch_survey_responses_incremental(
    http: HTTPMixin,
    data_center: str,
    window_size: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[QualtricsResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    max_end = log_cursor + timedelta(days=window_size)
    end = min(max_end, datetime.now(tz=UTC))
    max_modified_date = log_cursor
    has_docs = False

    async for survey in snapshot_surveys(http, data_center, log):
        if not isinstance(survey, Survey):
            log.warning(f"Unexpected resource type: {type(survey)}")
            raise TypeError(f"Expected Survey, got {type(survey)}")
        async for response in export_survey_responses(
            http,
            log,
            data_center,
            survey.id,
            log_cursor,
            end,
        ):
            if response.values.recordedDate > max_modified_date:
                max_modified_date = response.values.recordedDate
            yield response
            has_docs = True

    if not has_docs:
        log.warning("No survey responses found.")
        return

    yield max_modified_date + timedelta(milliseconds=1)
