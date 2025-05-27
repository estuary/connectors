from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator
import json

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPMixin

from .models import EndpointConfig, SurveysResponse, FullRefreshResource, SurveyQuestionResponse

BASE_URL = ".qualtrics.com/API/v3"

async def fetch_surveys(
        http: HTTPMixin,
        config: EndpointConfig,
        log: Logger,
        offset: PageCursor,
) -> AsyncGenerator[SurveysResponse, None]:
    base_url = f"https://{config.data_center}{BASE_URL}/surveys"

    params = {
        "offset": offset
    }

    while params["offset"] != "null":
        response = SurveysResponse.model_validate_json(
            await http.request(log, base_url, params=params)
        )

        surverys = response.result.elements

        for survey in surverys:
            yield survey

        offset = response.nextPage


async def fetch_survey_questions(
        http: HTTPMixin,
        config: EndpointConfig,
        log: Logger,
        survey_id: str
) -> AsyncGenerator[SurveyQuestionResponse, None]:
    base_url = f"https://{config.data_center}{BASE_URL}/survey-defintions"

    response = SurveyQuestionResponse.model_validate_json(
        await http.request(log, f"{base_url}/{survey_id}/questions")
    )

    questions = response.result.elements

    for question in questions:
        yield question