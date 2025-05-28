from datetime import timedelta
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError


from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    Survey,
    SurveyQuestion
)

from .api import (
    fetch_surveys,
    fetch_survey_questions
)

def surveys(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> common.Resource:
    def open(
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
            fetch_snapshot=functools.partial(
                fetch_surveys,
                http,
                config.genesys_cloud_domain,
            )
        )

    return common.Resource(
            name='Surveys',
            key=["id"],
            model=Survey,
            open=open,
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name='surveys', interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )

def survey_questions(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> common.Resource:
    def open(
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
            fetch_snapshot=functools.partial(
                fetch_survey_questions,
                http,
                config.genesys_cloud_domain,
            )
        )

    return common.Resource(
            name='SurveyQuestions',
            key=["QuestionID"],
            model=SurveyQuestion,
            open=open,
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name='SurveyQuestions', interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )

async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    
    return [
        surveys(log, http, config),
        survey_questions(log, http, config)
    ]