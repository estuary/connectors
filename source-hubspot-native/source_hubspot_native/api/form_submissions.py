from logging import Logger
from typing import (
    AsyncGenerator,
)

from estuary_cdk.capture.common import (
    LogCursor,
)
from estuary_cdk.http import HTTPSession

from .forms import fetch_forms

from ..models import (
    FormSubmission,
    FormSubmissionContext,
    PageResult,
)
from .shared import (
    HUB,
)


async def _fetch_form_submissions_since(
    http: HTTPSession,
    log: Logger,
    form_id: str,
    last_submitted_at: int,
) -> AsyncGenerator[FormSubmission, None]:
    url = f"{HUB}/form-integrations/v1/submissions/forms/{form_id}"
    after: str | None = None
    params: dict[str, str | int] = {
        "limit": 50,
    }

    validation_context = FormSubmissionContext(form_id)

    while True:
        if after:
            params["after"] = after

        result = PageResult[FormSubmission].model_validate_json(
            await http.request(log, url, params=params), context=validation_context
        )

        for form_submission in result.results:
            # Form submissions are returned in reverse chronological order.
            # We can safely stop paginating once we see a submission with
            # a timestamp before the previous sweep.
            if form_submission.submittedAt > last_submitted_at:
                yield form_submission
            else:
                return

        if not result.paging:
            return

        after = result.paging.next.after


async def fetch_form_submissions(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[FormSubmission | LogCursor, None]:
    assert isinstance(log_cursor, int)
    form_ids: list[str] = []

    async for form in fetch_forms(http, log):
        form_ids.append(form.id)

    latest_submitted_at = log_cursor

    for id in form_ids:
        async for submission in _fetch_form_submissions_since(
            http, log, id, log_cursor
        ):
            if submission.submittedAt > latest_submitted_at:
                latest_submitted_at = submission.submittedAt

            yield submission

    if latest_submitted_at != log_cursor:
        yield latest_submitted_at
