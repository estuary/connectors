from datetime import UTC, datetime, timedelta
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
    dt_to_ms,
)



# HubSpot refuses to export submissions for blog_comment forms, responding 400
# FORM_TYPE_NOT_ALLOWED.
FORM_TYPES_WITHOUT_SUBMISSIONS = frozenset({"blog_comment"})

FORM_SUBMISSIONS_LAG = timedelta(minutes=5)


async def _fetch_form_submissions_between(
    http: HTTPSession,
    log: Logger,
    form_id: str,
    since: int,
    until: int,
) -> AsyncGenerator[FormSubmission, None]:
    """Yield the form's submissions with `since < submittedAt <= until`."""
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
            # Submissions newer than `until` are skipped over, and we can
            # safely stop paginating once we see a submission with a
            # timestamp at or before `since`.
            if form_submission.submittedAt > until:
                continue
            elif form_submission.submittedAt > since:
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
    """
    Emit every form's submissions in (cursor, horizon], where the horizon is
    the sweep's start less FORM_SUBMISSIONS_LAG, then checkpoint the newest
    submittedAt emitted.
    """
    assert isinstance(log_cursor, int)

    horizon = dt_to_ms(datetime.now(tz=UTC) - FORM_SUBMISSIONS_LAG)
    if horizon <= log_cursor:
        return

    form_ids: list[str] = []

    async for form in fetch_forms(http, log):
        if form.formType in FORM_TYPES_WITHOUT_SUBMISSIONS:
            continue

        form_ids.append(form.id)

    latest_submitted_at = log_cursor

    for id in form_ids:
        async for submission in _fetch_form_submissions_between(
            http, log, id, log_cursor, horizon
        ):
            if submission.submittedAt > latest_submitted_at:
                latest_submitted_at = submission.submittedAt

            yield submission

    if latest_submitted_at != log_cursor:
        yield latest_submitted_at
