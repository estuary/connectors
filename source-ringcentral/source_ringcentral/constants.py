from enum import StrEnum


API_BASE_URL = "https://platform.ringcentral.com/restapi/v1.0"
OAUTH_BASE_URL = "https://platform.ringcentral.com/restapi/oauth"
PAGE_SIZE = 100
DETAILED_VIEW = "Detailed"


class RateGroup(StrEnum):
    """API Usage Plan Groups per RingCentral documentation."""

    HEAVY = "heavy"  # 10 req/min
    MEDIUM = "medium"  # 40 req/min
    LIGHT = "light"  # 50 req/min


RATE_LIMITS: dict[RateGroup, int] = {
    RateGroup.HEAVY: 10,
    RateGroup.MEDIUM: 40,
    RateGroup.LIGHT: 50,
}


class CursorField(StrEnum):
    LAST_MODIFIED_TIME = "lastModifiedTime"
    CREATION_TIME = "creationTime"
    START_TIME = "startTime"


class QueryParam(StrEnum):
    PAGE = "page"
    PER_PAGE = "perPage"
    DATE_FROM = "dateFrom"
    DATE_TO = "dateTo"
    VIEW = "view"


class Scope(StrEnum):
    READ_ACCOUNTS = "ReadAccounts"
    READ_CALL_LOG = "ReadCallLog"
    READ_MESSAGES = "ReadMessages"
    READ_CONTACTS = "ReadContacts"
    READ_ACCOUNT_CALL_LOG = "ReadAccountCallLog"
