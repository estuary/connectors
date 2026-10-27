import os
from logging import Logger

from pydantic import BaseModel

from estuary_cdk.http import HTTPSession

# Microsoft sunsets Bing Ads API versions; bump this when migrating.
# See https://learn.microsoft.com/en-us/advertising/guides/migration-guide?view=bingads-13
API_VERSION = "v13"

# Setting BING_ADS_ENVIRONMENT=sandbox points the connector at Microsoft's sandbox,
# whose hosts insert `.sandbox` into the production hostname. It's for local
# development and tests only, so it isn't part of the endpoint config.
# https://learn.microsoft.com/en-us/advertising/guides/sandbox?view=bingads-13
ENVIRONMENT_VAR = "BING_ADS_ENVIRONMENT"


def _host(service: str) -> str:
    environment = os.environ.get(ENVIRONMENT_VAR, "production").lower()
    match environment:
        case "production":
            return f"https://{service}.api.bingads.microsoft.com"
        case "sandbox":
            return f"https://{service}.api.sandbox.bingads.microsoft.com"
        case _:
            raise RuntimeError(
                f"{ENVIRONMENT_VAR} must be 'production' or 'sandbox', got {environment!r}"
            )


# The API is split across per-service hosts.
# https://learn.microsoft.com/en-us/advertising/guides/web-service-addresses?view=bingads-13
def customer_management_url() -> str:
    return f"{_host('clientcenter')}/CustomerManagement/{API_VERSION}"


def campaign_management_url() -> str:
    return f"{_host('campaign')}/CampaignManagement/{API_VERSION}"


def reporting_url() -> str:
    return f"{_host('reporting')}/Reporting/{API_VERSION}"


class User(BaseModel, extra="allow"):
    Id: int


class UserQueryResponse(BaseModel, extra="allow"):
    User: User


async def query_user(http: HTTPSession, log: Logger) -> User:
    """Return the authenticated user. Needs no CustomerId or CustomerAccountId headers.

    https://learn.microsoft.com/en-us/advertising/customer-management-service/getuser?view=bingads-13
    """
    url = f"{customer_management_url()}/User/Query"
    # A null UserId means the authenticated user.
    body = {"UserId": None}
    response = UserQueryResponse.model_validate_json(
        await http.request(log, url, method="POST", json=body)
    )
    return response.User
