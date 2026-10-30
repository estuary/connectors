import asyncio
import functools
import os
from dataclasses import dataclass
from logging import Logger
from typing import AsyncGenerator, Awaitable, Callable, Iterable

from pydantic import BaseModel

from estuary_cdk.buffer_ordered import buffer_ordered
from estuary_cdk.http import HTTPError, HTTPSession

from .errors import ACCESS_DENIED_CODES, fault_of
from .models import SANDBOX_SCOPE, SCOPE, Account, Ad, AdGroup, Campaign, ParentIds

# Microsoft sunsets Bing Ads API versions; bump this when migrating.
# See https://learn.microsoft.com/en-us/advertising/guides/migration-guide?view=bingads-13
API_VERSION = "v13"

# Setting BING_ADS_ENVIRONMENT=sandbox points the connector at Microsoft's sandbox,
# whose hosts insert `.sandbox` into the production hostname. It's for local
# development and tests only, so it isn't part of the endpoint config.
# https://learn.microsoft.com/en-us/advertising/guides/sandbox?view=bingads-13
ENVIRONMENT_VAR = "BING_ADS_ENVIRONMENT"

# Accounts/Search's maximum page size.
# https://learn.microsoft.com/en-us/advertising/customer-management-service/searchaccounts?view=bingads-13
ACCOUNTS_PAGE_SIZE = 1000

# Without every type, Campaigns/QueryByAccountId returns only Search campaigns.
# https://learn.microsoft.com/en-us/advertising/campaign-management-service/campaigntype?view=bingads-13
ALL_CAMPAIGN_TYPES = ",".join(
    [
        "Search",
        "Shopping",
        "DynamicSearchAds",
        "Audience",
        "Hotel",
        "PerformanceMax",
        "App",
        "ObjectiveBased",
    ]
)

# Ads/QueryByAdGroupId returns only the requested ad types.
# https://learn.microsoft.com/en-us/advertising/campaign-management-service/adtype?view=bingads-13
ALL_AD_TYPES = [
    "Text",
    "Image",
    "Product",
    "AppInstall",
    "ExpandedText",
    "DynamicSearch",
    "ResponsiveAd",
    "ResponsiveSearch",
    "Hotel",
]

# Bounds concurrent per-account and per-parent entity requests.
MAX_CONCURRENT_ENTITY_REQUESTS = 10


def _environment() -> str:
    environment = os.environ.get(ENVIRONMENT_VAR, "production").lower()
    if environment not in ("production", "sandbox"):
        raise RuntimeError(
            f"{ENVIRONMENT_VAR} must be 'production' or 'sandbox', got {environment!r}"
        )
    return environment


def _host(service: str) -> str:
    match _environment():
        case "sandbox":
            return f"https://{service}.api.sandbox.bingads.microsoft.com"
        case _:
            return f"https://{service}.api.bingads.microsoft.com"


def token_exchange_scope() -> str:
    """The scope the connector's own refresh-token exchanges request. The
    dashboard's sign-in always uses the production scope."""
    return SANDBOX_SCOPE if _environment() == "sandbox" else SCOPE


# The API is split across per-service hosts.
# https://learn.microsoft.com/en-us/advertising/guides/web-service-addresses?view=bingads-13
def customer_management_url() -> str:
    return f"{_host('clientcenter')}/CustomerManagement/{API_VERSION}"


def campaign_management_url() -> str:
    return f"{_host('campaign')}/CampaignManagement/{API_VERSION}"


def reporting_url() -> str:
    return f"{_host('reporting')}/Reporting/{API_VERSION}"


def by_id[T: (Account, Campaign, AdGroup, Ad)](items: list[T]) -> list[T]:
    """Sort entities by numeric Id, so snapshots are emitted in a stable order."""
    return sorted(items, key=lambda item: int(item.Id))


class User(BaseModel, extra="allow"):
    Id: str


class CustomerRole(BaseModel, extra="allow"):
    CustomerId: str
    # Null for a role that covers every account of the customer.
    AccountIds: list[str] | None = None
    LinkedAccountIds: list[str] | None = None


class UserQueryResponse(BaseModel, extra="allow"):
    User: User
    CustomerRoles: list[CustomerRole] | None = None


class AccountsSearchResponse(BaseModel, extra="allow"):
    Accounts: list[Account] | None = None


class CampaignsResponse(BaseModel, extra="allow"):
    Campaigns: list[Campaign] | None = None


class AdGroupsResponse(BaseModel, extra="allow"):
    AdGroups: list[AdGroup] | None = None


class AdsResponse(BaseModel, extra="allow"):
    Ads: list[Ad] | None = None


async def query_user(http: HTTPSession, log: Logger) -> UserQueryResponse:
    """Return the authenticated user and their customer roles. Needs no
    CustomerId or CustomerAccountId headers.

    https://learn.microsoft.com/en-us/advertising/customer-management-service/getuser?view=bingads-13
    """
    url = f"{customer_management_url()}/User/Query"
    # A null UserId means the authenticated user.
    body = {"UserId": None}
    return UserQueryResponse.model_validate_json(
        await http.request(log, url, method="POST", json=body)
    )


async def search_accounts(http: HTTPSession, log: Logger, user_id: str) -> list[Account]:
    """Every account the user can reach, deduplicated, in Id order.

    https://learn.microsoft.com/en-us/advertising/customer-management-service/searchaccounts?view=bingads-13
    """
    url = f"{customer_management_url()}/Accounts/Search"
    accounts: dict[str, Account] = {}
    index = 0
    while True:
        body = {
            "Predicates": [{"Field": "UserId", "Operator": "Equals", "Value": user_id}],
            "Ordering": [{"Field": "Id", "Order": "Ascending"}],
            "PageInfo": {"Index": index, "Size": ACCOUNTS_PAGE_SIZE},
        }
        response = AccountsSearchResponse.model_validate_json(
            await http.request(log, url, method="POST", json=body)
        )
        page = response.Accounts or []
        for account in page:
            accounts.setdefault(account.Id, account)

        # The response carries no total or has-more flag; a short page is the last.
        if len(page) < ACCOUNTS_PAGE_SIZE:
            break
        index += 1

    return by_id(list(accounts.values()))


def account_headers(customer_id: str, account_id: str) -> dict[str, str]:
    return {"CustomerId": customer_id, "CustomerAccountId": account_id}


async def _query_campaigns(
    http: HTTPSession,
    log: Logger,
    customer_id: str,
    account_id: str,
    campaign_type: str,
) -> list[Campaign]:
    """https://learn.microsoft.com/en-us/advertising/campaign-management-service/getcampaignsbyaccountid?view=bingads-13"""
    url = f"{campaign_management_url()}/Campaigns/QueryByAccountId"
    body = {"AccountId": account_id, "CampaignType": campaign_type}
    response = CampaignsResponse.model_validate_json(
        await http.request(
            log,
            url,
            method="POST",
            json=body,
            headers=account_headers(customer_id, account_id),
        ),
        context=ParentIds({"AccountId": account_id}),
    )
    return by_id(response.Campaigns or [])


@dataclass(frozen=True)
class AccountContext:
    """An account the capture reads, with the CustomerId header that reaches it."""

    account: Account
    customer_id: str

    @property
    def id(self) -> str:
        return self.account.Id

    def headers(self) -> dict[str, str]:
        return account_headers(self.customer_id, self.account.Id)


class AccountAccessError(RuntimeError):
    """One or more accounts couldn't be resolved or read. Carries every
    failure so they can be reported together."""

    def __init__(self, errors: list[str]):
        super().__init__("\n".join(errors))
        self.errors = errors


def _customer_id_candidates(
    account: Account, roles: list[CustomerRole]
) -> list[str]:
    """The CustomerId headers to try for an account, most likely first.

    Microsoft's docs disagree on which CustomerId reaches an account managed
    through a client link: the linking manager's (the role that lists it) or
    the account's owner (ParentCustomerId). For accounts the user's own
    customer owns, both are the same.
    """
    candidates: list[str] = []
    for role in roles:
        if account.Id in (role.AccountIds or []) or account.Id in (
            role.LinkedAccountIds or []
        ):
            candidates.append(role.CustomerId)
            break
    if account.ParentCustomerId not in candidates:
        candidates.append(account.ParentCustomerId)
    return candidates


async def _resolve_customer_id(
    http: HTTPSession, log: Logger, account: Account, candidates: list[str]
) -> str:
    """Return the first candidate CustomerId that can read the account.

    Each try is a cheap read with the headers the capture uses. Access-denied
    faults (105, 106) move on to the next candidate; anything else propagates.
    """
    last_error: HTTPError | None = None
    for customer_id in candidates:
        try:
            await _query_campaigns(http, log, customer_id, account.Id, "Search")
            return customer_id
        except HTTPError as err:
            fault = fault_of(err)
            if fault is None or not (fault.codes() & ACCESS_DENIED_CODES):
                raise
            last_error = err

    assert last_error is not None
    raise last_error


class AccountDirectory:
    """The accounts a capture reads, discovered once per connector run.

    Discovery fails as a whole rather than returning a partial list: an
    account missing from one run would have its entity rows deleted
    downstream.
    """

    def __init__(self, http: HTTPSession, account_ids: list[str]):
        self._http = http
        self._account_ids = account_ids
        self._lock = asyncio.Lock()
        self._accounts: list[AccountContext] | None = None

    async def accounts(self, log: Logger) -> list[AccountContext]:
        """The accounts to capture, in Id order. Raises `AccountAccessError`
        listing every account that couldn't be read."""
        async with self._lock:
            if self._accounts is None:
                self._accounts = await self._discover(log)
            return self._accounts

    async def _discover(self, log: Logger) -> list[AccountContext]:
        user = await query_user(self._http, log)
        reachable = await search_accounts(self._http, log, user.User.Id)

        if not reachable:
            raise AccountAccessError(
                ["The authenticated user can't access any Microsoft Advertising accounts."]
            )

        if self._account_ids:
            reachable_ids = {a.Id for a in reachable}
            unknown = [i for i in self._account_ids if i not in reachable_ids]
            if unknown:
                raise AccountAccessError(
                    [
                        f"The authenticated user can't access account IDs {', '.join(unknown)}. "
                        f"Accessible account IDs: {', '.join(a.Id for a in reachable)}. "
                        "Use numeric Account IDs, not 8-character account numbers."
                    ]
                )
            selected = [a for a in reachable if a.Id in set(self._account_ids)]
        else:
            selected = reachable

        roles = user.CustomerRoles or []
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_ENTITY_REQUESTS)

        async def resolve(account: Account) -> AccountContext | str:
            async with semaphore:
                try:
                    customer_id = await _resolve_customer_id(
                        self._http, log, account, _customer_id_candidates(account, roles)
                    )
                except HTTPError as err:
                    fault = fault_of(err)
                    detail = fault.describe() if fault else err.message
                    return f"Account {account.Id} ({account.Name}) can't be read: {detail}"
                return AccountContext(account=account, customer_id=customer_id)

        results = await asyncio.gather(*(resolve(a) for a in selected))
        errors = [r for r in results if isinstance(r, str)]
        if errors:
            raise AccountAccessError(errors)

        return [r for r in results if isinstance(r, AccountContext)]


async def snapshot_accounts(
    directory: AccountDirectory, log: Logger
) -> AsyncGenerator[Account, None]:
    for context in await directory.accounts(log):
        yield context.account


async def _campaigns(
    http: HTTPSession, log: Logger, context: AccountContext
) -> list[Campaign]:
    return await _query_campaigns(
        http, log, context.customer_id, context.id, ALL_CAMPAIGN_TYPES
    )


async def _ad_groups(
    http: HTTPSession, log: Logger, context: AccountContext, campaign: Campaign
) -> list[AdGroup]:
    """https://learn.microsoft.com/en-us/advertising/campaign-management-service/getadgroupsbycampaignid?view=bingads-13"""
    url = f"{campaign_management_url()}/AdGroups/QueryByCampaignId"
    response = AdGroupsResponse.model_validate_json(
        await http.request(
            log,
            url,
            method="POST",
            json={"CampaignId": campaign.Id},
            headers=context.headers(),
        ),
        context=ParentIds({"AccountId": context.id, "CampaignId": campaign.Id}),
    )
    return by_id(response.AdGroups or [])


async def _ads(
    http: HTTPSession, log: Logger, context: AccountContext, ad_group: AdGroup
) -> list[Ad]:
    """https://learn.microsoft.com/en-us/advertising/campaign-management-service/getadsbyadgroupid?view=bingads-13"""
    url = f"{campaign_management_url()}/Ads/QueryByAdGroupId"
    response = AdsResponse.model_validate_json(
        await http.request(
            log,
            url,
            method="POST",
            json={"AdGroupId": ad_group.Id, "AdTypes": ALL_AD_TYPES},
            headers=context.headers(),
        ),
        context=ParentIds(
            {
                "AccountId": context.id,
                "CampaignId": ad_group.CampaignId,
                "AdGroupId": ad_group.Id,
            }
        ),
    )
    return by_id(response.Ads or [])


async def _in_order[P, T](
    fetch: Callable[[P], Awaitable[list[T]]], parents: Iterable[P]
) -> AsyncGenerator[T, None]:
    """Fetch each parent's children concurrently, yielding them in parent order."""

    async def gen() -> AsyncGenerator[Awaitable[list[T]], None]:
        for parent in parents:
            yield fetch(parent)

    async for items in buffer_ordered(gen(), MAX_CONCURRENT_ENTITY_REQUESTS):
        for item in items:
            yield item


# The entity snapshots share one row numbering across every account, so they
# must emit in a stable order: accounts by Id, then each parent's children by Id.


async def snapshot_campaigns(
    http: HTTPSession, directory: AccountDirectory, log: Logger
) -> AsyncGenerator[Campaign, None]:
    contexts = await directory.accounts(log)
    async for campaign in _in_order(functools.partial(_campaigns, http, log), contexts):
        yield campaign


async def snapshot_ad_groups(
    http: HTTPSession, directory: AccountDirectory, log: Logger
) -> AsyncGenerator[AdGroup, None]:
    for context in await directory.accounts(log):
        campaigns = await _campaigns(http, log, context)
        async for ad_group in _in_order(
            functools.partial(_ad_groups, http, log, context), campaigns
        ):
            yield ad_group


async def snapshot_ads(
    http: HTTPSession, directory: AccountDirectory, log: Logger
) -> AsyncGenerator[Ad, None]:
    for context in await directory.accounts(log):
        for campaign in await _campaigns(http, log, context):
            ad_groups = await _ad_groups(http, log, context, campaign)
            async for ad in _in_order(functools.partial(_ads, http, log, context), ad_groups):
                yield ad
