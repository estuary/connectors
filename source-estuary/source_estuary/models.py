from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from enum import StrEnum
from typing import Annotated, Any, ClassVar, Literal, Self

from pydantic import AwareDatetime, BaseModel, Field

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken


class ApiKey(AccessToken):
    credentials_title: Literal["API Key"] = Field(
        default="API Key", json_schema_extra={"type": "string", "order": 0}
    )
    access_token: str = Field(
        title="API Key",
        description="An Estuary service account API key or refresh token.",
        json_schema_extra={"secret": True, "order": 1},
    )


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    prefixes: list[str] = Field(
        description="Catalog prefixes to capture, each ending in '/' (e.g. acmeCo/). If left empty, every prefix the API key can read is captured.",
        title="Prefixes",
        default_factory=list,
    )

    class Advanced(BaseModel):
        catalog_stats_lookback_hours: Annotated[int, Field(
            description="Hours before the cursor that every catalog stats sweep re-reads, so buckets that keep accruing after they close are captured in full. Estuary's stats pipeline normally settles within minutes; raise this only to recover from a stats pipeline delay.",
            title="Catalog Stats Lookback (Hours)",
            default=24,
            ge=1,
            le=8760,
        )]

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]



class CapabilityBit(StrEnum):
    CatalogRead = "CatalogRead"
    JournalRead = "JournalRead"
    JournalAppend = "JournalAppend"
    SpecEdit = "SpecEdit"
    CreateGrant = "CreateGrant"
    DeleteGrant = "DeleteGrant"
    CreateInviteLink = "CreateInviteLink"
    ViewDataPlanePrivateNetworking = "ViewDataPlanePrivateNetworking"
    ModifyDataPlanePrivateNetworking = "ModifyDataPlanePrivateNetworking"
    ViewBilling = "ViewBilling"
    EditBilling = "EditBilling"
    QueryServiceAccounts = "QueryServiceAccounts"
    CreateServiceAccount = "CreateServiceAccount"
    CreateApiKey = "CreateApiKey"
    RevokeApiKey = "RevokeApiKey"
    ViewSecret = "ViewSecret"
    EditSecret = "EditSecret"
    DecryptSecret = "DecryptSecret"
    Delegate = "Delegate"
    Assume = "Assume"
    CreateSandbox = "CreateSandbox"


# Resolvers that still check the legacy `Read` / `Admin` capability require
# every bit of the matching bundle. Mirrors `CapabilityBundle` in flow's
# crates/models/src/authz.rs.
READ_BUNDLE = frozenset({
    CapabilityBit.CatalogRead,
    CapabilityBit.JournalRead,
    CapabilityBit.ViewDataPlanePrivateNetworking,
})
ADMIN_BUNDLE = frozenset({
    CapabilityBit.CatalogRead,
    CapabilityBit.JournalRead,
    CapabilityBit.JournalAppend,
    CapabilityBit.SpecEdit,
    CapabilityBit.Delegate,
    CapabilityBit.ViewSecret,
    CapabilityBit.EditSecret,
    CapabilityBit.DecryptSecret,
    CapabilityBit.CreateGrant,
    CapabilityBit.DeleteGrant,
    CapabilityBit.CreateInviteLink,
    CapabilityBit.QueryServiceAccounts,
    CapabilityBit.CreateServiceAccount,
    CapabilityBit.CreateApiKey,
    CapabilityBit.RevokeApiKey,
    CapabilityBit.ViewBilling,
    CapabilityBit.EditBilling,
    CapabilityBit.ViewDataPlanePrivateNetworking,
    CapabilityBit.ModifyDataPlanePrivateNetworking,
})


class AuthorizedPrefix(BaseModel, extra="allow"):
    prefix: str
    capabilities: list[str]


class Pager(StrEnum):
    # `first`/`after`, continuing while `hasNextPage`.
    FORWARD = "forward"
    # `last`/`before`, continuing from `endCursor` while `hasPreviousPage`.
    BACKWARD = "backward"
    # An unpaginated list.
    LIST = "list"


class Scope(StrEnum):
    # One query per planned prefix, passed as `$scope`.
    PREFIX = "prefix"
    # One query per tenant, passed as `$tenant`.
    TENANT = "tenant"
    # One unscoped query, filtered client-side to the planned prefixes.
    GLOBAL_FILTERED = "global_filtered"
    # One unscoped query.
    GLOBAL = "global"


class EstuarySnapshot(BaseDocument, extra="allow"):
    name: ClassVar[str]
    query: ClassVar[str]
    connection_path: ClassVar[tuple[str, ...]]
    pager: ClassVar[Pager]
    scope: ClassVar[Scope]
    required: ClassVar[frozenset[CapabilityBit]] = frozenset()
    interval: ClassVar[timedelta] = timedelta(minutes=5)

    def identity(self) -> tuple[Any, ...]:
        raise NotImplementedError

    def in_scope(self, scopes: list[str]) -> bool:
        return True


class Alerts(EstuarySnapshot):
    name: ClassVar[str] = "alerts"
    query: ClassVar[str] = """
query Alerts($scope: String!, $last: Int!, $before: String) {
  alerts(by: { prefix: $scope }, last: $last, before: $before) {
    pageInfo { hasPreviousPage endCursor }
    edges { node { alertType catalogName firedAt resolvedAt arguments resolvedArguments } }
  }
}
"""
    connection_path: ClassVar[tuple[str, ...]] = ("alerts",)
    pager: ClassVar[Pager] = Pager.BACKWARD
    scope: ClassVar[Scope] = Scope.PREFIX
    required: ClassVar[frozenset[CapabilityBit]] = READ_BUNDLE

    alertType: str
    catalogName: str
    firedAt: AwareDatetime

    def identity(self) -> tuple[Any, ...]:
        return (self.firedAt, self.alertType, self.catalogName)


class AlertConfigs(EstuarySnapshot):
    name: ClassVar[str] = "alert_configs"
    query: ClassVar[str] = """
query AlertConfigs($scope: String!, $first: Int!, $after: String) {
  alertConfigs(filter: { catalogPrefixOrName: { startsWith: $scope } }, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    edges { node { id catalogPrefixOrName config detail createdAt updatedAt lastModifiedBy } }
  }
}
"""
    connection_path: ClassVar[tuple[str, ...]] = ("alertConfigs",)
    pager: ClassVar[Pager] = Pager.FORWARD
    scope: ClassVar[Scope] = Scope.PREFIX
    required: ClassVar[frozenset[CapabilityBit]] = READ_BUNDLE

    id: str
    catalogPrefixOrName: str

    def identity(self) -> tuple[Any, ...]:
        return (self.catalogPrefixOrName, self.id)


class AlertSubscriptions(EstuarySnapshot):
    name: ClassVar[str] = "alert_subscriptions"
    query: ClassVar[str] = """
query AlertSubscriptions($scope: Prefix!) {
  alertSubscriptions(by: { prefix: $scope }) {
    catalogPrefix alertTypes createdAt updatedAt detail email destination
  }
}
"""
    connection_path: ClassVar[tuple[str, ...]] = ("alertSubscriptions",)
    pager: ClassVar[Pager] = Pager.LIST
    scope: ClassVar[Scope] = Scope.PREFIX
    required: ClassVar[frozenset[CapabilityBit]] = ADMIN_BUNDLE

    catalogPrefix: str
    destination: str

    def identity(self) -> tuple[Any, ...]:
        return (self.catalogPrefix, self.destination)


class StorageMappings(EstuarySnapshot):
    name: ClassVar[str] = "storage_mappings"
    query: ClassVar[str] = """
query StorageMappings($scope: String!, $first: Int!, $after: String) {
  storageMappings(filter: { catalogPrefix: { startsWith: $scope } }, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    edges { node {
      catalogPrefix detail
      dataPlanes { name }
      fragmentStores { provider prefix bucket region storageAccountName containerName accountTenantId endpoint }
    } }
  }
}
"""
    connection_path: ClassVar[tuple[str, ...]] = ("storageMappings",)
    pager: ClassVar[Pager] = Pager.FORWARD
    scope: ClassVar[Scope] = Scope.PREFIX
    required: ClassVar[frozenset[CapabilityBit]] = frozenset({CapabilityBit.CatalogRead})

    catalogPrefix: str

    def identity(self) -> tuple[Any, ...]:
        return (self.catalogPrefix,)


class DataPlanes(EstuarySnapshot):
    name: ClassVar[str] = "data_planes"
    query: ClassVar[str] = """
query DataPlanes($first: Int!, $after: String) {
  dataPlanes(first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    edges { node {
      id name fqdn reactorAddress cloudProvider region tag isPublic closed cidrBlocks
      gcpServiceAccountEmail awsIamUserArn azureApplicationName azureApplicationClientId
    } }
  }
}
"""
    connection_path: ClassVar[tuple[str, ...]] = ("dataPlanes",)
    pager: ClassVar[Pager] = Pager.FORWARD
    scope: ClassVar[Scope] = Scope.GLOBAL_FILTERED

    id: str
    name_: str = Field(alias="name")
    isPublic: bool

    def identity(self) -> tuple[Any, ...]:
        return (self.name_, self.id)

    def in_scope(self, scopes: list[str]) -> bool:
        # Private data planes are named `ops/dp/private/<tenant>/...`.
        tenants = {s.split("/")[0] + "/" for s in scopes}
        return self.isPublic or any(
            self.name_.startswith(f"ops/dp/private/{t}") for t in tenants
        )


class ServiceAccounts(EstuarySnapshot):
    name: ClassVar[str] = "service_accounts"
    query: ClassVar[str] = """
query ServiceAccounts($first: Int!, $after: String) {
  serviceAccounts(first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    edges { node {
      catalogName createdByEmail createdAt lastUsedAt
      grants { prefix capability detail createdAt updatedAt }
      apiKeys { id detail createdByEmail createdAt expiresAt lastUsedAt }
    } }
  }
}
"""
    connection_path: ClassVar[tuple[str, ...]] = ("serviceAccounts",)
    pager: ClassVar[Pager] = Pager.FORWARD
    scope: ClassVar[Scope] = Scope.GLOBAL_FILTERED
    required: ClassVar[frozenset[CapabilityBit]] = frozenset({CapabilityBit.QueryServiceAccounts})

    catalogName: str

    def identity(self) -> tuple[Any, ...]:
        return (self.catalogName,)

    def in_scope(self, scopes: list[str]) -> bool:
        return any(self.catalogName.startswith(s) for s in scopes)


class Connectors(EstuarySnapshot):
    name: ClassVar[str] = "connectors"
    query: ClassVar[str] = """
query Connectors($first: Int!, $after: String) {
  connectors(first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    edges { node {
      id imageName title detail protocol recommended externalUrl logoUrl createdAt defaultImageTag
    } }
  }
}
"""
    connection_path: ClassVar[tuple[str, ...]] = ("connectors",)
    pager: ClassVar[Pager] = Pager.FORWARD
    scope: ClassVar[Scope] = Scope.GLOBAL

    id: str

    def identity(self) -> tuple[Any, ...]:
        return (self.id,)


class Invoices(EstuarySnapshot):
    name: ClassVar[str] = "invoices"
    # Stripe-backed fields (status, amountDue, invoicePdf, hostedInvoiceUrl,
    # paymentDetails) are left out: the API fetches them live from Stripe per
    # invoice, which trips Estuary's account-wide Stripe rate limit.
    query: ClassVar[str] = """
query Invoices($tenant: String!, $first: Int!, $after: String) {
  tenant(name: $tenant) {
    billing {
      invoices(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        edges { node { dateStart dateEnd invoiceType subtotal lineItems extra } }
      }
    }
  }
}
"""
    connection_path: ClassVar[tuple[str, ...]] = ("tenant", "billing", "invoices")
    pager: ClassVar[Pager] = Pager.FORWARD
    scope: ClassVar[Scope] = Scope.TENANT
    required: ClassVar[frozenset[CapabilityBit]] = frozenset({CapabilityBit.ViewBilling})
    # Invoices change about daily, and `extra` is large.
    interval: ClassVar[timedelta] = timedelta(hours=1)

    # Not part of the Invoice node; stamped from the query's tenant.
    tenant: str
    dateStart: str
    dateEnd: str
    invoiceType: str

    def identity(self) -> tuple[Any, ...]:
        return (self.tenant, self.dateStart, self.dateEnd, self.invoiceType)


class LiveSpecRefDocument(BaseDocument, extra="allow"):
    """A document derived from one `LiveSpecRef` node of a `liveSpecs` walk."""

    name: ClassVar[str]
    # Selection inside `node { catalogName <here> }`.
    node_selection: ClassVar[str]
    # Fragment definitions referenced by `node_selection`.
    fragments: ClassVar[str] = ""

    @classmethod
    def from_ref(cls, node: dict[str, Any]) -> Self | None:
        """Build the document, or None when the ref is a deleted spec awaiting hard-deletion."""
        raise NotImplementedError


def _edge_names(connection: dict[str, Any] | None) -> list[str] | None:
    if connection is None:
        return None
    return [edge["node"]["catalogName"] for edge in connection["edges"]]


class LiveSpec(LiveSpecRefDocument):
    name: ClassVar[str] = "live_specs"
    # `model` is left out because it carries each task's SOPS-encrypted
    # endpoint config, and `builtSpec` because it is large and internal.
    node_selection: ClassVar[str] = """
      liveSpec {
        liveSpecId catalogName catalogType lastBuildId lastPubId
        createdAt updatedAt isDisabled dataPlaneId
        readsFrom { edges { node { catalogName } } }
        writesTo { edges { node { catalogName } } }
        sourceCapture { catalogName }
      }"""

    liveSpecId: str
    catalogName: str
    catalogType: str
    createdAt: AwareDatetime
    updatedAt: AwareDatetime
    readsFrom: list[str] | None
    writesTo: list[str] | None
    sourceCapture: str | None

    @classmethod
    def from_ref(cls, node: dict[str, Any]) -> Self | None:
        spec = node["liveSpec"]
        if spec is None:
            return None
        return cls.model_validate({
            **spec,
            "readsFrom": _edge_names(spec["readsFrom"]),
            "writesTo": _edge_names(spec["writesTo"]),
            "sourceCapture": (spec["sourceCapture"] or {}).get("catalogName"),
        })


class TaskStatus(LiveSpecRefDocument):
    name: ClassVar[str] = "task_status"
    node_selection: ClassVar[str] = "status { ...TaskStatusFields }"
    fragments: ClassVar[str] = """
fragment Shard on ShardRef { name keyBegin rClockBegin build }
fragment Job on JobStatus { type lockFailures { catalogName expected actual } }
fragment Err on Error { catalogName scope detail }
fragment Change on DiscoverChange { resourcePath target disable }
fragment Outcome on AutoDiscoverOutcome {
  ts added { ...Change } modified { ...Change } removed { ...Change } errors { ...Err } publishResult { ...Job }
}
fragment TaskStatusFields on LiveSpecStatus {
  type summary
  connector { shard { ...Shard } ts message fields }
  controller {
    nextRun error failures updatedAt alerts
    activation { lastActivated lastActivatedAt recentFailureCount nextRetry
      shardStatus { count lastTs firstTs status }
      lastFailure { shard { ...Shard } ts message fields } }
    publications { nextAfter maxObservedPubId
      history { id created completed detail isTouch count result { ...Job } errors { ...Err } }
      pendingRepublish { receivedAt reason lastBuildId } }
    autoDiscover { nextAt pendingPublish { ...Outcome } lastSuccess { ...Outcome }
      failure { count firstTs lastOutcome { ...Outcome } } }
    sourceCapture { upToDate addBindings }
    inferredSchema { schemaLastUpdated schemaMd5 nextMd5 nextUpdateAfter }
    configUpdate { nextAttempt build }
    abandon { lastEvaluated }
  }
}
"""

    catalogName: str
    # StatusSummaryType: OK, TASK_DISABLED, WARNING or ERROR.
    type: str
    summary: str

    @classmethod
    def from_ref(cls, node: dict[str, Any]) -> Self | None:
        status = node["status"]
        if status is None:
            return None
        return cls.model_validate({"catalogName": node["catalogName"], **status})


class PublicationHistoryItem(BaseDocument, extra="allow"):
    name: ClassVar[str] = "publication_history"
    # `model` (a full spec copy per publication, with SOPS ciphertext) is left out.
    node_selection: ClassVar[str] = "publicationId catalogType publishedAt userId userEmail userFullName detail"

    # Not part of the history node; stamped from the parent spec.
    catalogName: str
    publicationId: str
    publishedAt: AwareDatetime


@dataclass(frozen=True)
class Grain:
    api_value: Literal["HOURLY", "DAILY", "MONTHLY"]
    stream_suffix: str
    interval: timedelta

    @property
    def stream_name(self) -> str:
        return f"catalog_stats_{self.stream_suffix}"

    def floor(self, dt: datetime) -> datetime:
        dt = dt.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
        if self.api_value != "HOURLY":
            dt = dt.replace(hour=0)
        if self.api_value == "MONTHLY":
            dt = dt.replace(day=1)
        return dt

    def add(self, dt: datetime, n: int) -> datetime:
        match self.api_value:
            case "HOURLY":
                return dt + timedelta(hours=n)
            case "DAILY":
                return dt + timedelta(days=n)
            case "MONTHLY":
                months = dt.year * 12 + dt.month - 1 + n
                return dt.replace(year=months // 12, month=months % 12 + 1)


CATALOG_STATS_GRAINS: list[Grain] = [
    Grain("HOURLY", "hourly", timedelta(minutes=15)),
    Grain("DAILY", "daily", timedelta(hours=1)),
    Grain("MONTHLY", "monthly", timedelta(hours=1)),
]


# UInt64 counters arrive as decimal strings and are coerced to int so schema
# inference types them as integers.
class DocsAndBytes(BaseModel, extra="allow"):
    docsTotal: int
    bytesTotal: int


class CatalogStatsSummary(BaseModel, extra="allow"):
    readByMe: DocsAndBytes
    readFromMe: DocsAndBytes
    writtenByMe: DocsAndBytes
    writtenToMe: DocsAndBytes
    warnings: int
    errors: int
    failures: int
    usageSeconds: int
    txnCount: int
    lastPublishedAt: AwareDatetime | None = None


class CaptureBindingStats(BaseModel, extra="allow"):
    collection: str
    right: DocsAndBytes | None = None
    out: DocsAndBytes | None = None
    lastPublishedAt: AwareDatetime | None = None


class MaterializeBindingStats(BaseModel, extra="allow"):
    collection: str
    left: DocsAndBytes | None = None
    right: DocsAndBytes | None = None
    out: DocsAndBytes | None = None
    lastSourcePublishedAt: AwareDatetime | None = None
    bytesBehind: int


class DeriveTransformStats(BaseModel, extra="allow"):
    transform: str
    source: str
    input: DocsAndBytes | None = None
    lastSourcePublishedAt: AwareDatetime | None = None
    bytesBehind: int


class DeriveStats(BaseModel, extra="allow"):
    transforms: list[DeriveTransformStats]
    published: DocsAndBytes | None = None
    out: DocsAndBytes | None = None
    lastPublishedAt: AwareDatetime | None = None


class CatalogTaskStats(BaseModel, extra="allow"):
    capture: list[CaptureBindingStats]
    derive: DeriveStats | None = None
    materialize: list[MaterializeBindingStats]


class CatalogStats(BaseDocument, extra="allow"):
    query: ClassVar[str] = """
query CatalogStats($names: [String!]!, $grain: CatalogStatsGrain!, $start: DateTime!, $end: DateTime!) {
  catalogStats(by: { names: $names, grain: $grain, start: $start, end: $end }) {
    edges { node {
      catalogName grain timestamp
      statsSummary {
        readByMe { docsTotal bytesTotal } readFromMe { docsTotal bytesTotal }
        writtenByMe { docsTotal bytesTotal } writtenToMe { docsTotal bytesTotal }
        warnings errors failures usageSeconds txnCount lastPublishedAt
      }
      taskStats {
        capture { collection right { docsTotal bytesTotal } out { docsTotal bytesTotal } lastPublishedAt }
        derive {
          transforms { transform source input { docsTotal bytesTotal } lastSourcePublishedAt bytesBehind }
          published { docsTotal bytesTotal } out { docsTotal bytesTotal } lastPublishedAt
        }
        materialize {
          collection left { docsTotal bytesTotal } right { docsTotal bytesTotal }
          out { docsTotal bytesTotal } lastSourcePublishedAt bytesBehind
        }
      }
    } }
  }
}
"""
    # catalogStats accepts at most 100 names, and errors past 10,000 buckets.
    names_per_query: ClassVar[int] = 100
    # Rows per query, kept well under the cap because task rows carry
    # per-binding stats.
    bucket_budget: ClassVar[int] = 2_000

    catalogName: str
    grain: Literal["HOURLY", "DAILY", "MONTHLY"]
    timestamp: AwareDatetime
    statsSummary: CatalogStatsSummary
    # Null on prefix rollup rows and plain collection rows.
    taskStats: CatalogTaskStats | None = None


LIVE_SPEC_SNAPSHOT_STREAMS: list[type[LiveSpecRefDocument]] = [LiveSpec, TaskStatus]

SNAPSHOT_STREAMS: list[type[EstuarySnapshot]] = [
    Alerts,
    AlertConfigs,
    AlertSubscriptions,
    StorageMappings,
    DataPlanes,
    ServiceAccounts,
    Connectors,
    Invoices,
]
