from datetime import datetime
from enum import StrEnum, auto
from pydantic import BaseModel, Field, AwareDatetime
from typing import Literal, Generic, TypeVar, Annotated, ClassVar, TYPE_CHECKING, Dict, List
import urllib.parse

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    AccessToken,
    BaseDocument,
    ResourceConfig,
    ResourceState,
)

from estuary_cdk.flow import BasicAuth


ConnectorState = GenericConnectorState[ResourceState]


class CatalogEnum(StrEnum):
    brand = "Brand"
    agency = "Agency"
    partners = "Partners"

class EndpointConfig(BaseModel, extra="allow"):
    credentials: BasicAuth = Field(
        title="API Key"        )
    api_catalog: CatalogEnum = Field(
        title="API Catalog (Brand, Agency, Partner)",
        description="As of now, only BRAND catalogs are allowed"
        
    )
    stop_date: AwareDatetime = Field(
        description="Replication Stop Date. Records will only be considered for backfilling "\
                    "before the stop_date, similar to a start date",
        default=datetime.fromisoformat("2010-01-01T00:00:00Z".replace('Z', '+00:00'))
    )


class Actions(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Actions"
    START_DATE: ClassVar[str] = ""
    START_DATE_INCREMENTAL: ClassVar[str]  = None
    END_DATE: ClassVar[str] = ""
    REP_KEY: ClassVar[str] = ""
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class ActionInquiries(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "ActionInquiries"
    START_DATE: ClassVar[str] = "StartDate"
    START_DATE_INCREMENTAL: ClassVar[str]  = None
    END_DATE: ClassVar[str] = "EndDate"
    REP_KEY: ClassVar[str] = "CreationDate"
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class Ads(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Ads"
    START_DATE: ClassVar[str] = ""
    START_DATE_INCREMENTAL: ClassVar[str]  = None
    END_DATE: ClassVar[str] = ""
    REP_KEY: ClassVar[str] = ""
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class Catalogs(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Catalogs"
    START_DATE: ClassVar[str] = ""
    START_DATE_INCREMENTAL: ClassVar[str]  = None
    END_DATE: ClassVar[str] = ""
    REP_KEY: ClassVar[str] = ""
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class Invoices(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Invoices"
    START_DATE_INCREMENTAL: ClassVar[str] = "StartDate"
    START_DATE: ClassVar[str] = "StartDate"
    END_DATE: ClassVar[str] = "EndDate"
    REP_KEY: ClassVar[str] = "CreatedDate"
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class Deals(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Deals"
    START_DATE_INCREMENTAL: ClassVar[str]  = None
    START_DATE: ClassVar[str] = ""
    END_DATE: ClassVar[str] = ""
    REP_KEY: ClassVar[str] = "StartDate"
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class TrackingValueRequests(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "TrackingValueRequests"
    START_DATE_INCREMENTAL: ClassVar[str]  = None
    START_DATE: ClassVar[str] = ""
    END_DATE: ClassVar[str] = ""
    REP_KEY: ClassVar[str] = "DatePlaced"
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class ExceptionLists(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "ExceptionLists"
    START_DATE_INCREMENTAL: ClassVar[str]  = None
    START_DATE: ClassVar[str] = ""
    END_DATE: ClassVar[str] = ""
    REP_KEY: ClassVar[str] = "CreatedDate"
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str


class Jobs(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Jobs"
    START_DATE_INCREMENTAL: ClassVar[str] = "CreatedAfter"
    START_DATE: ClassVar[str] = "CreatedAfter"
    END_DATE: ClassVar[str] = "CreatedBefore"
    REP_KEY: ClassVar[str] = "CreatedDate"
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class Campaigns(BaseDocument, extra="allow"):
    # No query parameters available, and no date replication key
    # will probably need to use snapshot
    START_DATE_INCREMENTAL: ClassVar[str]  = None
    NAME: ClassVar[str] = "Campaigns"
    START_DATE: ClassVar[str] = ""
    END_DATE: ClassVar[str] = ""
    REP_KEY: ClassVar[str] = ""
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class Reports(BaseDocument, extra="allow"):
    # No query parameters available, and no date replication key
    # will probably need to use snapshot
    NAME: ClassVar[str] = "Reports"
    START_DATE_INCREMENTAL: ClassVar[str]  = None
    START_DATE: ClassVar[str] = ""
    END_DATE: ClassVar[str] = ""
    REP_KEY: ClassVar[str] = ""
    PRIMARY_KEY: ClassVar[str] = "/_meta/row_id"

class UniqueUrls(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "UniqueUrls"
    START_DATE_INCREMENTAL: ClassVar[str]  = None
    START_DATE: ClassVar[str] = ""
    END_DATE: ClassVar[str] = ""
    REP_KEY: ClassVar[str] = "DateCreated"
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class PhoneNumbers(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "PhoneNumbers"
    START_DATE_INCREMENTAL: ClassVar[str]  = None
    START_DATE: ClassVar[str] = ""
    END_DATE: ClassVar[str] = ""
    REP_KEY: ClassVar[str] = "DateCreated"
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class PromoCodes(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "PromoCodes"
    START_DATE_INCREMENTAL: ClassVar[str] = "StartDate"
    START_DATE: ClassVar[str] = "StartDate"
    END_DATE: ClassVar[str] = "EndDate"
    REP_KEY: ClassVar[str] = "CreatedDate"
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str



### Child Streams


class Tasks(BaseDocument, extra="allow"):
    # URI example
    # https://api.impact.com/Advertisers/<AccountSID>/Programs/1000/Tasks
    NAME: ClassVar[str] = "Tasks"
    START_DATE_INCREMENTAL: ClassVar[str]  = None
    START_DATE: ClassVar[str] = ""
    END_DATE: ClassVar[str] = ""
    REP_KEY: ClassVar[str] = "DateCreated"
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class BlockRedirectRules(BaseDocument, extra="allow"):
    # URI example
    # https://api.impact.com/Advertisers/<AccountSID>/Campaigns/1000/BlockRedirectRules

    NAME: ClassVar[str] = "BlockRedirectRules"
    START_DATE_INCREMENTAL: ClassVar[str] = "DateLastUpdatedAfter"
    START_DATE: ClassVar[str] = "StartDateAfter"
    END_DATE: ClassVar[str] = "EndDateBefore"
    REP_KEY: ClassVar[str] = "DateCreated"
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class MediaPartnerGroups(BaseDocument, extra="allow"):

    NAME: ClassVar[str] = "MediaPartnerGroups"
    START_DATE_INCREMENTAL: ClassVar[str] = None
    START_DATE: ClassVar[str] = ""
    END_DATE: ClassVar[str] = ""
    REP_KEY: ClassVar[str] = ""
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class Notes(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Notes"
    START_DATE_INCREMENTAL: ClassVar[str] = "StartDate"
    START_DATE: ClassVar[str] = "StartDate"
    END_DATE: ClassVar[str] = "EndDate"
    REP_KEY: ClassVar[str] = "CreationDate"
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str

class Contracts(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Contracts"
    START_DATE_INCREMENTAL: ClassVar[str] = "DateLastUpdatedAfter"
    START_DATE: ClassVar[str] = "StartDateAfter"
    END_DATE: ClassVar[str] = "EndDateBefore"
    REP_KEY: ClassVar[str] = "DateCreated"
    PRIMARY_KEY: ClassVar[str] = "/Id"

    Id: str