from decimal import Decimal
from enum import StrEnum
from pydantic import BaseModel, Field, model_validator
from typing import TYPE_CHECKING

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    AccessToken,
    BaseDocument,
    BaseOAuth2Credentials,
    OAuth2Spec,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.flow import (
    GoogleServiceAccount,
    GoogleServiceAccountSpec,
)


scopes = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# TODO(johnny): Lift this string building into higher-order helpers.
OAUTH2_SPEC = OAuth2Spec(
    provider="google",
    authUrlTemplate=(
        "https://accounts.google.com/o/oauth2/auth?"
        "access_type=offline&"
        "prompt=consent&"
        "client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&"
        "redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&"
        "response_type=code&"
        f"scope={" ".join(scopes)}&"
        "state={{#urlencode}}{{{ state }}}{{/urlencode}}"
    ),
    accessTokenUrlTemplate="https://oauth2.googleapis.com/token",
    accessTokenBody=(
        '{"grant_type": "authorization_code", "client_id": "{{{ client_id }}}", "client_secret": "{{{ client_secret }}}", "redirect_uri": "{{{ redirect_uri }}}", "code": "{{{ code }}}"}'
    ),
    accessTokenHeaders={"content-type":"application/json"},
    accessTokenResponseMap={"refresh_token": "/refresh_token"},
)


if TYPE_CHECKING:
    OAuth2Credentials = BaseOAuth2Credentials
else:
    OAuth2Credentials = BaseOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)


GOOGLE_SPEC = GoogleServiceAccountSpec(
    scopes=scopes,
)

class EndpointConfig(BaseModel):
    credentials: OAuth2Credentials | GoogleServiceAccount | AccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    spreadsheet_url: str = Field(
        description="URL of the Google Spreadsheet",
        pattern="^https://docs.google.com/spreadsheets/",
    )


# We use ResourceState directly, without extending it.
ConnectorState = GenericConnectorState[ResourceState]


class NumberType(StrEnum):
    TEXT = "TEXT"
    NUMBER = "NUMBER"
    TIME = "TIME"
    CURRENCY = "CURRENCY"
    DATE = "DATE"
    DATE_TIME = "DATE_TIME"
    PERCENT = "PERCENT"
    SCIENTIFIC = "SCIENTIFIC"


class RowData(BaseModel, extra="forbid"):
    """
    Models the data of a Google Spreadsheet Row.
    """

    class EffectiveValue(BaseModel, extra="forbid"):
        stringValue: str | None = None
        numberValue: Decimal | None = None
        boolValue: bool | None = None
        errorValue: dict | None = None

    class EffectiveFormat(BaseModel, extra="forbid"):
        numberFormat: "RowData.NumberFormat"

    class NumberFormat(BaseModel, extra="forbid"):
        type: NumberType

    class Value(BaseModel, extra="forbid"):
        effectiveFormat: "RowData.EffectiveFormat | None" = None
        effectiveValue: "RowData.EffectiveValue | None" = None

    values: list[Value] | None = None


class Sheet(BaseModel, extra="allow"):
    """
    Models the metadata of a Google Spreadsheet Sheet.
    See: https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/sheets
    """

    class Properties(BaseModel, extra="allow"):
        class Grid(BaseModel, extra="forbid"):
            rowCount: int
            columnCount: int
            frozenRowCount: int = 0
            frozenColumnCount: int = 0
            hideGridlines: bool = False
            rowGroupControlAfter: bool = False
            columnGroupControlAfter: bool = False

        sheetId: int
        title: str
        index: int
        sheetType: str
        gridProperties: Grid

    properties: Properties


class Spreadsheet(BaseModel):
    """
    Models a Google Spreadsheet.
    See: https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/sheets
    """

    class Properties(BaseModel):
        title: str
        locale: str
        autoRecalc: str
        timeZone: str

    properties: Properties

    sheets: list[Sheet]


class Row(BaseDocument, extra="allow"):
    pass
