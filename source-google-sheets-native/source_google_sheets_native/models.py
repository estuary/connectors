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


# TODO(johnny): Lift this string building into higher-order helpers.
OAUTH2_SPEC = OAuth2Spec(
    provider="google",
    authUrlTemplate="https://accounts.google.com/o/oauth2/auth?access_type=offline&prompt=consent&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&response_type=code&scope=https://www.googleapis.com/auth/spreadsheets.readonly https://www.googleapis.com/auth/drive.readonly&state={{#urlencode}}{{{ state }}}{{/urlencode}}",
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


class EndpointConfig(BaseModel):
    credentials: OAuth2Credentials | AccessToken = Field(
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


class Sheet(BaseModel, extra="allow"):
    """
    Models a Google Spreadsheet Sheet.
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

    class EffectiveValue(BaseModel, extra="forbid"):
        stringValue: str | None = None
        numberValue: Decimal | None = None
        boolValue: bool | None = None
        errorValue: dict | None = None

    class EffectiveFormat(BaseModel, extra="forbid"):
        numberFormat: "Sheet.NumberFormat"

    class NumberFormat(BaseModel, extra="forbid"):
        type: NumberType

    class Value(BaseModel, extra="forbid"):
        effectiveFormat: "Sheet.EffectiveFormat | None" = None
        effectiveValue: "Sheet.EffectiveValue | None" = None

    class RowData(BaseModel, extra="forbid"):
        values: list["Sheet.Value"] | None = None

    class Data(BaseModel, extra="forbid"):
        rowData: list["Sheet.RowData"] | None = None

        @model_validator(mode="after")
        def _post_init(self) -> "Sheet.Data":
            # Remove all trailing rows which have no set cells.
            # Note that this can be represented as either no values, or a list of values where the
            # effectiveValue of each cell is empty.
            while self.rowData:
                if (not self.rowData[-1].values) or all(not v.effectiveValue for v in self.rowData[-1].values):
                    self.rowData.pop()
                else:
                    break

            return self

    data: tuple[Data] | None = None  # When present, it's always a single element.


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
