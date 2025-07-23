import asyncio
import time
import uuid
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator, ClassVar, Optional, TypeVar

import xmltodict
from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import AwareDatetime, BaseModel, create_model, model_validator

from .models import (
    COMPANY_ID_FIELD,
    ApiResponse,
    EndpointConfig,
    GenerateApiSessionResponse,
    GetUserByIDResponse,
    ListUserDateAndTimestampFormattingResponse,
    ObjectDefinition,
)

PAGE_SIZE = 2000

endpoint_url = "https://api.intacct.com/ia/xml/xmlgw.phtml"


DATATYPE_MAP: dict[str, Any] = {
    "TEXT": str,
    "INTEGER": int | float, # We have seen instances of string-encoded floats in "INTEGER" fields.
    "BOOLEAN": bool,
    "DATE": str,
    "TIMESTAMP": AwareDatetime,
    "ENUM": str,
    "DECIMAL": str,
    # TODO(whb): Update the remaining types once we see some values and know
    # what they look like. `str` is just a placeholder, and may or may not be
    # appropriate.
    "CURRENCY": str,
    "PERCENT": str,
    "SEQUENCE": str,
}

# These are fields for certain objects that aren't allow to be queried.
FORBIDDEN_FIELDS = {
    "PROJECT": [
        "CFDA",
        "FUNDEDNAME",
        "AGENCY",
        "PAYER",
        "FUNDINGOTHERID",
        "ASSISTANCETYPE",
        "REVRESTRICTION",
        "RESTRICTIONEXPIRY",
        "RESTRICTIONEXPIRATIONDATE",
        "TIMESATISFACTIONSCHEDULED",
    ],
}


class SageRecord(BaseModel):
    """
    Base class for all Sage Intacct records. Pydantic types are determined at
    runtime from the object definition in order to leverage as much of
    Pydantic's built-in value conversion as possible. Other specific cases are
    handled in the `_normalize_values` method.

    To use instances of this class, use `model_dump` to get a Python dict, and
    validate it into a more specific model.
    """

    tz_dt: ClassVar[datetime]
    field_names: ClassVar[list[str]]
    field_datatypes: ClassVar[list[str]]
    company_id: ClassVar[str | None] = None

    def model_dump(self, **kwargs):
        kwargs.setdefault("exclude_none", True)
        res = super().model_dump(**kwargs)
        if self.company_id:
            res[COMPANY_ID_FIELD] = self.company_id

        return res

    @model_validator(mode="before")
    @classmethod
    def _normalize_values(cls, values: dict[str, Any]) -> dict[str, Any]:
        out = {}

        for field_name, datatype in zip(cls.field_names, cls.field_datatypes):
            if field_name not in values or (val := values[field_name]) is None:
                continue

            match datatype:
                case "TIMESTAMP":
                    assert isinstance(val, str)
                    parsed = False
                    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y"):
                        try:
                            dt_naive = datetime.strptime(val, fmt)
                            parsed = True
                            break
                        except ValueError:
                            continue

                    if not parsed:
                        raise ValueError(
                            f"Invalid TIMESTAMP format for {field_name}: {val}"
                        )

                    dt_aware = dt_naive.replace(tzinfo=cls.tz_dt.tzinfo)
                    val = dt_aware
                case "BOOLEAN":
                    assert isinstance(val, str)
                    if val == "":
                        continue
                case "DECIMAL":
                    val = str(val)
                case "CURRENCY" | "SEQUENCE" | "PERCENT":
                    # TODO(whb): Update this once we see some values and know
                    # what they look like.
                    raise ValueError(
                        f"Unhandled datatype {datatype} for {field_name}: {val}"
                    )

            out[field_name] = val

        return out

    @classmethod
    def sourced_schema(cls) -> dict[str, Any]:
        schema = {
            "additionalProperties": False,
            "type": "object",
            "properties": {},
            "required": [],
        }

        for field, datatype in zip(cls.field_names, cls.field_datatypes):
            field_schema: dict[str, Any] = {}
            match datatype:
                case "TEXT" | "ENUM":
                    field_schema = {"type": "string"}
                case "INTEGER":
                    field_schema = {"type": "integer"}
                case "BOOLEAN":
                    field_schema = {"type": "boolean"}
                case "DATE":
                    field_schema = {
                        "type": "string",
                        "format": "date",
                    }
                case "TIMESTAMP":
                    field_schema = {
                        "type": "string",
                        "format": "date-time",
                    }
                case "DECIMAL":
                    field_schema = {
                        "type": "string",
                        "format": "number",
                    }
                case "CURRENCY" | "SEQUENCE" | "PERCENT":
                    # TODO(whb): Update this to an appropriate type once we see
                    # some values and know what they look like.
                    continue
                case _:
                    raise Exception(
                        f"Unknown type for field {field} of record {cls.__name__}: {datatype}"
                    )

            schema["properties"][field] = field_schema
            schema["required"].append(field)

        if cls.company_id:
            schema["properties"][COMPANY_ID_FIELD] = {"type": "string"}
            schema["required"].append(COMPANY_ID_FIELD)

        return schema


XMLRecord = TypeVar("XMLRecord", bound=BaseModel)

JSONRecord = TypeVar("JSONRecord", bound=SageRecord)


class Sage:
    """
    This class encapsulates the logic for interacting with the Sage Intacct API.
    """

    session_id: str
    user_id: str
    tz_dt: datetime
    deletion_model: type[SageRecord] | None = None

    def __init__(self, log: Logger, http: HTTPSession, config: EndpointConfig):
        self.log = log
        self.http = http
        self.config = config

        self.session_lock = asyncio.Lock()
        self.session_timeout: AwareDatetime = datetime.fromtimestamp(0, tz=UTC)
        self.model_cache: dict[str, tuple[AwareDatetime, type[SageRecord]]] = {}

    async def setup(self):
        await self._maybe_refresh_session()

        user = await self._req_xml(
            GetUserByIDResponse,
            get_user_by_id_request(self.config, self.session_id, self.user_id),
        )

        datetimeFormat = await self._req_xml(
            ListUserDateAndTimestampFormattingResponse,
            user_datetime_prefs_request(
                self.config, self.session_id, user.USERINFO.RECORDNO
            ),
        )
        format = datetimeFormat.userformatting

        # TODO(whb): The checks below are hardcoded as the only thing we have
        # been able to see or tests right now, and will need expanded in the
        # future for other configurations.

        if format.locale != "en_US":
            raise Exception(
                f"unknown locale {datetimeFormat.UserFormatting.locale}, must be 'en_US'"
            )

        if format.clock != "24":
            raise Exception(
                f"unknown clock format {datetimeFormat.UserFormatting.clock}, must be '24'"
            )

        if format.dateformat != "/mdY":
            raise Exception(
                f"unknown date format {datetimeFormat.UserFormatting.dateformat}, must be '/mdY'"
            )

        self.tz_dt = datetime.strptime(format.gmtoffset, "%z")

    async def fetch_since(
        self, obj: str, since: AwareDatetime
    ) -> AsyncGenerator[SageRecord, None]:
        model = await self.get_model(obj)
        formatted_since = since.astimezone(model.tz_dt.tzinfo).strftime(
            "%m/%d/%Y %H:%M:%S"
        )
        data = get_records_since_request(
            self.config, self.session_id, obj, model.field_names, formatted_since
        )
        async for rec in self._req_json(model, data):
            yield rec

    async def fetch_at(
        self,
        obj: str,
        at: AwareDatetime,
        after: int | None,
    ) -> AsyncGenerator[SageRecord, None]:
        model = await self.get_model(obj)
        formatted_at = at.astimezone(model.tz_dt.tzinfo).strftime("%m/%d/%Y %H:%M:%S")
        data = get_records_at_request(
            self.config, self.session_id, obj, model.field_names, formatted_at, after
        )
        async for rec in self._req_json(model, data):
            yield rec

    async def fetch_all(
        self, obj: str, after: int | None
    ) -> AsyncGenerator[SageRecord, None]:
        model = await self.get_model(obj)
        data = get_all_records_request(
            self.config, self.session_id, obj, model.field_names, after
        )
        async for rec in self._req_json(model, data):
            yield rec

    async def fetch_deleted(
        self, obj: str, since: AwareDatetime
    ) -> AsyncGenerator[SageRecord, None]:
        model = self.get_deletion_model()
        formatted_since = since.astimezone(model.tz_dt.tzinfo).strftime(
            "%m/%d/%Y %H:%M:%S"
        )
        data = get_deletions_since_request(
            self.config, self.session_id, obj, formatted_since
        )
        async for rec in self._req_json(model, data):
            yield rec

    async def fetch_deleted_at(
        self,
        obj: str,
        at: AwareDatetime,
        after: str | None,
    ) -> AsyncGenerator[SageRecord, None]:
        model = self.get_deletion_model()
        formatted_at = at.astimezone(model.tz_dt.tzinfo).strftime("%m/%d/%Y %H:%M:%S")
        data = get_deletions_at_request(
            self.config, self.session_id, obj, formatted_at, after
        )
        async for rec in self._req_json(model, data):
            yield rec

    async def get_model(self, obj: str) -> type[SageRecord]:
        if obj in self.model_cache:
            expires = self.model_cache[obj][0]
            if expires > datetime.now(tz=UTC):
                return self.model_cache[obj][1]

        expires = datetime.now(tz=UTC) + timedelta(minutes=30)
        model = await self._build_model(obj)
        self.model_cache[obj] = (expires, model)
        return model

    def get_deletion_model(self) -> type[SageRecord]:
        if self.deletion_model:
            return self.deletion_model

        field_defs: dict[str, Any] = {
            "OBJECTKEY": (str, None),
            "ACCESSTIME": (AwareDatetime, None),
            "ID": (str, None),
        }
        model = create_model(
            "DELETIONS",
            __base__=SageRecord,
            **field_defs,
        )
        model.tz_dt = self.tz_dt
        model.field_names = ["OBJECTKEY", "ACCESSTIME", "ID"]
        model.field_datatypes = ["TEXT", "TIMESTAMP", "TEXT"]
        self.deletion_model = model

        return model

    async def _req_xml(
        self, cls: type[XMLRecord], data: Any, skip_refresh=False
    ) -> XMLRecord:
        if not skip_refresh:
            await self._maybe_refresh_session()

        bs = await self.http.request(
            self.log,
            endpoint_url,
            method="POST",
            headers={"Content-Type": "application/xml"},
            form=data,
        )

        parsed = ApiResponse.model_validate(xmltodict.parse(bs))
        parsed.raise_for_error()
        assert (op := parsed.response.operation) is not None
        assert (result := op.result) is not None
        assert op.authentication.sessiontimeout is not None
        self.session_timeout = op.authentication.sessiontimeout
        self.user_id = op.authentication.userid

        return cls.model_validate(result.data)

    async def _req_json(
        self, cls: type[JSONRecord], data: Any
    ) -> AsyncGenerator[JSONRecord, None]:
        await self._maybe_refresh_session()

        hdr, rdr = await self.http.request_stream(
            self.log,
            endpoint_url,
            method="POST",
            headers={"Content-Type": "application/xml"},
            form=data,
        )

        if hdr.get("Content-Type") != "application/json":
            # A response that is not JSON is an error as XML.
            bs = []
            async for chunk in rdr():
                bs.append(chunk)

            parsed = ApiResponse.model_validate(xmltodict.parse(b"".join(bs)))
            parsed.raise_for_error()

        async for rec in IncrementalJsonProcessor(rdr(), "item", cls):
            yield rec

    async def _maybe_refresh_session(self):
        if self.session_timeout > datetime.now(tz=UTC) - timedelta(minutes=5):
            return

        async with self.session_lock:
            if self.session_timeout > datetime.now(tz=UTC):
                return

            sess = await self._req_xml(
                GenerateApiSessionResponse,
                api_session_request(self.config),
                skip_refresh=True,
            )
            self.session_id = sess.api.sessionid

    async def _build_model(self, obj: str) -> type[SageRecord]:
        object_definition = await self._req_xml(
            ObjectDefinition,
            object_definition_request(self.config, self.session_id, obj),
        )

        forbidden = FORBIDDEN_FIELDS.get(obj, [])

        base_fields = set(SageRecord.model_fields.keys())
        field_defs = {}
        field_info = []
        for field in object_definition.Type.Fields.Field:
            if field.ID in forbidden:
                continue

            if field.DATATYPE not in DATATYPE_MAP:
                raise ValueError(
                    f"Unknown type for field {field.ID} of record {obj}: {field.DATATYPE}"
                )

            field_info.append((field.ID, field.DATATYPE))
            if field.ID not in base_fields:
                field_defs[field.ID] = (Optional[DATATYPE_MAP[field.DATATYPE]], None)

        m = create_model(
            obj,
            __base__=SageRecord,
            **field_defs,
        )

        m.tz_dt = self.tz_dt
        m.field_names = [field[0] for field in field_info]
        m.field_datatypes = [field[1] for field in field_info]
        if self.config.advanced.include_company_id_in_documents:
            m.company_id = self.config.company_id

        return m


def xml_query(cfg: EndpointConfig, operation: str) -> str:
    return f"""
<?xml version="1.0" encoding="UTF-8"?>
<request>
  <control>
    <senderid>{cfg.sender_id}</senderid>
    <password>{cfg.sender_password}</password>
    <controlid>{int(time.time())}</controlid>
    <uniqueid>false</uniqueid>
    <dtdversion>3.0</dtdversion>
    <includewhitespace>false</includewhitespace>
  </control>
  <operation>
    {operation}
  </operation>
</request>
""".strip()


def api_session_request(cfg: EndpointConfig) -> str:
    operation = f"""
    <authentication>
      <login>
        <userid>{cfg.user_id}</userid>
        <companyid>{cfg.company_id}</companyid>
        <password>{cfg.password}</password>
      </login>
    </authentication>
    <content>
      <function controlid="{str(uuid.uuid4())}">
        <getAPISession />
      </function>
    </content>
""".strip()

    return xml_query(cfg, operation)


def function_with_session_id_xml(
    cfg: EndpointConfig, session_id: str, exec: str
) -> str:
    operation = f"""
    <authentication>
      <sessionid>{session_id}</sessionid>
    </authentication>
    <content>
      <function controlid="{str(uuid.uuid4())}">
        {exec}
      </function>
    </content>
""".strip()

    return xml_query(cfg, operation)


def user_datetime_prefs_request(
    cfg: EndpointConfig, session_id: str, user_record_no: int
) -> str:
    exec = f"""
        <readUserFormatting>
          <key>{user_record_no}</key>
        </readUserFormatting>
""".strip()

    return function_with_session_id_xml(cfg, session_id, exec)


def get_user_by_id_request(cfg: EndpointConfig, session_id: str, user_id: str) -> str:
    exec = f"""
        <readByName>
          <object>USERINFO</object>
          <keys>{user_id}</keys>
          <fields>RECORDNO</fields>
        </readByName>
""".strip()

    return function_with_session_id_xml(cfg, session_id, exec)


def object_definition_request(cfg: EndpointConfig, session_id: str, object: str) -> str:
    exec = f"""
        <lookup>
          <object>{object}</object>
        </lookup>
""".strip()

    return function_with_session_id_xml(cfg, session_id, exec)


def get_records_since_request(
    cfg: EndpointConfig,
    session_id: str,
    object: str,
    fields: list[str],
    since: str,  # ex: 12/08/202 10:46:26
) -> str:
    exec = f"""
        <query>
          <object>{object}</object>
          <select>
{"\n".join(f"            <field>{field}</field>" for field in fields)}
          </select>
          <filter>
              <greaterthan>
                  <field>WHENMODIFIED</field>
                  <value>{since}</value>
              </greaterthan>
          </filter>
          <orderby>
            <order>
                <field>WHENMODIFIED</field>
                <ascending/>
            </order>
          </orderby>
          <options>
            <returnformat>json</returnformat>
          </options>
          <pagesize>{PAGE_SIZE}</pagesize>
        </query>
""".strip()

    return function_with_session_id_xml(cfg, session_id, exec)


def get_records_at_request(
    cfg: EndpointConfig,
    session_id: str,
    object: str,
    fields: list[str],
    at: str,  # ex: 12/08/202 10:46:26
    after: int | None,
) -> str:
    def filter() -> str:
        if after is None:
            return ""
        return f"""
              <greaterthan>
                  <field>RECORDNO</field>
                  <value>{after}</value>
              </greaterthan>"""

    exec = f"""
        <query>
          <object>{object}</object>
          <select>
{"\n".join(f"            <field>{field}</field>" for field in fields)}
          </select>
          <filter>
              <equalto>
                  <field>WHENMODIFIED</field>
                  <value>{at}</value>
              </equalto>{filter()}
          </filter>
          <orderby>
            <order>
                <field>RECORDNO</field>
                <ascending/>
            </order>
          </orderby>
          <options>
            <returnformat>json</returnformat>
          </options>
          <pagesize>{PAGE_SIZE}</pagesize>
        </query>
""".strip()

    return function_with_session_id_xml(cfg, session_id, exec)


def get_all_records_request(
    cfg: EndpointConfig,
    session_id: str,
    object: str,
    fields: list[str],
    after: int | None,
) -> str:
    def filter() -> str:
        if after is None:
            return ""
        return f"""
          <filter>
              <greaterthan>
                  <field>RECORDNO</field>
                  <value>{after}</value>
              </greaterthan>
          </filter>"""

    exec = f"""
        <query>
          <object>{object}</object>
          <select>
{"\n".join(f"            <field>{field}</field>" for field in fields)}
          </select>{filter()}
          <orderby>
            <order>
                <field>RECORDNO</field>
                <ascending/>
            </order>
          </orderby>
          <options>
            <returnformat>json</returnformat>
          </options>
          <pagesize>{PAGE_SIZE}</pagesize>
        </query>
""".strip()

    return function_with_session_id_xml(cfg, session_id, exec)


def get_deletions_since_request(
    cfg: EndpointConfig,
    session_id: str,
    object: str,
    since: str,  # ex: 12/08/202 10:46:26
) -> str:
    exec = f"""
        <query>
          <object>AUDITHISTORY</object>
          <select>
            <field>OBJECTKEY</field>
            <field>ACCESSTIME</field>
            <field>ID</field>
          </select>
          <filter>
            <and>
              <equalto>
                <field>OBJECTTYPE</field>
                <value>{object.lower()}</value>
              </equalto>
              <equalto>
                <field>ACCESSMODE</field>
                <value>D</value>
              </equalto>
              <greaterthan>
                <field>ACCESSTIME</field>
                <value>{since}</value>
              </greaterthan>
            </and>
          </filter>
          <orderby>
            <order>
              <field>ACCESSTIME</field>
              <ascending />
            </order>
          </orderby>
          <options>
            <returnformat>json</returnformat>
          </options>
          <pagesize>{PAGE_SIZE}</pagesize>
        </query>
""".strip()

    return function_with_session_id_xml(cfg, session_id, exec)


def get_deletions_at_request(
    cfg: EndpointConfig,
    session_id: str,
    object: str,
    at: str,  # ex: 12/08/202 10:46:26
    after: str | None,
) -> str:
    def filter() -> str:
        if after is None:
            return ""
        return f"""
              <greaterthan>
                <field>ID</field>
                <value>{after}</value>
              </greaterthan>"""

    exec = f"""
        <query>
          <object>AUDITHISTORY</object>
          <select>
            <field>OBJECTKEY</field>
            <field>ACCESSTIME</field>
            <field>ID</field>
          </select>
          <filter>
            <and>
              <equalto>
                <field>OBJECTTYPE</field>
                <value>{object.lower()}</value>
              </equalto>
              <equalto>
                <field>ACCESSMODE</field>
                <value>D</value>
              </equalto>
              <equalto>
                <field>ACCESSTIME</field>
                <value>{at}</value>
              </equalto>{filter()}
            </and>
          </filter>
          <orderby>
            <order>
              <field>ID</field>
              <ascending />
            </order>
          </orderby>
          <options>
            <returnformat>json</returnformat>
          </options>
          <pagesize>{PAGE_SIZE}</pagesize>
        </query>
""".strip()

    return function_with_session_id_xml(cfg, session_id, exec)
