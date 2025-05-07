import asyncio
import time
import uuid
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator, Optional

import xmltodict
from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import AwareDatetime, create_model

from .models import (
    ApiResponse,
    DynamicRecordModel,
    EndpointConfig,
    GenerateApiSessionResponse,
    GetUserByIDResponse,
    JSONRecord,
    ListUserDateAndTimestampFormattingResponse,
    ObjectDefinition,
    XMLRecord,
)

endpoint_url = "https://api.intacct.com/ia/xml/xmlgw.phtml"


DATATYPE_MAP: dict[str, Any] = {
    "TEXT": str,
    "INTEGER": int,
    "BOOLEAN": bool,
    "DATE": str,
    "TIMESTAMP": AwareDatetime,
    "ENUM": str,
    "DECIMAL": float,
    "CURRENCY": str,
    "PERCENT": str,
    "SEQUENCE": str,
}

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


class Sage:
    session_id: str
    user_id: str
    tz_dt: datetime

    def __init__(self, log: Logger, http: HTTPSession, config: EndpointConfig):
        super().__init__()
        self.log = log
        self.http = http
        self.config = config

        self.session_lock = asyncio.Lock()
        self.session_timeout = datetime.fromtimestamp(0, tz=UTC)
        self.model_cache: dict[str, tuple[AwareDatetime, type[DynamicRecordModel]]] = {}

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
        self, name: str, since: AwareDatetime
    ) -> AsyncGenerator[DynamicRecordModel, None]:
        model = await self._get_model(name)
        formatted_since = since.astimezone(model.tz_dt.tzinfo).strftime(
            "%m/%d/%Y %H:%M:%S"
        )
        data = get_records_since_request(
            self.config, self.session_id, name, model.field_names, formatted_since
        )
        async for rec in self._req_json(model, data):
            yield rec

    async def fetch_at(
        self, name: str, at: AwareDatetime
    ) -> AsyncGenerator[DynamicRecordModel, None]:
        model = await self._get_model(name)
        formatted_at = at.astimezone(model.tz_dt.tzinfo).strftime("%m/%d/%Y %H:%M:%S")
        data = get_records_at_request(
            self.config, self.session_id, name, model.field_names, formatted_at
        )
        async for rec in self._req_json(model, data):
            yield rec

    async def fetch_all(
        self, name: str, after: int | None
    ) -> AsyncGenerator[DynamicRecordModel, None]:
        model = await self._get_model(name)
        data = get_all_records_request(
            self.config, self.session_id, name, model.field_names, after
        )
        async for rec in self._req_json(model, data):
            yield rec

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

        parsed = ApiResponse.model_validate(xmltodict.parse(bs)).response

        if parsed.errormessage:
            if isinstance(parsed.errormessage.error, list):
                error = parsed.errormessage.error[0]
            else:
                error = parsed.errormessage.error

            raise Exception(f"Error: {error.errorno} - {error.description2}")

        assert parsed.operation is not None
        op = parsed.operation

        if op.authentication.status != "success":
            raise Exception(f"authentication status: {op.authentication.status}")

        if op.result.status != "success":
            raise Exception(f"result status: {op.result.status}")

        self.session_timeout = op.authentication.sessiontimeout
        self.user_id = op.authentication.userid

        return cls.model_validate(op.result.data)

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
            bs = []
            async for chunk in rdr():
                bs.append(chunk)

            parsed = ApiResponse.model_validate(xmltodict.parse(b"".join(bs))).response
            assert parsed.operation is not None
            assert parsed.operation.result.errormessage is not None
            err = parsed.operation.result.errormessage.error
            if isinstance(err, list):
                error = err[0]
            else:
                error = err

            raise Exception(f"Error: {error.errorno} - {error.description2}")

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

    async def _build_model(self, name: str) -> type[DynamicRecordModel]:
        object_definition = await self._req_xml(
            ObjectDefinition,
            object_definition_request(self.config, self.session_id, name),
        )

        forbidden = FORBIDDEN_FIELDS.get(name, [])

        base_fields = set(DynamicRecordModel.model_fields.keys())
        field_defs = {}
        field_names = []
        for field in object_definition.Type.Fields.Field:
            if field.ID in forbidden:
                continue

            if field.DATATYPE not in DATATYPE_MAP:
                raise ValueError(
                    f"Unknown type for field {field.ID} of record {name}: {field.DATATYPE}"
                )

            field_names.append(field.ID)
            if field.ID not in base_fields:
                field_defs[field.ID] = (Optional[DATATYPE_MAP[field.DATATYPE]], None)

        dyn = create_model(
            name,
            __base__=DynamicRecordModel,
            **field_defs,
        )

        dyn.tz_dt = self.tz_dt
        dyn.field_names = field_names

        return dyn

    async def _get_model(self, name: str) -> type[DynamicRecordModel]:
        if name in self.model_cache:
            expires = self.model_cache[name][0]
            if expires > datetime.now(tz=UTC):
                return self.model_cache[name][1]

        expires = datetime.now(tz=UTC) + timedelta(minutes=30)
        model = await self._build_model(name)
        self.model_cache[name] = (expires, model)
        return model


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
    since: str,  # normalized like 12/08/202 10:46:26
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
          <pagesize>2000</pagesize>
        </query>
""".strip()

    return function_with_session_id_xml(cfg, session_id, exec)


def get_records_at_request(
    cfg: EndpointConfig,
    session_id: str,
    object: str,
    fields: list[str],
    at: str,  # normalized like 12/08/202 10:46:26
) -> str:
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
              </equalto>
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
          <pagesize>2000</pagesize>
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
    exec = f"""
        <query>
          <object>{object}</object>
          <select>
{"\n".join(f"            <field>{field}</field>" for field in fields)}
          </select>
          <filter>
              <greaterthan>
                  <field>RECORDNO</field>
                  <value>{after}</value>
              </greaterthan>
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
          <pagesize>2000</pagesize>
        </query>
""".strip()

    return function_with_session_id_xml(cfg, session_id, exec)
