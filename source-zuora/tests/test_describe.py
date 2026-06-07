"""Unit tests for discovery/describe parsing in source_zuora.api and the
export-context filtering on the describe models.

These cover the XML parsing, the default-closed `<contexts>` export filter, and
the `./fields/field` scoping that ignores nested <field>s — logic that the
spec/capture tests don't touch.
"""

import pytest

from source_zuora import api
from source_zuora.models import DescribeField


# --- DescribeField.is_exportable ----------------------------------------------


def test_is_exportable_requires_selectable_and_export_context():
    assert DescribeField(name="Id", selectable=True, contexts=["soap", "export"]).is_exportable
    # selectable but not export-context
    assert not DescribeField(name="X", selectable=True, contexts=["soap"]).is_exportable
    # export context but not selectable
    assert not DescribeField(name="X", selectable=False, contexts=["export"]).is_exportable
    # no contexts at all (default-closed)
    assert not DescribeField(name="X", selectable=True, contexts=[]).is_exportable


# --- _parse_describe_object ----------------------------------------------------

_DESCRIBE_XML = b"""<object>
  <name>Account</name>
  <fields>
    <field>
      <name>Id</name><selectable>true</selectable>
      <contexts><context>soap</context><context>export</context></contexts>
    </field>
    <field>
      <name>UpdatedDate</name><selectable>true</selectable>
      <contexts><context>export</context></contexts>
    </field>
    <field>
      <name>SoapOnly</name><selectable>true</selectable>
      <contexts><context>soap</context></contexts>
    </field>
    <field>
      <name>NoContexts</name><selectable>true</selectable>
    </field>
    <field>
      <name>NotSelectable</name><selectable>false</selectable>
      <contexts><context>export</context></contexts>
    </field>
  </fields>
  <related-objects>
    <object href="x">
      <name>Contact</name>
      <field>
        <name>StrayNestedField</name><selectable>true</selectable>
        <contexts><context>export</context></contexts>
      </field>
    </object>
  </related-objects>
</object>"""


def test_parse_describe_object_extracts_name_and_fields():
    obj = api._parse_describe_object(_DESCRIBE_XML)
    assert obj.name == "Account"
    # every top-level field is parsed (before exportability filtering)
    assert {f.name for f in obj.fields} == {
        "Id",
        "UpdatedDate",
        "SoapOnly",
        "NoContexts",
        "NotSelectable",
    }


def test_parse_describe_object_only_exportable_fields_survive_filter():
    obj = api._parse_describe_object(_DESCRIBE_XML)
    # soap-only, no-contexts, and non-selectable are all dropped
    assert obj.exportable_field_names == ["Id", "UpdatedDate"]


def test_parse_describe_object_ignores_nested_stray_fields():
    # StrayNestedField (under related-objects) is selectable + export-context, so
    # it would pass the filter if the scope were `.//field` instead of
    # `./fields/field`. It must NOT appear.
    obj = api._parse_describe_object(_DESCRIBE_XML)
    assert "StrayNestedField" not in {f.name for f in obj.fields}
    assert "StrayNestedField" not in obj.exportable_field_names


def test_parse_describe_object_skips_fields_without_a_name():
    xml = b"""<object><name>Obj</name><fields>
      <field><selectable>true</selectable><contexts><context>export</context></contexts></field>
      <field><name>Good</name><selectable>true</selectable><contexts><context>export</context></contexts></field>
    </fields></object>"""
    obj = api._parse_describe_object(xml)
    assert [f.name for f in obj.fields] == ["Good"]


# --- _parse_catalog ------------------------------------------------------------


def test_parse_catalog_extracts_object_names():
    xml = b"""<objects>
      <object><name>Account</name></object>
      <object><name>Invoice</name></object>
    </objects>"""
    catalog = api._parse_catalog(xml)
    assert [o.name for o in catalog.objects] == ["Account", "Invoice"]


def test_parse_catalog_skips_objects_without_a_name():
    xml = b"""<objects>
      <object><name>Account</name></object>
      <object></object>
    </objects>"""
    catalog = api._parse_catalog(xml)
    assert [o.name for o in catalog.objects] == ["Account"]


# --- describe_object / discover_object_names (async, over a fake http) ---------


class _FakeHTTP:
    def __init__(self, response: bytes):
        self.response = response
        self.urls: list[str] = []
        self.headers: list = []

    async def request(self, log, url, method=None, json=None, headers=None):
        self.urls.append(url)
        self.headers.append(headers)
        return self.response


@pytest.mark.asyncio
async def test_describe_object_returns_exportable_field_names():
    http = _FakeHTTP(_DESCRIBE_XML)
    fields = await api.describe_object("https://rest.zuora.com", http, None, "Account")
    assert fields == ["Id", "UpdatedDate"]
    assert http.urls == ["https://rest.zuora.com/v1/describe/Account"]


@pytest.mark.asyncio
async def test_describe_requests_pin_api_version_header():
    from source_zuora.shared import ZUORA_API_VERSION

    http = _FakeHTTP(_DESCRIBE_XML)
    await api.describe_object("https://rest.zuora.com", http, None, "Account")
    assert http.headers[0].get("Zuora-Version") == ZUORA_API_VERSION


@pytest.mark.asyncio
async def test_discover_object_names_returns_catalog_names():
    xml = b"<objects><object><name>Account</name></object><object><name>Order</name></object></objects>"
    http = _FakeHTTP(xml)
    names = await api.discover_object_names("https://rest.zuora.com", http, None)
    assert names == ["Account", "Order"]
    assert http.urls == ["https://rest.zuora.com/v1/describe"]
