import pytest
from airbyte_cdk.sources.streams.http import HttpStream

from estuary_cdk.shim_airbyte_cdk import transform_airbyte_key

def test_transform_airbyte_key():
    assert transform_airbyte_key("pizza") == ["pizza"]
    assert transform_airbyte_key("piz/za") == ["piz~1za"]
    assert transform_airbyte_key(["piz/za"]) == ["piz~1za"]
    assert transform_airbyte_key(["piz/za", "par~ty"]) == ["piz~1za", "par~0ty"]
    assert transform_airbyte_key([["pizza", "che/ese"], "potato"]) == [
       "pizza/che~1ese", "potato"
    ]


URL_BASE = "https://api.example.com/v1/"


@pytest.mark.parametrize("path, expected", [
    ("items", "https://api.example.com/v1/items"),
    ("/2010-04-01/Calls.json", "https://api.example.com/2010-04-01/Calls.json"),
    ("../v2/items", "https://api.example.com/v2/items"),
])
def test_join_url_on_url_base_host(path: str, expected: str):
    assert HttpStream._join_url(URL_BASE, path) == expected


@pytest.mark.parametrize("path", [
    "//other.example.com/items",
    "https://other.example.com/items",
    "http://api.example.com/v1/items",
])
def test_join_url_off_url_base_host(path: str):
    with pytest.raises(ValueError):
        HttpStream._join_url(URL_BASE, path)
