from estuary_cdk.shim_airbyte_cdk import transform_airbyte_key

def test_transform_airbyte_key():
    assert transform_airbyte_key("pizza") == ["pizza"]
    assert transform_airbyte_key("piz/za") == ["piz~1za"]
    assert transform_airbyte_key(["piz/za"]) == ["piz~1za"]
    assert transform_airbyte_key(["piz/za", "par~ty"]) == ["piz~1za", "par~0ty"]
    assert transform_airbyte_key([["pizza", "che/ese"], "potato"]) == [
       "pizza/che~1ese", "potato"
    ]
