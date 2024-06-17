import json
import subprocess


def test_nullable_pk(request):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "-o",
            "json",
            "--emit-raw"
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    schemas = [json.loads(l) for l in result.stdout.splitlines()]

    for schema in schemas:
        # Checking first if the schema is a type object (not nullable)
        assert schema["documentSchema"]["type"] == "object" or schema["documentSchema"]["type"] == ["object"], f"stream {schema['recommendedName']} type must be a non-null object"

        # Checking if schema has properties
        assert len(schema["documentSchema"]["properties"]) != 0, f"stream {schema['recommendedName']} has no properties, tests will always fail"

        # Checking if schema has primary_keys
        if schema.get("key") is None and schema["documentSchema"].get("required") is None:
            continue
    
        # Checking if the 'key' and 'required' fields have the same fields
        elif schema.get("key") and schema["documentSchema"].get("required"):
            required_fields_comp = [f"/{field}" for field in schema["documentSchema"]["required"]]
            assert schema["key"].sort() == required_fields_comp.sort(), f"stream {schema['recommendedName']} required keys and primary keys do not match"

            # Checking if the required fields are not null
            for field in schema["documentSchema"]["required"]:
                required_field = schema["documentSchema"]["properties"][f"{field}"]["type"]
                if type(required_field) == list:
                    assert ("null" in required_field) == False, f"{field} field from stream '{schema['recommendedName']}' is required and can not contain 'null'"
                else:
                    assert required_field != "null", f"{field} field from stream '{schema['recommendedName']}' is required and can not be 'null'"

        else:
            raise Exception(f"Missing required field: 'key' or 'required' for stream '{schema['recommendedName']}'")








