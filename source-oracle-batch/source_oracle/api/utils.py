from typing import Any, Set

from estuary_cdk.capture import common

from source_netsuite.models import Table


def assert_no_duplicate_primary_keys(
    key_fields: list[str],
    keys: Set[Any],
    row: common.BaseDocument,
):
    row_serialized = row.model_dump(by_alias=True, exclude_unset=True)
    key_tuple = tuple(resolve_json_pointer(row_serialized, k) for k in key_fields)

    if key_tuple in keys:
        key_repr = ", ".join((f"{k}: {v}" for k, v in zip(key_fields, key_tuple)))
        raise RuntimeError(
            f"""
                Duplicate primary key ({key_repr}) encountered.
            """
        )

    # Mutate the set in place.
    keys.add(key_tuple)


def resolve_json_pointer(data, path):
    """
    Resolves a JSON Pointer path to a value within a nested dictionary (JSON object).

    :param data: The nested dictionary to search.
    :param path: The JSON Pointer path string.
    :return: The value found at the specified path. If the path does not exist, returns None.
    """
    # Split the path into parts and filter out the empty string before the first '/'
    parts = path.split("/")[1:]

    # Iterate over the parts to traverse the dictionary
    for part in parts:
        # If the current part is not a key in the data, return None
        if not isinstance(data, dict) or part not in data:
            return None
        # Move to the next level in the dictionary
        data = data[part]

    return data
