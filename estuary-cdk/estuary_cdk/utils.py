from collections.abc import Mapping
from typing import Any


def sort_dict(obj: Any) -> Any:
    if isinstance(obj, Mapping):
        return {k: sort_dict(v) for k, v in sorted(obj.items())}
    if isinstance(obj, list):
        return [sort_dict(item) for item in obj]
    return obj


def json_merge_patch(target: dict, patch: dict) -> None:
    """
    Apply JSON Merge Patch (RFC 7396) to target dictionary in-place.

    Args:
        target: The dictionary to be modified
        patch: The patch to apply

    Semantics:
        - null values remove keys from target
        - nested objects are recursively merged
        - arrays and primitives are replaced entirely
    """

    for key, value in patch.items():
        if value is None:
            # Remove the key if value is None
            target.pop(key, None)
        elif (
            isinstance(value, dict) and key in target and isinstance(target[key], dict)
        ):
            # Recursively merge dictionaries
            json_merge_patch(target[key], value)
        else:
            # Replace the value (including arrays)
            target[key] = value
