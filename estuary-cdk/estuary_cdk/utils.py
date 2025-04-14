from collections.abc import Mapping
from typing import Any

def sort_dict(obj: Any) -> Any:
    if isinstance(obj, Mapping):
        return {k: sort_dict(v) for k, v in sorted(obj.items())}
    if isinstance(obj, list):
        return [sort_dict(item) for item in obj]
    return obj
