import asyncio
import traceback
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass
class TaskInfo:
    """Information about an asyncio task."""
    task: asyncio.Task[Any]
    task_name: str
    coro_name: str
    stack_trace: str


def get_running_tasks_info(
    exclude_tasks: set[asyncio.Task[Any]] | None = None,
) -> list[TaskInfo]:
    """
    Returns information about all currently running asyncio tasks,
    excluding any tasks in exclude_tasks and any completed tasks.
    """
    loop = asyncio.get_running_loop()
    all_tasks = asyncio.all_tasks(loop)
    exclude = exclude_tasks or set()

    result = []
    for task in all_tasks:
        if task in exclude or task.done():
            continue

        task_name = task.get_name()
        coro = task.get_coro()
        coro_name = coro.__name__ if coro else "unknown"

        frames = task.get_stack()
        if frames:
            summary = traceback.StackSummary.extract(
                (frame, frame.f_lineno) for frame in frames
            )
            stack_trace = "".join(summary.format()).rstrip()
        else:
            stack_trace = "No stack available"

        result.append(TaskInfo(
            task=task,
            task_name=task_name,
            coro_name=coro_name,
            stack_trace=stack_trace,
        ))

    return result


def format_error_message(err: BaseException):
    msg = f"{err}"
    # If the exeption doesn't have a meaningful string representation,
    # set it to the exception type's name so something more useful is logged.
    if msg == "":
        msg = f"{type(err).__name__}"

    return msg


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


def compare_capture_records(actual: list, expected: list) -> list[str]:
    """
    Compare captured documents against each other. All keys present in
    an expected document must be present in the analogous actual document.
    Extra keys in actual documents are allowed.

    Each element in the actual and expected list arguments is [binding_name, document].

    Returns a list that contains any error messages generated during the comparison validation.
    """
    errors: list[str] = []

    if len(actual) != len(expected):
        errors.append(f"Record count: got {len(actual)}, expected {len(expected)}")
        return errors

    for i, (actual_record, expected_record) in enumerate(zip(actual, expected)):
        binding = expected_record[0] if isinstance(expected_record, list) and expected_record else f"record[{i}]"
        actual_doc = actual_record[1] if isinstance(actual_record, list) and len(actual_record) > 1 else actual_record
        baseline_doc = expected_record[1] if isinstance(expected_record, list) and len(expected_record) > 1 else expected_record

        errors.extend(compare_values(actual_doc, baseline_doc, binding))

    return errors


def compare_values(actual: Any, expected: Any, path: str) -> list[str]:
    """
    Recursively find where the actual value fails to validate against the expected value.

    For dicts: all expected keys must exist in actual with matching values. Extra keys are allowed in actual.
    For lists and primitives: values must match exactly.

    Returns a list that contains any error messages generated during the comparison validation.
    """
    errors: list[str] = []

    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return [f"{path}: expected dict, got {type(actual).__name__}"]
        for k in expected.keys():
            if k not in actual:
                errors.append(f"{path}: key '{k}' missing from actual document")
            else:
                errors.extend(compare_values(actual[k], expected[k], f"{path}.{k}"))
    elif isinstance(expected, list):
        if not isinstance(actual, list):
            return [f"{path}: expected list, got {type(actual).__name__}"]
        if len(expected) != len(actual):
            errors.append(f"{path}: list length {len(actual)}, expected {len(expected)}")
        elif any(isinstance(e, (dict, list)) for e in expected):
            for i, (a, e) in enumerate(zip(actual, expected)):
                errors.extend(compare_values(a, e, f"{path}[{i}]"))
        elif actual != expected:
            errors.append(f"{path}: expected {expected!r}, got {actual!r}")
    else:
        if actual != expected:
            errors.append(f"{path}: expected {expected!r}, got {actual!r}")

    return errors
