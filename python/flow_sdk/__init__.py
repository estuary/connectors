import abc
import orjson
import os
import sys
import typing as t
from .logger import init_logger

logger = init_logger()


class BaseConnector(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def _run(self) -> None:
        raise NotImplemented

    def _emit(self, response: t.Any) -> None:
        # TODO(johnny): Make this buffered?
        os.write(
            1,
            orjson.dumps(
                response,
                # Map non-string dict keys into str
                option=orjson.OPT_NON_STR_KEYS
                # Prefer 'Z' over '+00:00'.
                | orjson.OPT_UTC_Z
                # Add a newline for newline JSON.
                | orjson.OPT_APPEND_NEWLINE,
                # Map unhandled JSON types through __str__() if available, or repr().
                default=str,
            ),
        )

    def main(self) -> None:
        try:
            self._run()

        except ValidateError as exc:
            logger.exception(exc.message)
            sys.exit(-1)

        except Exception as exc:
            msg, args = type(exc).__name__, exc.args
            if len(args) != 0:
                msg = f"{msg}: {args[0]}"

            logger.exception(msg, args)
            sys.exit(-1)


class ValidateError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

def retain_paths_in_jsonschema(schema: t.Dict[str, t.Any], paths: t.List[t.List[str]]):
    if "object" in schema["type"]:
        fields_to_keep: t.List[str] = []
        for property, sub_schema in schema["properties"].items():
            # Pop off the 0th item since we're recurring
            sub_paths = [path[1:] for path in paths if path[0]==property and len(path)>1]
            if len(sub_paths) > 0:
                # We have some fields in here to retain
                retain_paths_in_jsonschema(sub_schema, sub_paths)
                fields_to_keep.append(property)
            elif any(len(path) == 1 and path[0] == property for path in paths):
                # We reached the end and it's this element, so keep it
                fields_to_keep.append(property)
                if "null" in sub_schema["type"]:
                    sub_schema["type"] = [shape for shape in sub_schema["type"] if shape != "null"]
                    if len(sub_schema["type"])==1:
                        sub_schema["type"] = sub_schema["type"][0]
        schema["properties"] = {k:v for k,v in schema["properties"].items() if k in fields_to_keep}
        if "null" in schema["type"]:
            schema["type"] = "object"
    elif "array" in schema["type"]:
        max_index = 0
        for idx, sub_schema in enumerate(schema["items"]):
            # Pop off the 0th item since we're recurring
            sub_paths = [path[1:] for path in paths if path[0]==idx and len(path)>1]
            if len(sub_paths) > 0:
                # We have some fields in here to retain, but let's not remove anything from arrays
                # because it's not possible to remove from the middle without messing up later keys
                retain_paths_in_jsonschema(sub_schema, sub_paths)
                max_index = max(max_index, idx)
            elif any(len(path) == 1 and path[0] == idx for path in paths):
                max_index = max(max_index, idx)
                if "null" in sub_schema["type"]:
                    sub_schema["type"] = [shape for shape in sub_schema["type"] if shape != "null"]
                    if len(sub_schema["type"])==1:
                        sub_schema["type"] = sub_schema["type"][0]
        schema["items"] = schema["items"][:max_index+1]
        if "null" in schema["type"]:
            schema["type"] = "array"
    return schema
