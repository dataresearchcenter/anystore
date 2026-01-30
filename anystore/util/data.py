from typing import Any, TypeVar

import orjson
import yaml
from banal import clean_dict as _clean_dict
from banal import ensure_dict, ensure_list, is_listish, is_mapping
from pydantic import BaseModel

from anystore.types import SDict


def _clean(val: Any) -> Any:
    if val is False:
        return False
    return val or None


def clean_dict(data: Any) -> dict[str, Any]:
    """
    Ensure dict return, clean up defaultdicts, drop `None` values and ensure
    `str` keys (for serialization)

    Examples:
        >>> clean_dict({1: 2})
        {"1": 2}
        >>> clean_dict({"a": ""})
        {}
        >>> clean_dict({"a": None})
        {}
        >>> clean_dict("foo")
        {}

    Args:
        data: Arbitrary input data

    Returns:
        A cleaned dict with string keys (or an empty one)
    """
    if not is_mapping(data):
        return {}
    return _clean_dict(
        {
            str(k): clean_dict(dict(v)) or None if is_mapping(v) else _clean(v)
            for k, v in data.items()
        }
    )


def is_empty(value: Any) -> bool:
    """Check if a value is empty from a human point of view"""
    if isinstance(value, (bool, int)):
        return False
    if value == "":
        return False
    return not value


def dict_merge(d1: dict[Any, Any], d2: dict[Any, Any]) -> dict[Any, Any]:
    """Merge the second dict into the first but omit empty values"""
    d1, d2 = clean_dict(d1), clean_dict(d2)
    for key, value in d2.items():
        if not is_empty(value):
            if is_mapping(value):
                value = ensure_dict(value)
                d1[key] = dict_merge(d1.get(key, {}), value)
            elif is_listish(value):
                d1[key] = ensure_list(d1.get(key)) + ensure_list(value)
            else:
                d1[key] = value
    return d1


BM = TypeVar("BM", bound=BaseModel)


def pydantic_merge(m1: BM, m2: BM) -> BM:
    """Merge the second pydantic object into the first one"""
    if m1.__class__ != m2.__class__:
        raise ValueError(
            f"Cannot merge: `{m1.__class__.__name__}` with `{m2.__class__.__name__}`"
        )
    return m1.__class__(
        **dict_merge(m1.model_dump(mode="json"), m2.model_dump(mode="json"))
    )


def model_dump(obj: BaseModel, clean: bool | None = False) -> SDict:
    """
    Serialize a pydantic object to a dict by alias and json mode

    Args:
        clean: Apply [clean_dict][anystore.util.clean_dict]
    """
    data = obj.model_dump(by_alias=True, mode="json")
    if clean:
        data = clean_dict(data)
    return data


def dump_json(
    obj: SDict, clean: bool | None = False, newline: bool | None = False
) -> bytes:
    """
    Dump a python dictionary to json bytes via orjson

    Args:
        obj: The data object (dictionary with string keys)
        clean: Apply [clean_dict][anystore.util.clean_dict]
        newline: Add a linebreak
    """
    if clean:
        obj = clean_dict(obj)
    if newline:
        return orjson.dumps(obj, option=orjson.OPT_APPEND_NEWLINE)
    return orjson.dumps(obj)


def dump_json_model(
    obj: BaseModel, clean: bool | None = False, newline: bool | None = False
) -> bytes:
    """
    Dump a pydantic obj to json bytes via orjson

    Args:
        obj: The pydantic object
        clean: Apply [clean_dict][anystore.util.clean_dict]
        newline: Add a linebreak
    """
    data = model_dump(obj, clean)
    return dump_json(data, newline=newline)


def dump_yaml(obj: SDict, clean: bool | None = False, newline: bool | None = False):
    """
    Dump a python dictionary to bytes

    Args:
        obj: The data object (dictionary with string keys)
        clean: Apply [clean_dict][anystore.util.clean_dict]
        newline: Add a linebreak
    """
    if clean:
        obj = clean_dict(obj)
    data = yaml.dump(obj)
    if newline:
        data += "\n"
    return data.encode()


def dump_yaml_model(
    obj: BaseModel, clean: bool | None = False, newline: bool | None = False
) -> bytes:
    """
    Dump a pydantic obj to yaml bytes

    Args:
        obj: The pydantic object
        clean: Apply [clean_dict][anystore.util.clean_dict]
        newline: Add a linebreak
    """
    data = model_dump(obj, clean)
    return dump_yaml(data, newline=newline)
