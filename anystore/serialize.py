"""
Usually, these functions are not called directly by a program but by the higher
level `Store.get` or `Store.put` methods.

The store backends assume `bytes` to be written to and read from. The two main
functions, `to_store` and `from_store` (de)serialize given values based on
configuration. See higher level `anystore.store.BaseStore` for how to use
these serialization options in the stores `get`, `put` and `stream` methods.

Serialization options:

**serialization_mode**:

- "raw": Return value as is, assuming bytes
- "json": Use [`orjson`](https://pypi.org/project/orjson/) to (de)serialize
- "pickle": Use [`cloudpickle`](https://pypi.org/project/cloudpickle/) to
    (de)serialize
- "auto": Try different serialization methods, see below

**serialization_func**:

A callable that serializes the input to bytes

**deserialization_func**:

A callable that deserializes the bytes input to any data

**model**

A pydantic model class used for (de)serialization

"""

from typing import Any, Callable, Literal, TypeAlias

import cloudpickle
import orjson
from pydantic import BaseModel

from anystore.types import Model

Mode: TypeAlias = Literal["auto", "raw", "pickle", "json"]


def to_store(
    value: Any,
    serialization_mode: Mode | None = "auto",
    serialization_func: Callable | None = None,
    model: Model | None = None,
) -> bytes:
    """
    Serialize the given value to bytes.

    In "auto" mode, this tries to serialize `value` in the following ways:

    - If `value` is `bytes`, just store it
    - If `value` is `str`, encode to `bytes`
    - If `value` is an instance of a pydantic `BaseModel`, it is dumped to it's json byte string
    - If it is possible to serialize `value` to json, it is stored as that byte string
    - Try to cloudpickle or raise an error

    Args:
        serialization_mode: "auto", "pickle", "json", "raw"
        serialization_func: Function to use to serialize
        model: Pydantic model to use for serialization
    """
    if model is not None and value:
        return value.model_dump_json().encode()
    if serialization_func is not None:
        value = serialization_func(value)

    mode = serialization_mode or "auto"
    if mode == "json":
        return orjson.dumps(value)
    if mode == "pickle":
        return cloudpickle.dumps(value)
    if mode == "raw":
        if not isinstance(value, bytes):
            raise ValueError("Value is not bytes")
        return value

    # auto
    if isinstance(value, BaseModel):
        return value.model_dump_json().encode()
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    try:
        return orjson.dumps(value)
    except (orjson.JSONEncodeError, TypeError, ValueError):
        return cloudpickle.dumps(value)


def from_store(
    value: bytes,
    serialization_mode: Mode | None = "auto",
    deserialization_func: Callable | None = None,
    model: Model | None = None,
) -> Any:
    """
    Deserialize the bytes value retrieved from a store backend to any data.

    In "auto" mode, this tries to deserialize `value` in the following ways:

    - Try to load a data object via `orjson` from the input
    - Try to deserialize via `cloudpickle`
    - Try to decode the value to `str` (use mode="raw" to make sure to get `bytes` values)
    - Return the unserialized bytes value

    Args:
        serialization_mode: "auto", "pickle", "json", "raw"
        deserialization_func: Function to use to deserialize, takes bytes as input
        model: Pydantic model to use for serialization from a json bytes string

    Returns:
        The deserialized object
    """
    if model is not None:
        data = orjson.loads(value)
        if data:
            return model(**data)
    if deserialization_func is not None:
        value = deserialization_func(value)

    mode = serialization_mode or "auto"
    if mode == "raw":
        return value
    if mode == "pickle":
        return cloudpickle.loads(value)
    if mode == "json":
        return orjson.loads(value)

    # auto
    try:
        return orjson.loads(value)
    except (orjson.JSONDecodeError, TypeError, ValueError):
        try:
            return cloudpickle.loads(value)
        except Exception:
            if isinstance(value, bytes):
                try:
                    return value.decode()
                except UnicodeDecodeError:
                    pass
            return value
