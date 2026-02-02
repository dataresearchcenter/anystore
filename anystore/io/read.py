"""
# Generic io helpers

`anystore` is built on top of
[`fsspec`](https://filesystem-spec.readthedocs.io/en/latest/index.html) and
provides an easy wrapper for reading and writing content from and to arbitrary
locations using the `io` command:

Command-line usage:
    ```bash
    anystore io -i ./local/foo.txt -o s3://mybucket/other.txt

    echo "hello" | anystore io -o sftp://user:password@host:/tmp/world.txt

    anystore io -i https://investigativedata.io > index.html
    ```

Python usage:
    ```python
    from anystore import smart_read, smart_write

    data = smart_read("s3://mybucket/data.txt")
    smart_write(".local/data", data)
    ```
"""

from __future__ import annotations

import csv
from typing import Any, AnyStr, ContextManager, Generator, Type

import orjson

from anystore.io.handler import smart_open
from anystore.io.logging import logged_items
from anystore.logic.constants import DEFAULT_MODE
from anystore.logic.io import Uri, iter_lines
from anystore.logic.virtual import VirtualIO
from anystore.store.resource import UriResource
from anystore.types import M, MGenerator, SDictGenerator
from anystore.types import Uri as TUri

Formats = str  # "csv" | "json" — re-exported from write.py at package level


def smart_stream(
    uri: Uri, mode: str | None = DEFAULT_MODE, **kwargs: Any
) -> Generator[AnyStr, None, None]:
    """
    Stream content line by line.

    Example:
        ```python
        import orjson
        from anystore import smart_stream

        while data := smart_stream("s3://mybucket/data.json"):
            yield orjson.loads(data)
        ```

    Args:
        uri: string or path-like key uri to open, e.g. `./local/data.txt` or
            `s3://mybucket/foo`
        mode: open mode, default `rb` for byte reading.
        **kwargs: pass through storage-specific options

    Yields:
        A generator of `str` or `byte` content, depending on `mode`
    """
    with smart_open(uri, mode, **kwargs) as fh:
        yield from iter_lines(fh)


def smart_stream_csv(uri: Uri, **kwargs: Any) -> SDictGenerator:
    """
    Stream csv as python objects.

    Example:
        ```python
        from anystore import smart_stream_csv

        for data in smart_stream_csv("s3://mybucket/data.csv"):
            yield data.get("foo")
        ```

    Args:
        uri: string or path-like key uri to open, e.g. `./local/data.txt` or
            `s3://mybucket/foo`
        **kwargs: pass through storage-specific options

    Yields:
        A generator of `dict`s loaded via `csv.DictReader`
    """
    kwargs["mode"] = "r"
    with smart_open(uri, **kwargs) as f:
        yield from csv.DictReader(f)


def smart_stream_csv_models(uri: Uri, model: Type[M], **kwargs: Any) -> MGenerator:
    """
    Stream csv as pydantic objects
    """
    for row in logged_items(
        smart_stream_csv(uri, **kwargs),
        "Read",
        uri=uri,
        item_name=model.__name__,
    ):
        yield model(**row)


def smart_stream_json(
    uri: Uri, mode: str | None = DEFAULT_MODE, **kwargs: Any
) -> SDictGenerator:
    """
    Stream line-based json as python objects.

    Example:
        ```python
        from anystore import smart_stream_json

        for data in smart_stream_json("s3://mybucket/data.json"):
            yield data.get("foo")
        ```

    Args:
        uri: string or path-like key uri to open, e.g. `./local/data.txt` or
            `s3://mybucket/foo`
        mode: open mode, default `rb` for byte reading.
        **kwargs: pass through storage-specific options

    Yields:
        A generator of `dict`s loaded via `orjson`
    """
    for line in smart_stream(uri, mode, **kwargs):
        yield orjson.loads(line)


def smart_stream_json_models(uri: Uri, model: Type[M], **kwargs: Any) -> MGenerator:
    """
    Stream json as pydantic objects
    """
    for row in logged_items(
        smart_stream_json(uri, **kwargs),
        "Read",
        uri=uri,
        item_name=model.__name__,
    ):
        yield model(**row)


def smart_stream_data(uri: Uri, input_format: str, **kwargs: Any) -> SDictGenerator:
    """
    Stream data objects loaded as dict from json or csv sources

    Args:
        uri: string or path-like key uri to open, e.g. `./local/data.txt` or
            `s3://mybucket/foo`
        input_format: csv or json
        **kwargs: pass through storage-specific options

    Yields:
        A generator of `dict`s loaded via `orjson`
    """
    if input_format == "csv":
        yield from smart_stream_csv(uri, **kwargs)
    else:
        yield from smart_stream_json(uri, **kwargs)


def smart_stream_models(
    uri: Uri, model: Type[M], input_format: str, **kwargs: Any
) -> MGenerator:
    """
    Stream json as pydantic objects
    """
    if input_format == "csv":
        yield from smart_stream_csv_models(uri, model, **kwargs)
    elif input_format == "json":
        yield from smart_stream_json_models(uri, model, **kwargs)
    else:
        raise ValueError("Invalid format, only csv or json allowed")


def smart_read(uri: Uri, mode: str | None = DEFAULT_MODE, **kwargs: Any) -> AnyStr:
    """
    Return content for a given file-like key directly.

    Args:
        uri: string or path-like key uri to open, e.g. `./local/data.txt` or
            `s3://mybucket/foo`
        mode: open mode, default `rb` for byte reading.
        **kwargs: pass through storage-specific options

    Returns:
        `str` or `byte` content, depending on `mode`
    """
    with smart_open(uri, mode, **kwargs) as fh:
        return fh.read()


def open_virtual(uri: "TUri", **kwargs) -> ContextManager[VirtualIO]:
    """Wrapper for [UriResource.local_open][anystore.store.resource.UriResource.local_open]"""
    return UriResource(uri, **kwargs).local_open()
