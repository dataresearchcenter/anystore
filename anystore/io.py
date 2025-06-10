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

import contextlib
import csv
import logging
import sys
from enum import StrEnum
from io import BytesIO, StringIO
from typing import (
    IO,
    Any,
    AnyStr,
    BinaryIO,
    Generator,
    Iterable,
    Literal,
    Self,
    TextIO,
    Type,
    TypeAlias,
    TypeVar,
)

import orjson
from fsspec import open
from fsspec.core import OpenFile
from pydantic import BaseModel
from structlog.stdlib import BoundLogger

from anystore.exceptions import DoesNotExist
from anystore.logging import get_logger
from anystore.types import M, MGenerator, SDict, SDictGenerator
from anystore.types import Uri as _Uri
from anystore.util import clean_dict, ensure_uri

log = get_logger(__name__)

DEFAULT_MODE = "rb"
DEFAULT_WRITE_MODE = "wb"

Uri: TypeAlias = _Uri | BinaryIO | TextIO
T = TypeVar("T")
Formats = Literal["csv", "json"]
FORMAT_CSV = "csv"
FORMAT_JSON = "json"


class IOFormat(StrEnum):
    """For use in typer cli"""

    csv = "csv"
    json = "json"


def _get_sysio(mode: str | None = DEFAULT_MODE) -> TextIO | BinaryIO:
    if mode and "r" in mode:
        io = sys.stdin
    else:
        io = sys.stdout
    if mode and "b" in mode:
        return io.buffer
    return io


class SmartHandler:
    def __init__(
        self,
        uri: Uri,
        **kwargs: Any,
    ) -> None:
        self.uri = uri
        self.is_buffer = self.uri == "-"
        kwargs["mode"] = kwargs.get("mode", DEFAULT_MODE)
        self.sys_io = _get_sysio(kwargs["mode"])
        self.kwargs = kwargs
        self.handler: IO | None = None

    def open(self) -> IO[AnyStr]:
        try:
            if self.is_buffer:
                return self.sys_io
            elif isinstance(self.uri, (BytesIO, StringIO)):
                return self.uri
            else:
                self.uri = ensure_uri(self.uri, http_unquote=False)
                handler: OpenFile = open(self.uri, **self.kwargs)
                self.handler = handler.open()
                return self.handler
        except FileNotFoundError as e:
            raise DoesNotExist(str(e))

    def close(self):
        if not self.is_buffer and self.handler is not None:
            self.handler.close()

    def __enter__(self):
        return self.open()

    def __exit__(self, *args, **kwargs) -> None:
        self.close()


@contextlib.contextmanager
def smart_open(
    uri: Uri,
    mode: str | None = DEFAULT_MODE,
    **kwargs: Any,
) -> Generator[IO[AnyStr], None, None]:
    """
    IO context similar to pythons built-in `open()`.

    Example:
        ```python
        from anystore import smart_open

        with smart_open("s3://mybucket/foo.csv") as fh:
            return fh.read()
        ```

    Args:
        uri: string or path-like key uri to open, e.g. `./local/data.txt` or
            `s3://mybucket/foo`
        mode: open mode, default `rb` for byte reading.
        **kwargs: pass through storage-specific options

    Yields:
        A generic file-handler like context object
    """
    handler = SmartHandler(uri, mode=mode, **kwargs)
    try:
        yield handler.open()
    except FileNotFoundError as e:
        raise DoesNotExist(str(e))
    finally:
        handler.close()


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
        while line := fh.readline():
            yield line.strip()


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


def smart_stream_data(uri: Uri, input_format: Formats, **kwargs: Any) -> SDictGenerator:
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
    uri: Uri, model: Type[M], input_format: Formats, **kwargs: Any
) -> MGenerator:
    """
    Stream json as pydantic objects
    """
    if input_format == FORMAT_CSV:
        yield from smart_stream_csv_models(uri, model, **kwargs)
    elif input_format == FORMAT_JSON:
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


def smart_write(
    uri: Uri, content: bytes | str, mode: str | None = DEFAULT_WRITE_MODE, **kwargs: Any
) -> None:
    """
    Write content to a given file-like key directly.

    Args:
        uri: string or path-like key uri to open, e.g. `./local/data.txt` or
            `s3://mybucket/foo`
        content: `str` or `bytes` content to write.
        mode: open mode, default `wb` for byte writing.
        **kwargs: pass through storage-specific options
    """
    if uri == "-":
        if isinstance(content, str):
            content = content.encode()
    with smart_open(uri, mode, **kwargs) as fh:
        fh.write(content)


def smart_write_csv(
    uri: Uri,
    items: Iterable[SDict],
    mode: str | None = DEFAULT_WRITE_MODE,
    **kwargs: Any,
) -> None:
    """
    Write python data to csv

    Args:
        uri: string or path-like key uri to open, e.g. `./local/data.txt` or
            `s3://mybucket/foo`
        items: Iterable of dictionaries
        mode: open mode, default `wb` for byte writing.
        **kwargs: pass through storage-specific options
    """
    with Writer(uri, mode, output_format="csv", **kwargs) as writer:
        for item in items:
            writer.write(item)


def smart_write_json(
    uri: Uri,
    items: Iterable[SDict],
    mode: str | None = DEFAULT_WRITE_MODE,
    **kwargs: Any,
) -> None:
    """
    Write python data to json

    Args:
        uri: string or path-like key uri to open, e.g. `./local/data.txt` or
            `s3://mybucket/foo`
        items: Iterable of dictionaries
        mode: open mode, default `wb` for byte writing.
        **kwargs: pass through storage-specific options
    """
    with Writer(uri, mode, output_format="json", **kwargs) as writer:
        for item in items:
            writer.write(item)


def smart_write_data(
    uri: Uri,
    items: Iterable[SDict],
    mode: str | None = DEFAULT_WRITE_MODE,
    output_format: Formats | None = "json",
    **kwargs: Any,
) -> None:
    """
    Write python data to json or csv

    Args:
        uri: string or path-like key uri to open, e.g. `./local/data.txt` or
            `s3://mybucket/foo`
        items: Iterable of dictionaries
        mode: open mode, default `wb` for byte writing.
        output_format: csv or json (default: json)
        **kwargs: pass through storage-specific options
    """
    with Writer(uri, mode, output_format=output_format, **kwargs) as writer:
        for item in items:
            writer.write(item)


def smart_write_models(
    uri: Uri,
    objects: Iterable[BaseModel],
    mode: str | None = DEFAULT_WRITE_MODE,
    output_format: Formats | None = "json",
    clean: bool | None = False,
    **kwargs: Any,
) -> None:
    """
    Write pydantic objects to json lines or csv

    Args:
        uri: string or path-like key uri to open, e.g. `./local/data.txt` or
            `s3://mybucket/foo`
        objects: Iterable of pydantic objects
        mode: open mode, default `wb` for byte writing.
        clean: Apply [clean_dict][anystore.util.clean_dict]
        **kwargs: pass through storage-specific options
    """
    with ModelWriter(uri, mode, output_format, clean=clean, **kwargs) as writer:
        for obj in objects:
            writer.write(obj)


def smart_write_model(
    uri: Uri,
    obj: BaseModel,
    mode: str | None = DEFAULT_WRITE_MODE,
    output_format: Formats | None = "json",
    clean: bool | None = False,
    **kwargs: Any,
) -> None:
    """
    Write a single pydantic object to the target

    Args:
        uri: string or path-like key uri to open, e.g. `./local/data.txt` or
            `s3://mybucket/foo`
        obj: Pydantic object
        mode: open mode, default `wb` for byte writing.
        clean: Apply [clean_dict][anystore.util.clean_dict]
        **kwargs: pass through storage-specific options
    """
    with ModelWriter(uri, mode, output_format, clean=clean, **kwargs) as writer:
        writer.write(obj)


class Writer:
    """
    A generic writer for python dict objects to any out uri, either json or csv
    """

    def __init__(
        self,
        uri: Uri,
        mode: str | None = DEFAULT_WRITE_MODE,
        output_format: Formats | None = "json",
        fieldnames: list[str] | None = None,
        clean: bool | None = False,
        **kwargs,
    ) -> None:
        if output_format not in (FORMAT_JSON, FORMAT_CSV):
            raise ValueError("Invalid output format, only csv or json allowed")
        mode = mode or DEFAULT_WRITE_MODE
        self.mode = mode.replace("b", "") if output_format == "csv" else mode
        self.handler = SmartHandler(uri, mode=self.mode, **kwargs)
        self.fieldnames = fieldnames
        self.output_format = output_format
        self.clean = clean
        self.csv_writer: csv.DictWriter | None = None

    def __enter__(self) -> Self:
        self.io = self.handler.open()
        return self

    def __exit__(self, *args) -> None:
        self.handler.close()

    def write(self, row: SDict) -> None:
        if self.output_format == "csv" and self.csv_writer is None:
            self.csv_writer = csv.DictWriter(self.io, self.fieldnames or row.keys())
            self.csv_writer.writeheader()

        if self.output_format == "json":
            if self.clean:
                row = clean_dict(row)
            line = orjson.dumps(row, option=orjson.OPT_APPEND_NEWLINE)
            if "b" not in self.mode:
                line = line.decode()
            self.io.write(line)
        elif self.csv_writer:
            self.csv_writer.writerow(row)


class ModelWriter(Writer):
    """
    A generic writer for pydantic objects to any out uri, either json or csv
    """

    def write(self, row: BaseModel) -> None:
        data = row.model_dump(by_alias=True, mode="json")
        return super().write(data)


def logged_items(
    items: Iterable[T],
    action: str,
    chunk_size: int | None = 10_000,
    item_name: str | None = None,
    logger: logging.Logger | BoundLogger | None = None,
    **log_kwargs,
) -> Generator[T, None, None]:
    """
    Log process of iterating items for io operations.

    Example:
        ```python
        from anystore.io import logged_items

        items = [...]
        for item in logged_items(items, "Read", uri="/tmp/foo.csv"):
            yield item
        ```

    Args:
        items: Sequence of any items
        action: Action name to log
        chunk_size: Log on every chunk_size
        item_name: Name of item
        logger: Specific logger to use

    Yields:
        The input items
    """
    log_ = logger or log
    chunk_size = chunk_size or 10_000
    ix = 0
    item_name = item_name or "Item"
    for ix, item in enumerate(items, 1):
        if ix == 1:
            item_name = item_name or item.__class__.__name__.title()
        if ix % chunk_size == 0:
            item_name = item_name or item.__class__.__name__.title()
            log_.info(f"{action} `{item_name}` {ix} ...", **log_kwargs)
        yield item
    if ix:
        log_.info(f"{action} {ix} `{item_name}s`: Done.", **log_kwargs)
