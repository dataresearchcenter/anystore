from __future__ import annotations

import csv
from enum import StrEnum
from typing import IO, Any, Iterable, Literal, Self

import orjson
from pydantic import BaseModel

from anystore.io.handler import SmartHandler, smart_open
from anystore.logic.compress import CompressKind
from anystore.logic.constants import DEFAULT_WRITE_MODE
from anystore.logic.io import Uri
from anystore.types import SDict
from anystore.util.data import clean_dict

Formats = Literal["csv", "json"]
FORMAT_CSV = "csv"
FORMAT_JSON = "json"


class IOFormat(StrEnum):
    """For use in typer cli"""

    csv = "csv"
    json = "json"


def _default_serializer(obj: Any) -> str:
    """Custom serializer for orjson to handle types like pd.Timestamp / datetime"""
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)


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
        clean: Apply [clean_dict][anystore.util.data.clean_dict]
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
        clean: Apply [clean_dict][anystore.util.data.clean_dict]
        **kwargs: pass through storage-specific options
    """
    with ModelWriter(uri, mode, output_format, clean=clean, **kwargs) as writer:
        writer.write(obj)


class Writer:
    """
    A generic writer for python dict objects to any out uri, either json or csv

    Args:
        uri: string or path-like key uri to write to, `"-"` for stdout, or an
            already open handle
        mode: open mode, default `wb` (forced to text for csv)
        output_format: csv or json (default: json)
        fieldnames: csv header, inferred from the first row when omitted
        clean: Apply [clean_dict][anystore.util.data.clean_dict]
        compression: Codec to compress the output with ("gz", "bz2", "xz",
            "zst", "lz4")
        lazy: Defer creating the target to the first `write`, so a run that
            writes nothing leaves no file behind. Off by default – an empty
            csv carrying just its header is a legitimate thing to want.
        **kwargs: pass through storage-specific options
    """

    def __init__(
        self,
        uri: Uri,
        mode: str | None = DEFAULT_WRITE_MODE,
        output_format: Formats | None = "json",
        fieldnames: list[str] | None = None,
        clean: bool | None = False,
        compression: CompressKind | str | None = None,
        lazy: bool | None = False,
        **kwargs,
    ) -> None:
        if output_format not in (FORMAT_JSON, FORMAT_CSV):
            raise ValueError("Invalid output format, only csv or json allowed")
        mode = mode or DEFAULT_WRITE_MODE
        self.mode = mode.replace("b", "") if output_format == "csv" else mode
        self.handler = SmartHandler(
            uri, mode=self.mode, compression=compression, **kwargs
        )
        self.fieldnames = fieldnames
        self.output_format = output_format
        self.clean = clean
        self.lazy = lazy
        self.csv_writer: csv.DictWriter | None = None
        self._io: IO[Any] | None = None

    def open(self) -> IO[Any]:
        """The open target, created on first ask.

        A csv whose header is known up front writes it here, so a writer that
        never sees a row still leaves a complete – if empty – table instead of
        a zero-byte file.
        """
        if self._io is None:
            self._io = self.handler.open()
            if self.output_format == FORMAT_CSV and self.fieldnames:
                self._start_csv(self.fieldnames)
        return self._io

    def _start_csv(self, fieldnames: Iterable[str]) -> None:
        """Bind the csv writer to the open target and write its header."""
        self.csv_writer = csv.DictWriter(self.io, fieldnames)
        self.csv_writer.writeheader()

    def close(self) -> None:
        """Flush and close the target, if one was ever opened."""
        self.handler.close()

    @property
    def io(self) -> IO[Any]:
        return self.open()

    def __enter__(self) -> Self:
        if not self.lazy:
            self.open()
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def write(self, row: SDict) -> None:
        if self.output_format == FORMAT_CSV:
            # opening starts the writer itself when the header is known up front
            self.open()
            if self.csv_writer is None:
                self._start_csv(row.keys())

        if self.output_format == FORMAT_JSON:
            if self.clean:
                row = clean_dict(row)
            line = orjson.dumps(
                row,
                default=_default_serializer,
                option=orjson.OPT_APPEND_NEWLINE | orjson.OPT_NAIVE_UTC,
            )
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
