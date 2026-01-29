from __future__ import annotations

import contextlib
import sys
from io import BytesIO, IOBase, StringIO
from typing import IO, Any, AnyStr, BinaryIO, Generator, TextIO, TypeAlias

from anystore.exceptions import DoesNotExist
from anystore.types import Uri as _Uri

CHUNK_SIZE = 8192
DEFAULT_MODE = "rb"
DEFAULT_WRITE_MODE = "wb"

Uri: TypeAlias = _Uri | BinaryIO | TextIO


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
            elif isinstance(self.uri, (BytesIO, StringIO, IOBase)):
                return self.uri
            else:
                from anystore.core.resource import UriResource

                resource = UriResource(self.uri)
                mode = self.kwargs.pop("mode", DEFAULT_MODE)
                self.handler = resource.open(mode, **self.kwargs).__enter__()
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


def stream(reader: IO, writer: IO, chunk_size: int = CHUNK_SIZE) -> None:
    """Copy data from *reader* to *writer* in chunks."""
    while chunk := reader.read(chunk_size):
        writer.write(chunk)
