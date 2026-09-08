from __future__ import annotations

import contextlib
import sys
from io import BytesIO, IOBase, StringIO
from typing import IO, Any, AnyStr, BinaryIO, Generator, TextIO

from anystore.exceptions import DoesNotExist
from anystore.logic.compress import CompressKind, binary_mode, open_codec
from anystore.logic.constants import DEFAULT_MODE
from anystore.logic.io import Uri
from anystore.store.resource import UriResource


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
        compression: CompressKind | str | None = None,
        **kwargs: Any,
    ) -> None:
        self.uri = uri
        self.is_buffer = self.uri == "-"
        kwargs["mode"] = kwargs.get("mode", DEFAULT_MODE)
        self.mode = kwargs["mode"]
        self.compression = compression
        # stdio and an injected handle need the codec applied here; a uri goes
        # through `Store.open`, which applies it at the funnel – doing both
        # would encode the stream twice
        self.sys_io = _get_sysio(binary_mode(self.mode) if compression else self.mode)
        self.kwargs = kwargs
        self.handler: IO | None = None

    def open(self) -> IO[AnyStr]:
        try:
            if self.is_buffer:
                return self._wrap(self.sys_io)
            elif isinstance(self.uri, (BytesIO, StringIO, IOBase)):
                return self._wrap(self.uri)
            else:
                resource = UriResource(self.uri)
                mode = self.kwargs.pop("mode", DEFAULT_MODE)
                self.handler = resource.open(
                    mode, compression=self.compression, **self.kwargs
                ).__enter__()
                return self.handler
        except FileNotFoundError as e:
            raise DoesNotExist(str(e))

    def _wrap(self, io: IO[Any]) -> IO[Any]:
        """Apply the codec to a handle this class did not open.

        `own=False`, so closing the codec flushes its frame and leaves the
        handle for whoever owns it – stdout, or the caller who passed it in.
        The codec itself does become ours to close, so it is tracked.
        """
        if self.compression is None:
            return io
        self.handler = open_codec(io, self.compression, self.mode)
        return self.handler

    def close(self):
        # a tracked handler is always ours to close: either the store's stream
        # or a codec we layered over someone else's handle
        if self.handler is not None:
            self.handler.close()

    def __enter__(self):
        return self.open()

    def __exit__(self, *args, **kwargs) -> None:
        self.close()


@contextlib.contextmanager
def smart_open(
    uri: Uri,
    mode: str | None = DEFAULT_MODE,
    compression: CompressKind | str | None = None,
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
        compression: Codec to (de-)compress the stream with ("gz", "zst")
        **kwargs: pass through storage-specific options

    Yields:
        A generic file-handler like context object
    """
    handler = SmartHandler(uri, mode=mode, compression=compression, **kwargs)
    try:
        yield handler.open()
    except FileNotFoundError as e:
        raise DoesNotExist(str(e))
    finally:
        handler.close()
