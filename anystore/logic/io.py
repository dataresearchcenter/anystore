from __future__ import annotations

from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    AnyStr,
    BinaryIO,
    Generator,
    TextIO,
    TypeAlias,
)

from anystore.logic.constants import CHUNK_SIZE
from anystore.types import Uri as _Uri

if TYPE_CHECKING:
    from anystore.store import Store

Uri: TypeAlias = _Uri | BinaryIO | TextIO


def stream(reader: IO, writer: IO, chunk_size: int = CHUNK_SIZE) -> None:
    """Copy data from *reader* to *writer* in chunks."""
    while chunk := reader.read(chunk_size):
        writer.write(chunk)


def stream_bytes(key: str, source: "Store", target: "Store", **kwargs: Any) -> None:
    """Stream binary content for *key* from *source* to *target* store."""
    with source.open(key, "rb", **kwargs) as i:
        with target.open(key, "wb") as o:
            stream(i, o)


def _is_seekable(fh: IO) -> bool:
    """Test whether a file handle actually supports seeking.

    ``fsspec``'s ``HTTPStreamFile.seekable()`` returns ``True`` but
    ``seek`` raises ``ValueError``, so we perform a real no-op seek
    instead of trusting the method.
    """
    try:
        fh.seek(1, 0)
        fh.seek(0, 0)
        return True
    except (ValueError, OSError):
        return False


def _iter_lines_chunked(
    fh: IO, chunk_size: int = CHUNK_SIZE
) -> Generator[AnyStr, None, None]:
    """Line iterator using chunk-based reading for non-seekable streams."""
    probe = fh.read(0)
    sep = b"\n" if isinstance(probe, bytes) else "\n"
    buf = b"" if isinstance(probe, bytes) else ""
    while chunk := fh.read(chunk_size):
        buf += chunk
        while sep in buf:
            line, buf = buf.split(sep, 1)
            if line.strip():
                yield line.strip()
    if buf.strip():
        yield buf.strip()


def iter_lines(fh: IO, chunk_size: int = CHUNK_SIZE) -> Generator[AnyStr, None, None]:
    """Iterate stripped lines from a file handle, falling back to chunk-based
    reading for non-seekable streams (e.g. HTTP streaming files in fsspec)."""
    if _is_seekable(fh):
        while line := fh.readline():
            yield line.strip()
    else:
        yield from _iter_lines_chunked(fh, chunk_size)
