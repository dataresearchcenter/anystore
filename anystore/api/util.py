from __future__ import annotations

from typing import IO, Iterator

from anystore.logic.io import CHUNK_SIZE


def iter_chunks(fh: IO, size: int = CHUNK_SIZE) -> Iterator[bytes]:
    while chunk := fh.read(size):
        yield chunk


def iter_range(
    fh: IO, start: int, length: int, size: int = CHUNK_SIZE
) -> Iterator[bytes]:
    """Seek to *start* and yield *length* bytes in chunks."""
    fh.seek(start)
    remaining = length
    while remaining > 0:
        chunk = fh.read(min(size, remaining))
        if not chunk:
            break
        remaining -= len(chunk)
        yield chunk


def parse_range(header: str, total: int) -> tuple[int, int]:
    """Parse an HTTP Range header (bytes only), return (start, end) inclusive."""
    if not header.startswith("bytes="):
        raise ValueError("Only byte ranges supported")
    spec = header[len("bytes=") :]
    if spec.startswith("-"):
        # suffix: last N bytes
        suffix = int(spec[1:])
        return max(total - suffix, 0), total - 1
    parts = spec.split("-", 1)
    start = int(parts[0])
    end = int(parts[1]) if parts[1] else total - 1
    return start, min(end, total - 1)
