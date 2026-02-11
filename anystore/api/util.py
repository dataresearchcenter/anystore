from __future__ import annotations

from typing import IO, Iterator

from anystore.logic.constants import CHUNK_SIZE
from anystore.model import Stats
from anystore.store.base import Store


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


def is_file(store: Store, key: str) -> bool:
    """Return True only if *key* is an actual stored value (not a directory prefix)."""
    fs_key = store._keys.to_fs_key(key)
    try:
        info = store._fs.info(fs_key)
        return info.get("type") != "directory"
    except FileNotFoundError:
        return False


def stats_headers(stats: Stats) -> dict[str, str]:
    """Build HTTP response headers from a Stats object."""
    headers: dict[str, str] = {}
    headers["Content-Length"] = str(stats.size)
    headers["Content-Type"] = stats.mimetype
    headers["Accept-Ranges"] = "bytes"
    ts = stats.updated_at or stats.created_at
    if ts is not None:
        headers["Last-Modified"] = ts.strftime("%a, %d %b %Y %H:%M:%S GMT")
    for field, value in stats.model_dump(mode="json").items():
        if value is None:
            continue
        headers[f"x-anystore-{field.replace('_', '-')}"] = str(value)
    return headers
