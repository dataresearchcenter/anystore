from pathlib import Path
from typing import BinaryIO

from anystore.model import Stats


class VirtualIO:
    """Thin wrapper around an open binary file handle with extra metadata.

    Exposes the full :class:`BinaryIO` interface via delegation.
    """

    def __init__(self, fh: BinaryIO, checksum: str, path: Path, info: Stats) -> None:
        self._fh = fh
        self.checksum = checksum
        self.path = path
        self.info = info

    # -- BinaryIO interface (explicit for type checkers / IDE support) --

    def read(self, n: int = -1) -> bytes:
        return self._fh.read(n)

    def readline(self, limit: int = -1) -> bytes:
        return self._fh.readline(limit)

    def readlines(self, hint: int = -1) -> list[bytes]:
        return self._fh.readlines(hint)

    def write(self, s: bytes) -> int:
        return self._fh.write(s)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._fh.seek(offset, whence)

    def tell(self) -> int:
        return self._fh.tell()

    def close(self) -> None:
        self._fh.close()

    def flush(self) -> None:
        self._fh.flush()

    def readable(self) -> bool:
        return self._fh.readable()

    def seekable(self) -> bool:
        return self._fh.seekable()

    def writable(self) -> bool:
        return self._fh.writable()

    @property
    def closed(self) -> bool:
        return self._fh.closed

    # -- fallback for anything else --

    def __getattr__(self, name: str):
        return getattr(self._fh, name)

    def __iter__(self):
        return iter(self._fh)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self._fh.close()
