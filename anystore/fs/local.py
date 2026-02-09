"""
fsspec-compatible local filesystem with streaming iter_find.
"""

from __future__ import annotations

import os
import re
from typing import Generator

from fsspec.implementations.local import LocalFileSystem
from fsspec.utils import glob_translate


class AnyLocalFileSystem(LocalFileSystem):
    """LocalFileSystem subclass that adds lazy iter_find via os.walk."""

    protocol = ("file", "local")

    def iter_find(
        self, path: str, glob: str | None = None
    ) -> Generator[str, None, None]:
        """Yield file paths under *path* lazily using os.walk.

        Args:
            path: Root directory (or file) to search.
            glob: Optional glob pattern matched against paths relative to
                *path* (uses the same syntax as fsspec glob).
        """
        path = self._strip_protocol(path)

        if os.path.isfile(path):
            if glob:
                rel = os.path.basename(path)
                rx = re.compile(glob_translate(glob))
                if rx.fullmatch(rel):
                    yield path
            else:
                yield path
            return

        if not os.path.isdir(path):
            return

        rx = re.compile(glob_translate(glob)) if glob else None

        for dirpath, _, filenames in os.walk(path):
            for name in filenames:
                full = os.path.join(dirpath, name)
                if rx is not None:
                    rel = os.path.relpath(full, path)
                    if not rx.fullmatch(rel):
                        continue
                yield full
