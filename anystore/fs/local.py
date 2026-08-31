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

    def exists(self, path, **kwargs):
        """Use fast path instead of AbstractFileSystem checking via .info()"""
        return os.path.lexists(self._strip_protocol(path))

    def iter_find(
        self, path: str, glob: str | None = None, depth: int | None = None
    ) -> Generator[str, None, None]:
        """Yield file paths under *path* lazily using os.walk.

        Args:
            path: Root directory (or file) to search.
            glob: Optional glob pattern matched against paths relative to
                *path* (uses the same syntax as fsspec glob).
            depth: Optional maximum number of path segments a yielded key may
                have, relative to *path* (`depth=1` are the files directly in
                it). Directories beyond it are never descended into, so a
                shallow listing does not pay for a deep tree.
        """
        path = self._strip_protocol(path)

        if depth is not None and depth < 1:
            return  # "at most zero segments" can never match a key

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

        for dirpath, dirnames, filenames in os.walk(path):
            if depth is not None:
                # a file here has `level + 1` segments; level 0 is *path*
                level = (
                    0
                    if dirpath == path
                    else os.path.relpath(dirpath, path).count(os.sep) + 1
                )
                if level + 1 >= depth:
                    # nothing below this can fit -- prune in place so os.walk
                    # never descends into it. That pruning is also why no
                    # level beyond `depth` is ever reached here.
                    dirnames[:] = []
            for name in filenames:
                full = os.path.join(dirpath, name)
                if rx is not None:
                    rel = os.path.relpath(full, path)
                    if not rx.fullmatch(rel):
                        continue
                yield full
