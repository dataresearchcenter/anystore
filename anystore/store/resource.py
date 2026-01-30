"""
A UriResource wraps a single URI and provides the full Store interface
with the key pre-bound. Consumers use it as:

    resource = UriResource("s3://bucket/path/file.txt")
    data = resource.get(raise_on_nonexist=True)
    resource.put(b"hello")
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import IO, Any, Callable, ContextManager, Generator

from anystore.logic.serialize import Mode
from anystore.logic.uri import CURRENT, UriHandler
from anystore.logic.virtual import VirtualIO
from anystore.model import Stats
from anystore.store.base import Store
from anystore.types import Model, Uri


class UriResource(UriHandler):
    """Key-bound facade over a :class:`Store`.

    Example:
        ```python
        from anystore.store.resource import UriResource

        r = UriResource("s3://bucket/path/file.txt")
        r.put(b"hello world")
        assert r.exists()
        data = r.get(raise_on_nonexist=True)
        ```
    """

    def __init__(self, uri: Uri, **kwargs: Any) -> None:
        super().__init__(uri, **kwargs)
        if self.parsed.path:
            base, key = self.uri.rsplit("/", 1)
            self.store = Store(uri=base, **kwargs)
            self.key = key
        else:  # https://example.org
            self.store = Store(uri=self.uri, **kwargs)
            self.key = CURRENT

    def info(self) -> Stats:
        return self.store.info(self.key)

    def checksum(self, *args, **kwargs) -> str:
        return self.store.checksum(self.key, *args, **kwargs)

    def exists(self) -> bool:
        return self.store.exists(self.key)

    def get(
        self,
        raise_on_nonexist: bool | None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Any:
        return self.store.get(
            self.key,
            raise_on_nonexist=raise_on_nonexist,
            serialization_mode=serialization_mode,
            deserialization_func=deserialization_func,
            model=model,
            **kwargs,
        )

    def pop(
        self,
        raise_on_nonexist: bool | None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Any:
        return self.store.pop(
            self.key,
            raise_on_nonexist=raise_on_nonexist,
            serialization_mode=serialization_mode,
            deserialization_func=deserialization_func,
            model=model,
            **kwargs,
        )

    def stream(
        self,
        raise_on_nonexist: bool | None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Generator[Any, None, None] | None:
        return self.store.stream(
            self.key,
            raise_on_nonexist=raise_on_nonexist,
            serialization_mode=serialization_mode,
            deserialization_func=deserialization_func,
            model=model,
            **kwargs,
        )

    def put(
        self,
        value: Any,
        serialization_mode: Mode | None = None,
        serialization_func: Callable | None = None,
        model: Model | None = None,
        ttl: int | None = None,
        **kwargs,
    ) -> None:
        self.store.put(
            self.key,
            value,
            serialization_mode=serialization_mode,
            serialization_func=serialization_func,
            model=model,
            ttl=ttl,
            **kwargs,
        )

    def delete(self, ignore_errors: bool = False) -> None:
        self.store.delete(self.key, ignore_errors=ignore_errors)

    def open(self, mode: str | None = None, **kwargs: Any) -> ContextManager[IO]:
        return self.store.open(self.key, mode=mode, **kwargs)

    def touch(self, **kwargs: Any) -> datetime:
        return self.store.touch(self.key, **kwargs)

    def local_path(self) -> ContextManager[Path]:
        """
        Download the resource for temporary local processing and get its local
        path. If the file itself is already on the local filesystem, the actual
        file will be used. The file is cleaned up when leaving the context,
        unless it was a local file, it won't be deleted in any case.

        Example:
            ```python
            r = UriResource("http://example.org/test.txt")
            with r.local_path() as path:
                do_something(path)
            ```
        Yields:
            The absolute temporary `path` as a `pathlib.Path` object
        """
        return self.store.local_path(self.key)

    def local_open(self) -> ContextManager[VirtualIO]:
        """
        Download a file for temporary local processing and get its checksum and
        an open handler. If the file itself is already on the local filesystem,
        the actual file will be used. The file is cleaned up when leaving the
        context, except if it was a local file, it won't be deleted in any case.

        Example:
            ```python
            r = UriResource("http://example.org/test.txt")
            with r.local_open() as fh:
                smart_write(uri=f"./local/{fh.checksum}", fh.read())
            ```

        Yields:
            A generic file-handler like context object. It has 3 extra attributes:
                - `checksum`
                - the absolute temporary `path` as a `pathlib.Path` object
                - [`info`][anystore.model.Stats] object
        """
        return self.store.local_open(self.key)
