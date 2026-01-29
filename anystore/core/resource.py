"""
A UriResource wraps a single URI and provides the full Store interface
with the key pre-bound. Consumers use it as:

    resource = UriResource("s3://bucket/path/file.txt")
    data = resource.get(raise_on_nonexist=True)
    resource.put(b"hello")
"""

from __future__ import annotations

from datetime import datetime
from typing import IO, Any, Callable, ContextManager, Generator

from anystore.logic.uri import UriHandler
from anystore.model import Stats
from anystore.serialize import Mode
from anystore.store.base import Store
from anystore.types import Model, Uri
from anystore.util import CURRENT


class UriResource(UriHandler):
    """Key-bound facade over a :class:`Store`.

    Example:
        ```python
        from anystore.core.resource import UriResource

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
            self.store = Store(uri=base)
            self.key = key
        else:  # https://example.org
            self.store = Store(uri=self.uri)
            self.key = CURRENT

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

    def exists(self) -> bool:
        return self.store.exists(self.key)

    def info(self) -> Stats:
        return self.store.info(self.key)

    def checksum(self, algorithm: str | None = None, **kwargs: Any) -> str:
        return self.store.checksum(self.key, algorithm=algorithm, **kwargs)

    def open(self, mode: str | None = None, **kwargs: Any) -> ContextManager[IO]:
        return self.store.open(self.key, mode=mode, **kwargs)

    def touch(self, **kwargs: Any) -> datetime:
        return self.store.touch(self.key, **kwargs)
