from functools import cached_property
from urllib.parse import ParseResult, urlparse

import fsspec
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from anystore.types import Uri
from anystore.util import ensure_uri, join_uri


class UriHandler:
    def __init__(self, uri: Uri, **kwargs) -> None:
        self.uri = ensure_uri(uri)
        self._kwargs = kwargs

    def __str__(self) -> str:
        return self.uri

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.uri})>"

    def __contains__(self, other: str) -> bool:
        return other in str(self.uri)

    def __truediv__(self, other: str) -> "UriHandler":
        return self.__class__(join_uri(self.uri, other), **self._kwargs)

    @cached_property
    def parsed(self) -> ParseResult:
        return urlparse(self.uri)

    @cached_property
    def scheme(self) -> str:
        return self.parsed.scheme

    @cached_property
    def _fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.url_to_fs(self.uri, **self._kwargs)[0]

    @cached_property
    def is_local(self) -> bool:
        return isinstance(self._fs, LocalFileSystem)

    @cached_property
    def is_http(self) -> bool:
        return isinstance(self._fs, HTTPFileSystem)

    @cached_property
    def is_s3(self) -> bool:
        return self.scheme == "s3"

    @cached_property
    def is_memory(self) -> bool:
        return isinstance(self._fs, MemoryFileSystem)

    @cached_property
    def is_redis(self) -> bool:
        from anystore.fs.redis import RedisFileSystem

        return isinstance(self._fs, RedisFileSystem)

    @cached_property
    def is_sql(self) -> bool:
        from anystore.fs.sql import SqlFileSystem

        return isinstance(self._fs, SqlFileSystem)
