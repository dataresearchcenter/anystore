"""Core handler for store absolute/relative key conversion"""

from functools import cached_property

import fsspec
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from anystore.fs.api import ApiFileSystem
from anystore.fs.redis import RedisFileSystem
from anystore.fs.sql import SqlFileSystem
from anystore.logic.uri import UriHandler, validate_relative_uri, validate_uri
from anystore.types import Uri


class Keys:
    def __init__(self, uri: Uri) -> None:
        self.uri = UriHandler(uri)
        self.fs, self._base_path = fsspec.url_to_fs(uri)

    def __repr__(self) -> str:
        return f"<Keys({self.uri})>"

    @cached_property
    def key_prefix(self) -> str:
        if isinstance(self.fs, LocalFileSystem):
            return self.uri.parsed.path.rstrip("/")
        path = self.uri.parsed.path.strip("/")
        if isinstance(self.fs, MemoryFileSystem):
            return self._base_path.strip("/")
        if isinstance(self.fs, RedisFileSystem):
            return path
        if isinstance(self.fs, SqlFileSystem):
            return ""
        if isinstance(self.fs, ApiFileSystem):
            return ApiFileSystem._strip_protocol(str(self.uri))
        if isinstance(self.fs, HTTPFileSystem):
            return str(self.uri)
        # other fsspec implementations want relative path with netloc
        base = self.uri.parsed.netloc
        if path:
            return f"{base}/{path}"
        return base

    def to_fs_key(self, key: Uri) -> str:
        """Convert a relative key to the backend fs key"""
        key = validate_relative_uri(key)
        if self.key_prefix:
            if key:
                return f"{self.key_prefix}/{key}"
            return self.key_prefix
        return key

    def from_fs_key(self, key: Uri) -> str:
        """Convert a fs key to relative key"""
        key = validate_uri(key)
        # MemoryFileSystem.find() may return keys with a leading slash
        if isinstance(self.fs, MemoryFileSystem):
            key = key.lstrip("/")
        if key.startswith(self.key_prefix):
            return key[len(self.key_prefix) :].strip("/")
        raise ValueError(f"Invalid key `{key}`, doesn't has base `{self.key_prefix}`")

    def to_absolute_uri(self, key: Uri) -> str:
        return str(self.uri / key)
