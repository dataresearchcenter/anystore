"""Core handler for store absolute/relative key conversion"""

from functools import cached_property
from urllib.parse import unquote

import fsspec

try:
    from fsspec.implementations.http import HTTPFileSystem

    CAN_HTTP = True
except ImportError:
    HTTPFileSystem = None
    CAN_HTTP = False

from anystore.logic.constants import SCHEME_FILE, SCHEME_MEMORY, SCHEME_REDIS
from anystore.logic.uri import (
    UriHandler,
    split_uri,
    validate_relative_uri,
    validate_uri,
)
from anystore.types import Uri


class Keys:
    def __init__(self, uri: Uri) -> None:
        self.uri = UriHandler(uri)
        self.fs = fsspec.url_to_fs(uri)[0]

    def __repr__(self) -> str:
        return f"<Keys({self.uri})>"

    @cached_property
    def key_prefix(self) -> str:
        scheme, rest = split_uri(self.uri.uri)
        if scheme == SCHEME_FILE:
            return rest.rstrip("/")
        if scheme == SCHEME_REDIS:
            # drop the host; a numeric first path segment is the redis db
            # selector (see RedisFileSystem._get_kwargs_from_urls)
            parts = rest.strip("/").split("/")[1:]
            if parts and parts[0].isdigit():
                parts = parts[1:]
            return "/".join(parts)
        if "sql" in scheme:
            return ""
        if CAN_HTTP and isinstance(self.fs, HTTPFileSystem):
            return str(self.uri)
        # memory, s3 and other fsspec implementations want the relative path
        return rest.strip("/")

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
        if self.uri.scheme == SCHEME_MEMORY:
            key = key.lstrip("/")
        if not key.startswith(self.key_prefix):
            raise ValueError(
                f"Invalid key `{key}`, doesn't has base `{self.key_prefix}`"
            )
        key = key[len(self.key_prefix) :].strip("/")
        # http directory listings are percent-encoded hrefs
        if self.uri.scheme in ("http", "https"):
            key = unquote(key)
        return key

    def to_absolute_uri(self, key: Uri) -> str:
        return str(self.uri / key)
