"""
# Models

Pydantic model interfaces to initialize stores and handle metadata for keys.
"""

from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Self
from urllib.parse import urlparse

from pydantic import field_validator, model_validator
from rigour.mime import DEFAULT, normalize_mimetype

from anystore.mixins import BaseModel
from anystore.serialize import Mode
from anystore.settings import Settings
from anystore.types import Model, SDict, Uri
from anystore.util import (
    SCHEME_FILE,
    SCHEME_MEMORY,
    SCHEME_REDIS,
    SCHEME_S3,
    ensure_uri,
    guess_mimetype,
    join_uri,
)

settings = Settings()

if TYPE_CHECKING:
    from anystore.store.base import BaseStore


class BaseStats(BaseModel):
    """Shared base metadata object"""

    created_at: datetime | None = None
    """Created at timestamp"""

    updated_at: datetime | None = None
    """Last updated timestamp"""

    size: int
    """Size (content length) in bytes"""

    raw: SDict = {}
    """Raw data (to preserve headers)"""

    @field_validator("size", mode="before")
    @classmethod
    def ensure_size(cls, value: Any) -> Any:
        return value or 0

    @model_validator(mode="after")
    def ensure_updated_at(self) -> Self:
        self.updated_at = self.updated_at or self.created_at
        return self


class Stats(BaseStats):
    """Meta information for a store key"""

    name: str
    """Key name: last part of the key (aka file name without path)"""

    store: str
    """Store base uri"""

    key: str
    """Full path of key"""

    @property
    def uri(self) -> str:
        """
        Computed uri property. Absolute when file-like prepended with store
        schema, relative if using different store backend

        Returns:
            file-like: `file:///tmp/foo.txt`, `ssh://user@host:data.csv`
            relative path for other (redis, sql, ...): `tmp/foo.txt`
        """
        store = StoreModel(uri=self.store)
        if store.is_fslike:
            return join_uri(self.store, self.key)
        return self.key

    @property
    def mimetype(self) -> str:
        """
        Return the mimetype based on response headers or extension
        """
        if self.raw:
            mtype = self.raw.get("ContentType") or self.raw.get("mimetype")
            mtype = normalize_mimetype(mtype)
            if mtype not in (DEFAULT, "binary/octet-stream"):
                return mtype
        return guess_mimetype(self.name)


class StoreModel(BaseModel):
    """Store model to initialize a store from configuration"""

    uri: Uri
    """Store base uri"""
    serialization_mode: Mode | None = settings.serialization_mode
    """Default serialization (auto, raw, pickle, json)"""
    serialization_func: Callable | None = None
    """Default serialization function"""
    deserialization_func: Callable | None = None
    """Default deserialization function"""
    model: Model | None = None
    """Default pydantic model for serialization"""
    raise_on_nonexist: bool | None = settings.raise_on_nonexist
    """Raise `anystore.exceptions.DoesNotExist` if key doesn't exist"""
    store_none_values: bool | None = True
    """Store `None` as value in store"""
    default_ttl: int | None = settings.default_ttl
    """Default ttl for keys (only backends that support it: redis, sql, ..)"""
    backend_config: dict[str, Any] | None = None
    """Backend-specific configuration to pass through for initialization"""
    readonly: bool | None = False
    """Consider this store as a read-only store, writing will raise an exception"""

    @cached_property
    def scheme(self) -> str:
        return urlparse(str(self.uri)).scheme

    @cached_property
    def path(self) -> str:
        return urlparse(str(self.uri)).path.strip("/")

    @cached_property
    def netloc(self) -> str:
        return urlparse(str(self.uri)).netloc

    @cached_property
    def is_local(self) -> bool:
        """Check if it is a local file store"""
        return self.scheme == SCHEME_FILE

    @cached_property
    def is_fslike(self) -> bool:
        """Check if it is a file-like store usable with `fsspec`"""
        return not self.is_sql and self.scheme not in (SCHEME_REDIS, SCHEME_MEMORY)

    @cached_property
    def is_http(self) -> bool:
        """Check if it is a http(s) remote store"""
        return self.scheme.startswith("http")

    @cached_property
    def is_s3(self) -> bool:
        """Check if it is a s3 (compatible) remote store"""
        return self.scheme == SCHEME_S3

    @cached_property
    def is_sql(self) -> bool:
        """Check if it is a sql-like store (sqlite, postgres, ...)"""
        return "sql" in self.scheme

    @field_validator("uri", mode="before")
    @classmethod
    def ensure_uri(cls, v: Any) -> str:
        uri = ensure_uri(v)
        return uri.rstrip("/")

    def to_store(self) -> "BaseStore":
        from anystore.store import get_store

        return get_store(**self.model_dump())
