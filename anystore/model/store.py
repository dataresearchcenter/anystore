from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import urlparse

from pydantic import field_validator

from anystore.logic.serialize import Mode
from anystore.model.base import BaseModel
from anystore.settings import Settings
from anystore.types import Model, Uri
from anystore.util import (
    SCHEME_FILE,
    SCHEME_MEMORY,
    SCHEME_REDIS,
    SCHEME_S3,
    ensure_uri,
)

if TYPE_CHECKING:
    from anystore.store import Store


settings = Settings()


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
    backend_config: dict[str, Any] = {}
    """Backend-specific configuration to pass through for initialization"""

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
        return ensure_uri(v)

    def to_store(self, **kwargs) -> "Store":
        from anystore.store import get_store

        return get_store(**{**self.model_dump(), **kwargs})
