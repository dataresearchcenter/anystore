"""
# Models

Pydantic model interfaces to initialize stores and handle metadata for keys.
"""

from datetime import datetime, timezone
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Self
from urllib.parse import urlparse

from pydantic import AliasChoices, ConfigDict, Field, field_validator, model_validator
from rigour.mime import DEFAULT, normalize_mimetype

from anystore.logic.serialize import Mode
from anystore.mixins import BaseModel
from anystore.settings import Settings
from anystore.types import Model, Uri
from anystore.util import (
    SCHEME_FILE,
    SCHEME_MEMORY,
    SCHEME_REDIS,
    SCHEME_S3,
    ensure_uri,
    guess_mimetype,
)

settings = Settings()

if TYPE_CHECKING:
    from anystore.store import Store


CREATED_AT_CHOICES = (
    "created_at",
    "created",
)

UPDATED_AT_CHOICES = (
    "updated_at",
    "mtime",
    "Last-Modified",
    "LastModified",
)


def _ensure_datetime(val: Any) -> datetime | None:
    """Coerce int/float timestamps and date strings to datetime."""
    if val is None:
        return None
    if isinstance(val, datetime):
        if val.tzinfo is None:
            val = val.replace(tzinfo=timezone.utc)
        return val
    if isinstance(val, (int, float)):
        return datetime.fromtimestamp(val, tz=timezone.utc)
    if isinstance(val, str):
        from dateutil.parser import parse as parse_date

        try:
            return parse_date(val)
        except (ValueError, TypeError):
            return None
    return None


class Info(BaseModel):
    """Streamline fs.info()"""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    name: str
    """Key name: last part of the key (aka file name without path)"""

    created_at: datetime | None = Field(
        default=None,
        validation_alias=AliasChoices(*CREATED_AT_CHOICES),
    )
    """Created at timestamp"""

    updated_at: datetime | None = Field(
        default=None,
        validation_alias=AliasChoices(*UPDATED_AT_CHOICES),
    )
    """Last updated timestamp"""

    size: int
    """Size (content length) in bytes"""

    @field_validator("created_at", "updated_at", mode="before")
    @classmethod
    def coerce_timestamp(cls, v: Any) -> datetime | None:
        return _ensure_datetime(v)

    @model_validator(mode="after")
    def ensure_timestamp_fallback(self) -> Self:
        """Fall back created_at <-> updated_at."""
        if self.created_at and not self.updated_at:
            self.updated_at = self.created_at
        elif self.updated_at and not self.created_at:
            self.created_at = self.updated_at
        return self


class Stats(Info):
    """Meta information for a store key"""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    store: str
    """Store base uri"""

    key: str
    """Full path of key"""

    mimetype: str
    """Mime type for that key"""

    @model_validator(mode="before")
    @classmethod
    def ensure_mimetype(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Extract mimetype from fsspec info dict headers."""
        if not values.get("mimetype"):
            lower = {k.lower(): v for k, v in values.items()}
            mtype = lower.get("contenttype") or lower.get("mimetype")
            if mtype:
                mtype = normalize_mimetype(mtype)
                if mtype not in (DEFAULT, "binary/octet-stream"):
                    values["mimetype"] = mtype
        if not values.get("mimetype"):
            values["mimetype"] = guess_mimetype(values.get("name", ""))
        return values

    @property
    def uri(self) -> str:
        """
        Computed uri property from store uri and key.

        Returns:
            e.g. `file:///tmp/foo.txt`, `ssh://user@host:data.csv`,
                `sqlite:////tmp/db.sqlite/foo/bar`
        """
        return f"{self.store}/{self.key}"


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
