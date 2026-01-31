from datetime import datetime, timezone
from typing import Any, Self

from pydantic import AliasChoices, ConfigDict, Field, field_validator, model_validator
from rigour.mime import DEFAULT, normalize_mimetype

from anystore.model.base import BaseModel
from anystore.settings import Settings
from anystore.util.misc import guess_mimetype

settings = Settings()


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
