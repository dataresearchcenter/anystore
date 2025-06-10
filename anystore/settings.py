from multiprocessing import cpu_count
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings as _BaseSettings
from pydantic_settings import SettingsConfigDict

from anystore.serialize import Mode


class BaseSettings(_BaseSettings):
    """
    Base settings that are not app-specific and can be subclassed in other
    applications
    """

    debug: bool = Field(alias="debug", default=False)
    """Enable debug mode"""

    redis_debug: bool = Field(alias="redis_debug", default=False)
    """Use fakeredis when using redis backend"""

    log_json: bool = Field(alias="log_json", default=False)
    """Enable json log format"""

    log_level: str = Field(alias="log_level", default="info")
    """Log level (debug, info, warning, error)"""

    worker_threads: int = Field(alias="worker_threads", default=cpu_count())
    """Default number of threads to use for workers"""

    worker_heartbeat: int = Field(alias="worker_heartbeat", default=15)
    """Default heartbeat for worker logging"""

    use_cache: bool = Field(alias="cache", default=True)
    """Globally enable or disable caching (used in @anycache decorator)"""


class Settings(BaseSettings):
    """
    `anystore` settings management using
    [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)

    Note:
        All settings can be set via environment variables in uppercase,
        prepending `ANYSTORE_` (except for those with a given prefix)

        Backend config: Use `__` as a separator for dictionary content, e.g.:
        `ANYSTORE_BACKEND_CONFIG__REDIS_PREFIX="foo"`
    """

    model_config = SettingsConfigDict(
        env_prefix="anystore_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    uri: str = ".anystore"
    """Default store base uri"""

    yaml_uri: str | None = None
    """Load a (remote) store configuration (yaml) from this uri"""

    json_uri: str | None = None
    """Load a (remote) store configuration (json) from this uri"""

    serialization_mode: Mode = "auto"
    """Default serialization mode, one of ("auto", "pickle", "json", "raw")"""

    raise_on_nonexist: bool = True
    """Silence errors for non-existing keys"""

    default_ttl: int = 0
    """Key ttl for backends that support it (e.g. redis, sql)"""

    backend_config: dict[str, Any] = {}
    """Arbitrary backend config to pass through"""
