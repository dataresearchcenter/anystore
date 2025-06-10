"""
# Top-level store entrypoint
"""

import os
from typing import Any
from urllib.parse import urlparse

from anystore.logging import get_logger
from anystore.settings import Settings
from anystore.store.base import BaseStore
from anystore.store.fs import Store
from anystore.store.memory import MemoryStore
from anystore.store.zip import ZipStore
from anystore.types import Uri
from anystore.util import ensure_uri

log = get_logger(__name__)


def get_store(
    uri: Uri | None = None, settings: Settings | None = None, **kwargs: Any
) -> BaseStore:
    """
    Short-hand initializer for a new store. The call is cached during runtime if
    input doesn't change.

    Example:
        ```python
        from anystore import get_store

        # initialize from current configuration
        store = get_store()
        # get a redis store with custom prefix
        store = get_store("redis://localhost", backend_config={"redis_prefix": "foo"})
        ```

    Args:
        uri: Store base uri, if relative it is considered as a local file store,
             otherwise the store backend is inferred from the scheme. If omitted,
             store is derived from settings defaults (taking current environment
             into account).
        **kwargs: pass through storage-specific options

    Returns:
        A `Store` class
    """
    settings = settings or Settings()
    kwargs = {**{"backend_config": settings.backend_config}, **kwargs}
    if uri is None:
        if settings.yaml_uri is not None:
            store = BaseStore.from_yaml_uri(settings.yaml_uri, **kwargs)
            return get_store(**store.model_dump())
        if settings.json_uri is not None:
            store = BaseStore.from_json_uri(settings.json_uri, **kwargs)
            return get_store(**store.model_dump())
        uri = settings.uri
    uri = ensure_uri(uri)
    parsed = urlparse(uri)
    if parsed.scheme == "memory":
        return MemoryStore(uri=uri, **kwargs)
    if parsed.scheme == "redis":
        try:
            from anystore.store.redis import RedisStore

            return RedisStore(uri=uri, **kwargs)
        except ImportError as e:
            log.error("Install redis dependencies via `anystore[redis]`")
            raise ImportError(e)
    if "sql" in parsed.scheme:
        try:
            from anystore.store.sql import SqlStore

            return SqlStore(uri=uri, **kwargs)
        except ImportError as e:
            log.error("Install sql dependencies via `anystore[sql]`")
            raise ImportError(e)
    if "zip" in os.path.splitext(uri)[1]:
        return ZipStore(uri=uri, **kwargs)
    return Store(uri=uri, **kwargs)


def get_store_for_uri(uri: Uri, **kwargs) -> tuple[BaseStore, str]:
    parsed = urlparse(ensure_uri(uri))
    if parsed.scheme in ("redis", "memory") or "sql" in parsed.scheme:
        raise NotImplementedError(f"Cannot parse `{uri}` with scheme `{parsed.scheme}`")
    base_uri, path = str(uri).rsplit("/", 1)
    return get_store(base_uri, **kwargs), path


__all__ = ["get_store", "Store", "MemoryStore"]
