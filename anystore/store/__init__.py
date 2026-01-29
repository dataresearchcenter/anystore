"""
# Top-level store entrypoint
"""

from typing import Any

from anystore.logging import get_logger
from anystore.settings import Settings
from anystore.store.base import Store
from anystore.types import Uri
from anystore.util import ensure_uri

log = get_logger(__name__)


def get_store(
    uri: Uri | None = None, settings: Settings | None = None, **kwargs: Any
) -> Store:
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
        A `Store` instance
    """
    settings = settings or Settings()
    kwargs = {**{"backend_config": settings.backend_config}, **kwargs}
    if uri is None:
        if settings.yaml_uri is not None:
            return Store.from_yaml_uri(settings.yaml_uri, **kwargs)
        if settings.json_uri is not None:
            return Store.from_json_uri(settings.json_uri, **kwargs)
        uri = settings.uri
    uri = ensure_uri(uri)
    store = Store(uri=uri, **kwargs)
    # test if backend fs is available, raises ImportError if not
    _ = store._fs
    return store
