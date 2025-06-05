"""
Decorate functions to store results in a configurable cache and retrieve cached
results on next call.

Example:
    ```python
    from anystore import anycache

    @anycache(uri="./local-cache")
    def calculate(data):
        # a very time consuming task
        return result

    # 1. time: slow
    res = calculate(100)

    # 2. time: fast, as now cached
    res = calculate(100)
    ```

The cache key is computed based on the input arguments, but can be configured.

See below for reference details.
"""

import functools
import random
import time
from typing import Any, Callable, Type

from pydantic import BaseModel
from structlog import BoundLogger

from anystore.exceptions import DoesNotExist
from anystore.logging import get_logger
from anystore.serialize import Mode
from anystore.settings import Settings
from anystore.store import BaseStore, get_store
from anystore.util import make_signature_key

log = get_logger(__name__)
settings = Settings()


def _setup_decorator(**kwargs) -> tuple[Callable, BaseStore]:
    key_func: Callable = kwargs.pop("key_func", None) or make_signature_key
    store: BaseStore = kwargs.pop("store", None) or get_store(**kwargs)
    store = store.model_copy()
    store.default_ttl = kwargs.pop("ttl", None) or store.default_ttl
    for key, value in kwargs.items():
        if key != "uri":
            try:
                setattr(store, key, value)
            except ValueError as e:
                log.error(f"{e.__class__.__name__}: {e}")
    store.raise_on_nonexist = True
    return key_func, store


def _handle_result(key: str, res: Any, store: BaseStore) -> Any:
    if store.serialization_func is not None:
        res = store.serialization_func(res)
    if key:
        store.put(key, res, serialization_func=lambda x: x)  # already serialized
    return res


def anycache(
    func: Callable[..., Any] | None = None,
    store: BaseStore | None = None,
    model: Type[BaseModel] | None = None,
    key_func: Callable[..., str | None] | None = None,
    serialization_mode: Mode | None = "auto",
    serialization_func: Callable | None = None,
    deserialization_func: Callable | None = None,
    ttl: int | None = None,
    use_cache: bool | None = settings.use_cache,
    **store_kwargs: Any,
) -> Callable[..., Any]:
    """
    Cache a function call in a configurable cache backend. By default, the
    default store is used (configured via environment)

    Example:
        ```python
        @anycache(
            store=get_store("redis://localhost"),
            key_func=lambda *args, **kwargs: args[0].upper()
        )
        def compute(*args, **kwargs):
            return "result"
        ```

    Note:
        If the `key_func` returns `None` as the computed cache key, the result
        will not be cached (this can be used to dynamically disable caching
        based on function input)

    See [`anystore.serialize`][anystore.serialize] for serialization reference.

    Args:
        func: The function to wrap
        serialization_mode: "auto", "pickle", "json", "raw"
        serialization_func: Function to use to serialize
        deserialization_func: Function to use to deserialize, takes bytes as input
        model: Pydantic model to use for serialization from a json bytes string
        key_func: Function to compute the cache key
        ttl: Key ttl for supported backends
        use_cache: Lookup cache (default), results are always stored
        **store_kwargs: Any other store options or backend specific
            configuration to pass through

    Returns:
        Callable: The decorated function
    """
    key_func, store = _setup_decorator(
        store=store,
        key_func=key_func,
        serialization_mode=serialization_mode,
        serialization_func=serialization_func,
        deserialization_func=deserialization_func,
        model=model,
        ttl=ttl,
        **store_kwargs,
    )

    def _decorator(func):
        @functools.wraps(func)
        def _inner(*args, **kwargs):
            key = key_func(*args, **kwargs)
            try:
                if key is not None and use_cache:
                    return store.get(key)
                raise DoesNotExist
            except DoesNotExist:
                res = func(*args, **kwargs)
                return _handle_result(key, res, store)

        return _inner

    if func is None:
        return _decorator
    return _decorator(func)


def async_anycache(func=None, **kwargs):
    """
    Async implementation of the [@anycache][anystore.decorators.anycache]
    decorator
    """
    key_func, store = _setup_decorator(**kwargs)

    def _decorator(func):
        @functools.wraps(func)
        async def _inner(*args, **kwargs):
            key = key_func(*args, **kwargs)
            try:
                if key is not None and kwargs.get("use_cache"):
                    return store.get(key)
                raise DoesNotExist
            except DoesNotExist:
                res = await func(*args, **kwargs)
                return _handle_result(key, res, store)

        return _inner

    if func is None:
        return _decorator
    return _decorator(func)


def _error_handler(
    func: Callable[..., Any],
    logger: BoundLogger | None = None,
    max_retries: int | None = 1,
    backoff_factor: int | None = 2,
    backoff_random: bool | None = True,
    *args,
    **kwargs,
) -> Any:
    _log = logger or log
    retry = 0
    retries = max_retries or 1
    exc: Exception | None = None
    while retry < retries:
        retry += 1
        try:
            return func(*args, **kwargs)
        except Exception as e:
            backoff = retry
            if backoff_random:
                backoff = backoff + random.random()
            backoff = backoff ** (backoff_factor or 2)
            exc = e
            _log.error(
                f"{e.__class__.__name__}: {e}",
                args=args,
                retry=retry,
                max_retries=retries,
                backoff=backoff,
                **kwargs,
            )
            if retry <= retries:
                time.sleep(backoff)

    if exc is not None:
        _log.error(
            f"{exc.__class__.__name__}: {exc} [MAX RETRIES REACHED]",
            args=args,
            retry=retry,
            **kwargs,
        )
        if settings.debug:
            raise exc


def error_handler(
    func: Callable[..., Any] | None = None,
    logger: BoundLogger | None = None,
    max_retries: int | None = 1,
    backoff_factor: int | None = 2,
    backoff_random: bool | None = True,
) -> Callable[..., Any]:
    """
    Wrap any execution into an error handler that catches Exceptions and
    optional retries. If in debug mode (env var `DEBUG=1`) it will raise the
    exception, otherwise log the error after `max_retries`.

    Example:
        ```python
        @error_handler(max_retries=3)
        def compute(*args, **kwargs):
            return "result"
        ```

    Args:
        func: The function to wrap
        logger: An optional `BoundLogger` instance to use as the error logger
        max_retries: Maximum retries
        backoff_factor: Increase backoff seconds by this power
        backoff_random: Calculate a bit of randomness for backoff seconds

    Returns:
        Callable: The decorated function
    """

    def _decorator(func):
        @functools.wraps(func)
        def _inner(*args, **kwargs):
            return _error_handler(
                func,
                logger,
                max_retries,
                backoff_factor,
                backoff_random,
                *args,
                **kwargs,
            )

        return _inner

    if func is None:
        return _decorator
    return _decorator(func)


def async_error_handler(
    func: Callable[..., Any] | None = None,
    logger: BoundLogger | None = None,
    max_retries: int | None = 1,
    backoff_factor: int | None = 2,
    backoff_random: bool | None = True,
) -> Callable[..., Any]:
    """
    Async implementation of the
    [@error_handler][anystore.decorators.error_handler] decorator
    """

    def _decorator(func):
        @functools.wraps(func)
        async def _inner(*args, **kwargs):
            return await _error_handler(
                func,
                logger,
                max_retries,
                backoff_factor,
                backoff_random,
                *args,
                **kwargs,
            )

        return _inner

    if func is None:
        return _decorator
    return _decorator(func)
