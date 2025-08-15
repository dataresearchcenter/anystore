import functools
import weakref
from typing import Any, Callable, Dict, ParamSpec, Protocol, Tuple, TypeVar, cast

P = ParamSpec("P")
R = TypeVar("R", covariant=True)


class CachedCallable(Protocol[P, R]):
    """Protocol for a cached callable with cache management methods."""

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R: ...  # noqa: E704
    def cache_info(self) -> functools._CacheInfo: ...  # noqa: E704
    def cache_clear(self) -> None: ...  # noqa: E704


def weakref_cache(func: Callable[P, R]) -> CachedCallable[P, R]:
    """
    A decorator that caches function results using weak references.

    Similar to functools.cache, but stores weak references to cached objects,
    allowing them to be garbage collected when no other references exist.
    """
    cache: Dict[Tuple, Any] = {}
    hits = misses = 0

    def cleanup_callback(key: Tuple):
        """Callback to remove cache entry when weakref is garbage collected."""
        cache.pop(key, None)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        nonlocal hits, misses

        # Create cache key from arguments
        key = _make_key(args, kwargs)

        # Check if we have a cached result
        if key in cache:
            cached_ref = cache[key]
            if isinstance(cached_ref, weakref.ref):
                # Try to get the object from the weak reference
                cached_obj = cached_ref()
                if cached_obj is not None:
                    hits += 1
                    return cached_obj
                else:
                    # Object was garbage collected, remove from cache
                    del cache[key]
            else:
                # Non-weakref-able object (like primitives), return directly
                hits += 1
                return cached_ref

        # Cache miss - compute the result
        misses += 1
        result = func(*args, **kwargs)

        # Try to store as weak reference
        try:
            # Create weak reference with cleanup callback
            weak_result = weakref.ref(result, lambda ref: cleanup_callback(key))
            cache[key] = weak_result
        except TypeError:
            # Object doesn't support weak references (e.g., int, str, tuple)
            # Store directly
            cache[key] = result

        return result

    def cache_info():
        """Return cache statistics."""
        maxsize = None  # Unlimited like functools.cache

        # Count valid weak references for current size
        currsize = 0
        for value in cache.values():
            if isinstance(value, weakref.ref):
                if value() is not None:
                    currsize += 1
            else:
                currsize += 1

        return functools._CacheInfo(hits, misses, maxsize, currsize)

    def cache_clear():
        """Clear the cache."""
        nonlocal hits, misses
        cache.clear()
        hits = misses = 0

    # Add cache management methods
    wrapper.cache_info = cache_info
    wrapper.cache_clear = cache_clear

    return cast(CachedCallable[P, R], wrapper)


def _make_key(args, kwargs):
    """Create a hashable key from function arguments."""
    key = args
    if kwargs:
        # Sort kwargs for consistent key generation
        key += tuple(sorted(kwargs.items()))
    return key
