from anystore.decorators import error_handler
from anystore.store.base import BaseStore


class Lock:
    """
    A global shared locking mechanism based on a store (e.g. redis, or file-like
    s3)

    Example:
        ```python
        from anystore.lock import Lock
        from anystore.store import get_store

        store = get_store("redis://localhost")
        lock = Lock(store)

        with lock:
            # do something, others have to wait
            # lock will be released when leaving the context
            return
        ```
    """

    def __init__(
        self,
        store: BaseStore,
        key: str | None = ".LOCK",
        max_retries: float | None = float("inf"),
    ) -> None:
        self.store = store
        self.key = key or ".LOCK"
        self.max_retries = max_retries
        self.exc = RuntimeError(f"Already locked: `{self.key}`")
        self.error = False
        self.acquire = error_handler(
            max_retries=self.max_retries,
            backoff_factor=1,
            do_raise=False,
        )(self._acquire)

    def _acquire(self) -> bool:
        if self.store.exists(self.key):
            raise self.exc
        return True

    def __enter__(self) -> None:
        if self.acquire():
            self.store.touch(self.key)
        else:
            self.error = True
            raise self.exc

    def __exit__(self, *args, **kwargs):
        if not self.error:
            try:
                self.store.delete(self.key)
            except Exception:
                pass  # FIXME
