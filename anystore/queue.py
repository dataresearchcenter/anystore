import contextlib
from typing import Generator, Generic, TypeVar

from pydantic import BaseModel

from anystore.store import get_store
from anystore.store.base import BaseStore
from anystore.tags import Tags
from anystore.types import Model, Uri
from anystore.util import ensure_uuid

T = TypeVar("T")


class Queue(Tags, Generic[T]):
    """
    Simple queue interface to store and potentially notify distributed consumers
    across the file-like system (or any other anystore backend). This is not
    intended to be a super efficient and/or complex queue-worker implementation,
    more like an interface to store "tags" that contain some payload that
    another service can check out later. The queue is typed, given that store
    and retrieve data is expected to have that strict type.
    """

    def __init__(self, store: BaseStore, model: Model | None = None) -> None:
        super().__init__(store)
        self.model = model
        # Override the store methods assigned by Tags to use our typed versions
        del self.get
        del self.put

    def get(self, key: Uri) -> T | None:
        """
        Get a value from the queue for the given key.

        Args:
            key: Key relative to store base uri

        Returns:
            The typed value for the key, or None if not found
        """
        return self.store.get(key, model=self.model)

    def put(self, value: T) -> None:
        """
        Store a value at a uuid7 key.

        Args:
            value: The typed content to store
        """
        self.store.put(ensure_uuid(), value)

    def __call__(self, value: T) -> None:
        """Alias for put() to allow queue(value) syntax."""
        self.put(value)

    @contextlib.contextmanager
    def checkout(self) -> Generator[T | None, None, None]:
        """
        Checkout the next item in queue (no order guaranteed).

        If an exception is thrown within the context, the item is not removed
        from the queue. Yields None if the queue is empty.

        Example:
            ```python
            while True:
                with queue.checkout() as payload:
                    if payload is None:
                        break
                    do_something(payload)
        """
        try:
            key = next(self.iterate_keys())
        except StopIteration:
            yield None
            return
        try:
            yield self.get(key)
        except Exception:
            raise
        else:
            self.pop(key)

    def consume(self) -> Generator[T, None, None]:
        """
        Continuously consume items from the queue until empty.

        Each item is automatically removed after successful processing.
        If an exception occurs while processing an item, it remains in the queue.

        Example:
            ```python
            for payload in queue.consume():
                process(payload)
            ```

        Yields:
            Queue items one by one until the queue is empty
        """
        while True:
            with self.checkout() as item:
                if item is None:
                    return
                yield item


def get_queue(t: type[T], *args, **kwargs) -> Queue[T]:
    """
    Get a typed queue instance.

    Example:
        ```python
        queue = get_queue(MyPayload, uri="s3://bucket/queue")
        queue.put(MyPayload(...))
        for payload in queue.consume():
            process(payload)
        ```

    Args:
        t: The type of items stored in the queue (used as model for Pydantic types)
        *args: Arguments passed to `get_store`
        **kwargs: Keyword arguments passed to `get_store`

    Returns:
        A typed Queue instance
    """
    store = get_store(*args, **kwargs)
    model = t if isinstance(t, type) and issubclass(t, BaseModel) else None
    return Queue[t](store, model=model)


class Queues(Generic[T]):
    """
    A collection of named queues with a common type and prefix.

    Example:
        ```python
        crawl = Queues(str, "s3://bucket/crawl", ["upsert", "delete"])
        crawl.upsert("checksum123")
        crawl.delete("checksum456")

        for item in crawl.upsert.consume():
            process_upsert(item)
        ```

    Args:
        t: The type of items stored in the queues
        uri: Base URI for the queues
        actions: List of action/queue names
    """

    def __init__(self, t: type[T], uri: Uri, actions: list[str]) -> None:
        self._type = t
        self._uri = str(uri).rstrip("/")
        self._actions = set(actions)
        self._queues: dict[str, Queue[T]] = {}

    def _get_queue(self, action: str) -> Queue[T]:
        if action not in self._queues:
            queue_uri = f"{self._uri}/{action}"
            self._queues[action] = get_queue(self._type, uri=queue_uri)
        return self._queues[action]

    def __getattr__(self, name: str) -> Queue[T]:
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._actions:
            return self._get_queue(name)
        raise AttributeError(f"No queue action '{name}'")
