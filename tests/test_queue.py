from pydantic import BaseModel

from anystore.queue import Queue, Queues, get_queue
from anystore.store import get_store


class Task(BaseModel):
    name: str
    priority: int = 0


def test_queue_typed_put_get():
    store = get_store(uri="memory://")
    queue: Queue[Task] = Queue(store)

    queue.put(Task(name="task1", priority=1))

    # get the first key and retrieve it
    key = next(queue.iterate_keys())
    retrieved = queue.get(key)
    assert retrieved is not None
    assert retrieved["name"] == "task1"
    assert retrieved["priority"] == 1


def test_queue_checkout():
    store = get_store(uri="memory://")
    queue: Queue[Task] = Queue(store)

    queue.put(Task(name="task1"))
    queue.put(Task(name="task2"))

    with queue.checkout() as payload:
        assert payload is not None
        assert payload["name"] in ("task1", "task2")

    # one item should be removed after successful checkout
    keys = list(queue.iterate_keys())
    assert len(keys) == 1


def test_queue_checkout_rollback_on_error():
    store = get_store(uri="memory://")
    queue: Queue[Task] = Queue(store)

    queue.put(Task(name="task1"))

    try:
        with queue.checkout() as payload:
            assert payload is not None
            raise ValueError("simulated error")
    except ValueError:
        pass

    # task should still exist after failed checkout
    keys = list(queue.iterate_keys())
    assert len(keys) == 1


def test_queue_consume():
    store = get_store(uri="memory://")
    queue: Queue[Task] = Queue(store)

    queue.put(Task(name="task1", priority=1))
    queue.put(Task(name="task2", priority=2))
    queue.put(Task(name="task3", priority=3))

    processed = []
    for item in queue.consume():
        processed.append(item["name"])

    assert len(processed) == 3
    assert set(processed) == {"task1", "task2", "task3"}

    # queue should be empty now
    assert list(queue.iterate_keys()) == []


def test_queue_consume_empty():
    store = get_store(uri="memory://")
    queue: Queue[Task] = Queue(store)

    # consuming an empty queue should just return immediately
    items = list(queue.consume())
    assert items == []


def test_get_queue_factory():
    queue = get_queue(Task, uri="memory://")

    queue.put(Task(name="factory_test"))

    for item in queue.consume():
        # With get_queue, Pydantic models are properly deserialized
        assert isinstance(item, Task)
        assert item.name == "factory_test"


def test_queues_collection():
    crawl = Queues(str, "memory://crawl", ["upsert", "delete"])

    # Put using callable syntax
    crawl.upsert("checksum1")
    crawl.upsert("checksum2")
    crawl.delete("checksum3")

    # Consume from upsert queue
    upserts = list(crawl.upsert.consume())
    assert len(upserts) == 2
    assert set(upserts) == {"checksum1", "checksum2"}

    # Consume from delete queue
    deletes = list(crawl.delete.consume())
    assert len(deletes) == 1
    assert deletes[0] == "checksum3"


def test_queues_with_pydantic_model():
    queues = Queues(Task, "memory://tasks", ["pending", "completed"])

    queues.pending(Task(name="task1"))
    queues.completed(Task(name="task2"))

    for item in queues.pending.consume():
        assert isinstance(item, Task)
        assert item.name == "task1"

    for item in queues.completed.consume():
        assert isinstance(item, Task)
        assert item.name == "task2"
