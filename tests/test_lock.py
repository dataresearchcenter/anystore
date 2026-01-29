import pytest

from anystore.interface.lock import Lock
from anystore.store import get_store


def test_lock():
    store = get_store("memory://")
    lock = Lock(store)

    tested = False
    with lock:
        tested = True
    assert tested

    # lock externally
    store.touch(".LOCK")
    lock = Lock(store, max_retries=1)

    with pytest.raises(RuntimeError):
        with lock:
            tested = True
