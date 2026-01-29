from anystore.functools import weakref_cache as cache
from anystore.interface.lock import Lock
from anystore.interface.queue import Queue
from anystore.interface.tags import Tags
from anystore.store import get_store
from anystore.types import Model, Uri


@cache
def get_tags(uri: Uri) -> Tags:
    store = get_store(uri)
    return Tags(store)


@cache
def get_lock(uri: Uri) -> Lock:
    store = get_store(uri)
    return Lock(store)


@cache
def get_queue(uri: Uri, model: Model | None = None) -> Queue:
    store = get_store(uri)
    return Queue(store, model)
