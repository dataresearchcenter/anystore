import contextlib
import tempfile
from typing import Generator

from anystore.logic.virtual import VirtualIO
from anystore.store import Store, get_store
from anystore.util.misc import rm_rf

__all__ = ["VirtualIO", "get_virtual_store"]


@contextlib.contextmanager
def get_virtual_store(prefix: str = "anystore-") -> Generator[Store, None, None]:
    """
    Get a temporary store at local filesystem tmp dir, cleaned up after
    leaving the context

    Args:
        prefix: Custom name prefix for the tmpdir

    Yields:
        Store instance
    """
    tmp = tempfile.mkdtemp(prefix=prefix)
    store = get_store(tmp)
    try:
        yield store
    finally:
        rm_rf(store.uri)
