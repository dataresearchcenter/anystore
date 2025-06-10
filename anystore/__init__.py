from anystore.decorators import anycache, async_anycache
from anystore.io import smart_open, smart_read, smart_stream, smart_write
from anystore.store import get_store

__all__ = [
    "get_store",
    "anycache",
    "async_anycache",
    "smart_open",
    "smart_read",
    "smart_write",
    "smart_stream",
]


__version__ = "0.3.6"
