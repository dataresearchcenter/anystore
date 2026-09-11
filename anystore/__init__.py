from anystore.decorators import anycache, async_anycache
from anystore.io import smart_open, smart_read, smart_stream, smart_write
from anystore.logic.compress import CompressKind
from anystore.store import get_store

__all__ = [
    "CompressKind",
    "get_store",
    "anycache",
    "async_anycache",
    "smart_open",
    "smart_read",
    "smart_write",
    "smart_stream",
]


__version__ = "1.4.1"
