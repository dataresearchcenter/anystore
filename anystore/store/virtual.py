import contextlib
import tempfile
from pathlib import Path
from typing import IO, Generator

from anystore.io import DEFAULT_MODE
from anystore.model import Stats
from anystore.store import get_store, get_store_for_uri
from anystore.store.base import BaseStore
from anystore.types import Uri
from anystore.util import DEFAULT_HASH_ALGORITHM, make_checksum, rm_rf, uri_to_path


class VirtualStore:
    """
    Temporary file storage for local processing
    """

    def __init__(self, prefix: str | None = None, keep: bool | None = False) -> None:
        self.path = tempfile.mkdtemp(prefix=(prefix or "anystore-"))
        self.store = get_store(uri=self.path, serialization_mode="raw")
        self.keep = keep

    def download(self, uri: Uri, store: BaseStore | None = None, **kwargs) -> str:
        if store is None:
            store, uri = get_store_for_uri(uri, serialization_mode="raw")
        if store.is_local:  # omit download
            return store.get_key(uri)
        with store.open(uri, mode=kwargs.pop("mode", "rb"), **kwargs) as i:
            with self.store.open(uri, mode="wb") as o:
                o.write(i.read())
        return str(uri)

    def cleanup(self, path: Uri | None = None) -> None:
        if path is not None:
            path = Path(self.path) / path
            rm_rf(path)
        else:
            rm_rf(self.path)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if not self.keep:
            self.cleanup()


def get_virtual(prefix: str | None = None, keep: bool | None = False) -> VirtualStore:
    return VirtualStore(prefix, keep=keep)


class VirtualIO(IO):
    checksum: str | None
    path: Path
    info: Stats


@contextlib.contextmanager
def open_virtual(
    uri: Uri,
    store: BaseStore | None = None,
    tmp_prefix: str | None = None,
    keep: bool | None = False,
    checksum: str | None = DEFAULT_HASH_ALGORITHM,
    **kwargs,
) -> Generator[VirtualIO, None, None]:
    """
    Download a file for temporary local processing and get its checksum and an
    open handler. If the file itself is already on the local filesystem, the
    actual file will be used. The file is cleaned up when leaving the context,
    unless `keep=True` is given (except if it was a local file, it won't be
    deleted in any case)

    Example:
        ```python
        with open_virtual("http://example.org/test.txt") as fh:
            smart_write(uri=f"./local/{fh.checksum}", fh.read())

        # without checksum computation:
        with open_virtual("http://example.org/test.txt", checksum=None, keep=True) as fh:
            print(fh.read())

        # still exists after leaving context
        assert fh.path.exists()
        ```

    Args:
        uri: Any uri fsspec can handle
        store: An initialized store to fetch the uri from
        tmp_prefix: Set a manual temporary prefix, otherwise random
        keep: Don't delete the file after leaving context, default `False`
        checksum: Algorithm from `hashlib` to use, default: sha1. Explicitly set
            to `None` to not compute a checksum at all.
        **kwargs: pass through storage-specific options

    Yields:
        A generic file-handler like context object. It has 3 extra attributes:
        - `checksum` (if computed)
        - the absolute temporary `path` as a `pathlib.Path` object
        - [`info`][anystore.model.Stats] object

    """
    mode = kwargs.get("mode", DEFAULT_MODE)
    if store is None:
        store, uri = get_store_for_uri(uri)
    info = store.info(uri)
    if store.is_local:
        tmp = None
        open = store.open
        path = uri_to_path(store.get_key(uri))
    else:
        tmp = get_virtual(tmp_prefix, keep)
        uri = tmp.download(uri, store, **kwargs)
        open = tmp.store.open
        path = uri_to_path(tmp.store.get_key(uri))
    try:
        with open(uri, mode=mode) as handler:
            if checksum:
                checksum = make_checksum(handler, checksum)
            else:
                checksum = None
            handler.seek(0)
            handler.checksum = checksum
            handler.path = path
            handler.info = info
            yield handler
    finally:
        if not keep and tmp is not None:
            tmp.cleanup()


@contextlib.contextmanager
def get_virtual_path(
    uri: Uri,
    store: BaseStore | None = None,
    tmp_prefix: str | None = None,
    keep: bool | None = False,
    **kwargs,
) -> Generator[Path, None, None]:
    """
    Download a file for temporary local processing and get its local path.  If
    the file itself is already on the local filesystem, the actual file will be
    used. The file is cleaned up when leaving the context, unless `keep=True` is
    given (except if it was a local file, it won't be deleted in any case)

    Example:
        ```python
        with get_virtual_path("http://example.org/test.txt") as path:
            do_something(path)
        ```

    Args:
        uri: Any uri fsspec can handle
        store: An initialized store to fetch the uri from
        tmp_prefix: Set a manual temporary prefix, otherwise random
        keep: Don't delete the file after leaving context, default `False`
        **kwargs: pass through storage-specific options

    Yields:
        The absolute temporary `path` as a `pathlib.Path` object
    """
    if store is None:
        store, uri = get_store_for_uri(uri)
    if store.is_local:
        tmp = None
        path = uri_to_path(store.get_key(uri))
    else:
        tmp = get_virtual(tmp_prefix, keep)
        uri = tmp.download(uri, store, **kwargs)
        path = uri_to_path(tmp.store.get_key(uri))
    try:
        yield path
    finally:
        if not keep and tmp is not None:
            tmp.cleanup()
