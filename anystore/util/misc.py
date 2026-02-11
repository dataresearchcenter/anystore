import mimetypes
import re
import shutil
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from os.path import splitext
from typing import Self

from rigour.mime import normalize_mimetype
from uuid_extensions import uuid7

from anystore.logging import get_logger
from anystore.logic.uri import ensure_uri, uri_to_path
from anystore.types import Uri

log = get_logger(__name__)


def rm_rf(uri: Uri) -> None:
    """
    like `rm -rf`, ignoring errors.
    """
    uri = ensure_uri(uri)
    if not uri.startswith("file"):
        raise ValueError(f"Uri not local: `{uri}`")
    try:
        p = uri_to_path(uri)
        if p.is_dir():
            shutil.rmtree(str(p), ignore_errors=True)
        else:
            p.unlink()
    except Exception as e:
        log.warn(f"Couldn't delete file or folder: `{e}`", uri=uri)


def get_extension(uri: Uri) -> str | None:
    """
    Extract file extension from given uri.

    Examples:
        >>> get_extension("foo/bar.txt")
        "txt"
        >>> get_extension("foo/bar")
        None

    Args:
        uri: Full path-like uri

    Returns:
        Extension or `None`
    """
    if isinstance(uri, (BytesIO, StringIO)):
        return None
    _, ext = splitext(str(uri))
    if ext:
        return ext[1:].lower()


def guess_mimetype(key: Uri) -> str:
    """
    Guess the mimetype based on a file extension and normalize it via
    `rigour.mime`
    """
    mtype, _ = mimetypes.guess_type(str(key))
    return normalize_mimetype(mtype)


def mask_uri(uri: Uri) -> str:
    """
    Replace username and password in a URI with asterisks
    """
    pattern = r"([a-zA-Z][a-zA-Z0-9+.-]*)://([^:]+):([^@]+)@"
    return re.sub(pattern, r"\1://***:***@", str(uri))


def ensure_uuid(uuid: str | None = None) -> str:
    """Ensure uuid or create one"""
    if uuid:
        return str(uuid)
    return str(uuid7())


class Took:
    """
    Shorthand to measure time of a code block

    Examples:
        ```python
        from anystore.util import Took

        with Took() as t:
            # do something
            log.info(f"Job took:", t.took)
        ```
    """

    def __init__(self) -> None:
        self.start = datetime.now()

    @property
    def took(self) -> timedelta:
        return datetime.now() - self.start

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args, **kwargs):
        pass
