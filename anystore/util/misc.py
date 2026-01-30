import mimetypes
import re
import shutil
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from os.path import splitext
from pathlib import Path
from typing import Self

from rigour.mime import normalize_mimetype
from uuid_extensions import uuid7

from anystore.types import Uri

SCHEME_FILE = "file"
SCHEME_S3 = "s3"
SCHEME_REDIS = "redis"
SCHEME_MEMORY = "memory"


def rm_rf(uri: Uri) -> None:
    """
    like `rm -rf`, ignoring errors.
    """
    try:
        p = Path(uri)
        if p.is_dir():
            shutil.rmtree(str(p), ignore_errors=True)
        else:
            p.unlink()
    except Exception:
        pass


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


def mask_uri(uri: str) -> str:
    """
    Replace username and password in a URI with asterisks
    """
    pattern = r"([a-zA-Z][a-zA-Z0-9+.-]*)://([^:]+):([^@]+)@"
    return re.sub(pattern, r"\1://***:***@", uri)


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
