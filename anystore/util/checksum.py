import hashlib
from io import BytesIO
from typing import Any, BinaryIO
from urllib.parse import urlparse

from banal.cache import bytes_iter

from anystore.logic.constants import CHUNK_SIZE
from anystore.logic.uri import join_relpaths
from anystore.types import Uri

DEFAULT_HASH_ALGORITHM = "sha256"


def make_checksum(io: BinaryIO, algorithm: str = DEFAULT_HASH_ALGORITHM) -> str:
    """
    Calculate checksum for bytes input for given algorithm

    Example:
        This can be used for file handlers:

        ```python
        with open("data.pdf") as fh:
            return make_checksum(fh, algorithm="md5")
        ```

    Note:
        See [`make_data_checksum`][anystore.util.checksum.make_data_checksum] for easier
        implementation for arbitrary input data.

    Args:
        io: File-like open handler
        algorithm: Algorithm from `hashlib` to use, default: sha1

    Returns:
        Generated checksum
    """
    hash_ = getattr(hashlib, algorithm)()
    for chunk in iter(lambda: io.read(CHUNK_SIZE), b""):
        hash_.update(chunk)
    return hash_.hexdigest()


def make_data_checksum(data: Any, algorithm: str = DEFAULT_HASH_ALGORITHM) -> str:
    """
    Calculate checksum for input data based on given algorithm

    Examples:
        >>> make_data_checksum({"foo": "bar"})
        "8f3536a88e3405de70ca2524cfd962203db9a84a"

    Args:
        data: Arbitrary input object
        algorithm: Algorithm from `hashlib` to use, default: sha1

    Returns:
        Generated checksum
    """
    if isinstance(data, bytes):
        return make_checksum(BytesIO(data), algorithm)
    if isinstance(data, str):
        return make_checksum(BytesIO(data.encode()), algorithm)
    data = b"".join(bytes_iter(data))
    return make_checksum(BytesIO(data), algorithm)


def make_signature_key(
    *args: Any, algorithm: str = DEFAULT_HASH_ALGORITHM, **kwargs: Any
) -> str:
    """
    Calculate data checksum for arbitrary input (used for caching function
    calls)

    Examples:
        >>> make_signature_key(1, "foo", bar="baz")
        "c6b22da6bcf4bf7158ba600594cae404648acd41"

    Args:
        *args: Arbitrary input arguments
        algorithm: Algorithm from `hashlib` to use, default: sha1
        **kwargs: Arbitrary input keyword arguments

    Returns:
        Generated checksum
    """
    return make_data_checksum((args, kwargs), algorithm)


def make_uri_key(uri: Uri, algorithm: str = DEFAULT_HASH_ALGORITHM) -> str:
    """
    Make a verbose key usable for caching. It strips the scheme, uses host and
    path as key parts and creates a checksum for the uri (including fragments,
    params, etc.). This is useful for invalidating a cache store partially by
    deleting keys by given host or path prefixes.

    Examples:
        >>> make_uri_key("https://example.org/foo/bar#fragment?a=b&c")
        "example.org/foo/bar/ecdb319854a7b223d72e819949ed37328fe034a0"

    Args:
        uri: Input URI
        algorithm: Algorithm from `hashlib` to use, default: sha1
    """
    uri = str(uri)
    parsed = urlparse(uri)
    return join_relpaths(parsed.netloc, parsed.path, make_data_checksum(uri, algorithm))
