from functools import cached_property
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any
from urllib.parse import ParseResult, unquote, urlparse

import fsspec

from anystore.logic.constants import SCHEME_FILE, SCHEME_MEMORY, SCHEME_REDIS, SCHEME_S3
from anystore.types import Uri

CURRENT = "."


def ensure_uri(uri: Any, http_unquote: bool | None = True) -> str:
    """
    Normalize arbitrary uri-like input to an absolute uri with scheme.

    Example:
        ```python
        assert util.ensure_uri("https://example.com") == "https://example.com"
        assert util.ensure_uri("s3://example.com") == "s3://example.com"
        assert util.ensure_uri("foo://example.com") == "foo://example.com"
        assert util.ensure_uri("-") == "-"
        assert util.ensure_uri("./foo").startswith("file:///")
        assert util.ensure_uri(Path("./foo")).startswith("file:///")
        assert util.ensure_uri("/foo") == "file:///foo"
        ```

    Args:
        uri: uri-like string
        http_unquote: Return unquoted uri, manually disable for some http edge cases

    Returns:
        Absolute uri with scheme

    Raises:
        ValueError: For invalid uri (e.g. stdin: "-")
    """
    if isinstance(uri, (BytesIO, StringIO)):
        raise ValueError(f"Invalid uri: `{uri}`")
    if not uri:
        raise ValueError(f"Invalid uri: `{uri}`")
    if uri == "-":  # stdin/stout
        return uri
    if isinstance(uri, Path):
        return unquote(uri.absolute().as_uri())
    uri = validate_uri(uri)
    parsed = urlparse(uri)
    if parsed.scheme:
        if not parsed.netloc and not parsed.path:
            return f"{parsed.scheme}://"
        if parsed.scheme.startswith("http") and not http_unquote:
            return uri
        return unquote(uri)
    return unquote(Path(uri).absolute().as_uri())


class UriHandler:
    def __init__(self, uri: Uri, **kwargs) -> None:
        self.uri = ensure_uri(uri)
        self._kwargs = kwargs

    def __str__(self) -> str:
        return self.uri

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.uri})>"

    def __contains__(self, other: str) -> bool:
        return other in str(self.uri)

    def __truediv__(self, other: str) -> "UriHandler":
        return self.__class__(join_uri(self.uri, other), **self._kwargs)

    @cached_property
    def parsed(self) -> ParseResult:
        return urlparse(self.uri)

    @cached_property
    def scheme(self) -> str:
        return self.parsed.scheme

    @cached_property
    def _fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.url_to_fs(self.uri, **self._kwargs)[0]

    @cached_property
    def is_local(self) -> bool:
        return self.scheme == SCHEME_FILE

    @cached_property
    def is_http(self) -> bool:
        return "http" in self.scheme

    @cached_property
    def is_s3(self) -> bool:
        return self.scheme == SCHEME_S3

    @cached_property
    def is_memory(self) -> bool:
        return self.scheme == SCHEME_MEMORY

    @cached_property
    def is_redis(self) -> bool:
        return self.scheme == SCHEME_REDIS

    @cached_property
    def is_sql(self) -> bool:
        return "sql" in self.scheme


def make_uri(uri: Uri, **kwargs) -> UriHandler:
    """
    Factory for creating a `UriHandler` instance.

    Args:
        uri: Any uri-like input
        **kwargs: Extra keyword arguments passed to `UriHandler`

    Returns:
        A `UriHandler` instance
    """
    return UriHandler(uri, **kwargs)


def join_uri(uri: Any, path: str) -> str:
    """
    Ensure correct joining of arbitrary uris with a path.

    Example:
        ```python
        assert util.join_uri("http://example.org", "foo") == "http://example.org/foo"
        assert util.join_uri("http://example.org/", "foo") == "http://example.org/foo"
        assert util.join_uri("/tmp", "foo") == "file:///tmp/foo"
        assert util.join_uri(Path("./foo"), "bar").startswith("file:///")
        assert util.join_uri(Path("./foo"), "bar").endswith("foo/bar")
        assert util.join_uri("s3://foo/bar.pdf", "../baz.txt") == "s3://foo/baz.txt"
        assert util.join_uri("redis://foo/bar.pdf", "../baz.txt") == "redis://foo/baz.txt"
        ```

    Args:
        uri: Base uri
        path: Relative path to join on

    Returns:
        Absolute joined uri

    Raises:
        ValueError: For invalid uri (e.g. stdin: "-")
    """
    uri = ensure_uri(uri)
    if not uri or uri == "-":
        raise ValueError(f"Invalid uri: `{uri}`")
    if path == CURRENT:
        return uri
    # Normalize path: strip leading slashes, remove "." segments
    path = "/".join(p for p in path.split("/") if p and p != CURRENT)
    if not path:
        return uri
    validate_uri(path)
    parsed = urlparse(uri)
    scheme_prefix = f"{parsed.scheme}://"
    rest = uri[len(scheme_prefix) :]
    sep = "/" if rest else ""
    return f"{scheme_prefix}{rest.rstrip('/')}{sep}{path}"


def path_from_uri(uri: Uri) -> Path:
    """
    Get `pathlib.Path` object from an uri

    Examples:
        >>> path_from_uri("/foo/bar")
        Path("/foo/bar")
        >>> path_from_uri("file:///foo/bar")
        Path("/foo/bar")
        >>> path_from_uri("s3://foo/bar")
        Path("/foo/bar")

    Args:
        uri: (Full) path-like uri

    Returns:
        Path object for given uri
    """
    uri = ensure_uri(uri)
    path = "/" + uri[len(urlparse(uri).scheme) + 3 :].lstrip("/")
    return Path(path)


def name_from_uri(uri: Uri) -> str:
    """
    Extract the file name from an uri.

    Examples:
        >>> name_from_uri("/foo/bar.txt")
        bar.txt

    Args:
        uri: (Full) path-like uri

    Returns:
        File name
    """
    return path_from_uri(uri).name


def join_relpaths(*parts: Uri) -> str:
    """
    Join relative paths, strip leading and trailing "/"

    Examples:
        >>> join_relpaths("/a/b/c/", "d/e")
        "a/b/c/d/e"

    Args:
        *parts: Relative path segments

    Returns:
        Joined relative path
    """
    return "/".join(
        (p.strip("/") for p in map(str, parts) if p and p != CURRENT)
    ).strip("/")


def uri_to_path(uri: Uri) -> Path:
    uri = ensure_uri(uri)
    parsed = urlparse(uri)
    rest = uri[len(parsed.scheme) + 3 :]
    return Path(rest) if rest else Path("/")


def validate_uri(uri: Uri | None = None) -> str:
    if not uri:
        raise ValueError(f"Invalid empty uri: `{uri}`")
    uri = unquote(str(uri).strip())
    if not uri:
        raise ValueError(f"Invalid empty uri: `{uri}`")
    if "../" in uri:
        raise ValueError(f"Path traversal forbidden: `{uri}`")
    return uri


def validate_relative_uri(uri: Uri | None = None) -> str:
    """Empty uris, absolute uris are not allowed here"""
    uri = validate_uri(uri)
    if uri.startswith("/"):
        raise ValueError(f"Invalid absolute key: `{uri}`")
    uri_ = urlparse(uri)
    if uri_.scheme:
        raise ValueError(f"Invalid absolute key: `{uri_}`")
    uri = unquote(uri).rstrip("/")
    return "/".join(p for p in uri.split("/") if p != CURRENT)
