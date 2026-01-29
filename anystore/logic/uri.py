from functools import cached_property
from urllib.parse import ParseResult, urlparse

import fsspec

from anystore.types import Uri
from anystore.util import ensure_uri


class UriHandler:
    def __init__(self, uri: Uri, **kwargs) -> None:
        self.uri = ensure_uri(uri)
        self._kwargs = kwargs

    def __str__(self) -> str:
        return self.uri

    def __repr__(self) -> str:
        return f"<UriHandler({self.uri})>"

    def __contains__(self, other: str) -> bool:
        return other in str(self.uri)

    @cached_property
    def parsed(self) -> ParseResult:
        return urlparse(self.uri)

    @cached_property
    def _fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.url_to_fs(self.uri, **self._kwargs)[0]
