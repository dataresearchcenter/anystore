"""
Store backend using any file-like location usable via `fsspec`
"""

import re
from datetime import datetime
from functools import cached_property
from typing import IO, AnyStr, ContextManager, Generator

import fsspec
import httpx
from dateparser import parse as parse_date

from anystore.exceptions import DoesNotExist
from anystore.io import smart_open, smart_read, smart_write
from anystore.model import BaseStats
from anystore.store.base import BaseStore
from anystore.types import Value
from anystore.util import join_relpaths, join_uri


class Store(BaseStore):
    @cached_property
    def _fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.url_to_fs(self.uri, **self.ensure_kwargs())[0]

    def _write(self, key: str, value: Value, **kwargs) -> None:
        kwargs.pop("ttl", None)
        smart_write(key, value, **kwargs)

    def _read(
        self, key: str, raise_on_nonexist: bool | None = True, **kwargs
    ) -> Value | None:
        self._check_ttl(key)
        try:
            return smart_read(key, **kwargs)
        except FileNotFoundError:
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    def _exists(self, key: str) -> bool:
        self._check_ttl(key)
        return self._fs.exists(key)

    def _info(self, key: str, check_ttl: bool | None = True) -> BaseStats:
        if check_ttl:
            self._check_ttl(key)
        data = self._fs.info(key)
        # FIXME fsspec http no headers?
        if self.is_http:
            try:
                res = httpx.head(key)
                if "last-modified" in res.headers:
                    data["updated_at"] = parse_date(res.headers["last-modified"])
            except httpx.ReadTimeout:
                pass
        ts = data.pop("created", None)
        data["updated_at"] = data.get("updated_at") or data.pop(
            "LastModified", None
        )  # s3
        if ts:
            data["created_at"] = datetime.fromtimestamp(ts)
        return BaseStats(**data, raw=data)

    def _delete(self, key: str, check_ttl=False) -> None:
        if check_ttl:
            self._check_ttl(key)
        self._fs.delete(key)

    def _get_key_prefix(self) -> str:
        return str(self.uri).rstrip("/")

    def _open(self, key: str, **kwargs) -> ContextManager[IO[AnyStr]]:
        self._check_ttl(key)
        return smart_open(key, **kwargs)

    def _iterate_keys(
        self,
        prefix: str | None = None,
        exclude_prefix: str | None = None,
        glob: str | None = None,
    ) -> Generator[str, None, None]:
        prefix = prefix or ""
        exclude_prefix = exclude_prefix or ""
        glob = glob or ""

        def _filter(key: str) -> bool:
            if not self._check_ttl(key):
                return False
            if exclude_prefix and key.startswith(exclude_prefix):
                return False
            if self.is_http and HTTP_INDEX_RE.search(key):
                return False
            return True

        if glob:
            glob_path = self.get_key(join_relpaths(prefix, glob))
            for key in self._fs.glob(glob_path):
                if self.is_s3:  # /{bucket}/key
                    key = f"{self.scheme}://{key}"
                key = self._get_relpath(join_uri(self.uri, key))
                if _filter(key):
                    yield key
        else:
            path = self.get_key(prefix) + "/"
            for _, children, keys in self._fs.walk(path, maxdepth=1):
                for key in keys:
                    key = join_relpaths(self._get_relpath(path), key)
                    if _filter(key):
                        yield key
                for key in children:
                    key = self._get_relpath(join_uri(path, key))
                    if _filter(key):
                        yield from self._iterate_keys(key, exclude_prefix)

    def _check_ttl(self, key: str) -> bool:
        """Check if given key is expired and delete it"""

        if self.default_ttl:
            try:
                info = self._info(key, check_ttl=False)
                if info.updated_at:
                    now = datetime.now().astimezone(info.updated_at.tzinfo)
                    last = info.updated_at.astimezone(now.tzinfo)
                    if (now - last).total_seconds() > self.default_ttl:
                        self._delete(key, check_ttl=False)
                        return False
            except FileNotFoundError:
                pass
        return True


HTTP_INDEX_RE = re.compile(r"\?C=.&amp;.=.")
