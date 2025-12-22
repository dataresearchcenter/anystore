"""
Simple memory dictionary store
"""

from datetime import datetime, timedelta
from fnmatch import fnmatch
from typing import Any, Generator

from anystore.exceptions import DoesNotExist
from anystore.logging import get_logger
from anystore.model import BaseStats
from anystore.store.abstract import VirtualIOMixin
from anystore.store.base import BaseStore
from anystore.types import Value

log = get_logger(__name__)


class MemoryStore(VirtualIOMixin, BaseStore):
    def __init__(self, **data):
        data["uri"] = "memory://"
        super().__init__(**data)
        self._store: dict[str, Any] = {}
        self._ttl: dict[str, datetime] = {}

    def _write(self, key: str, value: Value, **kwargs) -> None:
        self._store[key] = value
        ttl = kwargs.get("ttl")
        if ttl:
            self._ttl[key] = datetime.now() + timedelta(seconds=ttl)

    def _read(self, key: str, raise_on_nonexist: bool | None = True, **kwargs) -> Any:
        self._check_ttl(key)
        # `None` could be stored as an actual value, to implement `raise_on_nonexist`
        # we need to check this first:
        if raise_on_nonexist and not self._exists(key):
            raise DoesNotExist
        res = self._store.get(key)
        # mimic fs read mode:
        if kwargs.get("mode") == "r" and isinstance(res, bytes):
            res = res.decode()
        return res

    def _exists(self, key: str) -> bool:
        self._check_ttl(key)
        return key in self._store

    def _info(self, key: str) -> BaseStats:
        self._check_ttl(key)
        data = self._read(key)
        return BaseStats(size=len(data))

    def _delete(self, key: str) -> None:
        self._store.pop(key, None)

    def _iterate_keys(
        self,
        prefix: str | None = None,
        exclude_prefix: str | None = None,
        glob: str | None = None,
    ) -> Generator[str, None, None]:
        prefix = self.get_key(prefix or "")
        keys = list(self._store.keys())
        for key in keys:
            if key.startswith(prefix):
                if not exclude_prefix or not key.startswith(exclude_prefix):
                    if not glob or fnmatch(key, glob):
                        yield key

    def _check_ttl(self, key: str) -> None:
        ttl = self._ttl.get(key)
        if ttl and datetime.now() > ttl:
            self._delete(key)
