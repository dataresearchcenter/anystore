"""
Use a zip file as a store backend. Read-only for remote sources, writeable locally
"""

import contextlib
from datetime import datetime
from typing import Any, BinaryIO, Generator, Literal, TextIO

from fsspec.implementations.zip import ZipFileSystem

from anystore.exceptions import DoesNotExist, ReadOnlyError
from anystore.io import DEFAULT_MODE, DEFAULT_WRITE_MODE
from anystore.model import BaseStats
from anystore.store.base import BaseStore
from anystore.types import Uri, Value
from anystore.util import join_relpaths


class ZipStore(BaseStore):
    def __init__(self, **data):
        super().__init__(**data)
        self.__exists = None

    @contextlib.contextmanager
    def _get_handler(self, mode: Literal["r", "a"]) -> Any:
        if self.__exists is None:
            try:
                handler = ZipFileSystem(str(self.uri), mode, **self.ensure_kwargs())
            except FileNotFoundError:
                # create initial file
                ZipFileSystem(str(self.uri), "w", **self.ensure_kwargs()).close()
                self.__exists = True
                handler = ZipFileSystem(str(self.uri), mode, **self.ensure_kwargs())
        else:
            handler = ZipFileSystem(str(self.uri), mode, **self.ensure_kwargs())
        try:
            yield handler
        finally:
            handler.close()

    @contextlib.contextmanager
    def _reader(self, key: str, **kwargs) -> Any:
        kwargs.pop("compression", None)
        with self._get_handler("r") as reader:
            handler = reader.open(key, **kwargs)
            try:
                yield handler
            finally:
                handler.close()

    @contextlib.contextmanager
    def _writer(self, key: str, **kwargs) -> Any:
        kwargs.pop("compression", None)
        if self._exists(key):
            raise ReadOnlyError(
                f"Can not overwrite already existing key `{key}` (ZipFile)"
            )
        with self._get_handler("a") as writer:
            handler = writer.open(key, **kwargs)
            try:
                yield handler
            finally:
                handler.close()

    def _write(self, key: str, value: Value, **kwargs) -> None:
        kwargs["mode"] = kwargs.pop("mode", DEFAULT_WRITE_MODE)
        with self._writer(key, **kwargs) as fh:
            fh.write(value)

    def _read(
        self, key: str, raise_on_nonexist: bool | None = True, **kwargs
    ) -> Value | None:
        kwargs["mode"] = kwargs.pop("mode", DEFAULT_MODE)
        try:
            with self._reader(key, **kwargs) as fh:
                return fh.read()
        except (KeyError, DoesNotExist):
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    def _exists(self, key: str) -> bool:
        with self._get_handler("r") as reader:
            try:
                return reader.exists(key)
            except DoesNotExist:
                return False

    def _info(self, key: str) -> BaseStats:
        with self._get_handler("r") as reader:
            return BaseStats(**reader.info(key))

    def _delete(self, key: str) -> None:
        raise ReadOnlyError(f"Can not delete `{key}`: ZipStore is append-only!")

    def _open(self, key: str, **kwargs) -> BinaryIO | TextIO:
        kwargs["mode"] = kwargs.pop("mode", DEFAULT_MODE)
        if "r" in kwargs["mode"]:
            return self._reader(key, **kwargs)
        return self._writer(key, **kwargs)

    def touch(self, key: Uri, **kwargs) -> None:
        if not self.exists(key):
            self.put(key, datetime.now(), **kwargs)

    def _iterate_keys(
        self,
        prefix: str | None = None,
        exclude_prefix: str | None = None,
        glob: str | None = None,
    ) -> Generator[str, None, None]:
        prefix = self.get_key(prefix or "")
        exclude_prefix = exclude_prefix or ""
        glob = glob or ""

        with self._get_handler("r") as reader:
            if glob:
                for key in reader.glob(self.get_key(join_relpaths(prefix, glob))):
                    if not exclude_prefix or not key.startswith(exclude_prefix):
                        yield key
            else:
                path = self.get_key(prefix)
                for _, children, keys in reader.walk(path, maxdepth=1):
                    for key in keys:
                        key = join_relpaths(self._get_relpath(path), key)
                        if not exclude_prefix or not key.startswith(exclude_prefix):
                            yield key
                    for key in children:
                        key = join_relpaths(path, key)
                        if not exclude_prefix or not key.startswith(exclude_prefix):
                            yield from self._iterate_keys(key, exclude_prefix)
