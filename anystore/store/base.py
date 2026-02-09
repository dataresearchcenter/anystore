"""
# Base store interface

The store class provides the top-level interface regardless for the storage
backend.
"""

import contextlib
from datetime import datetime, timezone
from functools import cached_property
from pathlib import Path
from typing import (
    IO,
    Any,
    Callable,
    ContextManager,
    Generator,
    Generic,
    Literal,
    overload,
)

import fsspec

from anystore.exceptions import DoesNotExist
from anystore.logging import get_logger
from anystore.logic.constants import DEFAULT_MODE
from anystore.logic.io import iter_lines, stream_bytes
from anystore.logic.serialize import Mode, from_store, to_store
from anystore.logic.uri import uri_to_path
from anystore.logic.virtual import VirtualIO
from anystore.model import Info, Stats, StoreModel
from anystore.settings import Settings
from anystore.store.keys import Keys
from anystore.types import Model, Raise, Uri, V
from anystore.util.checksum import DEFAULT_HASH_ALGORITHM, make_checksum
from anystore.util.data import clean_dict

settings = Settings()
log = get_logger(__name__)


class Store(StoreModel, Generic[V, Raise]):
    @cached_property
    def _fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.url_to_fs(self.uri, **self.ensure_kwargs())[0]

    @cached_property
    def _keys(self) -> Keys:
        return Keys(self.uri)

    # Explicit raise_on_nonexist=True always returns V
    @overload
    def get(
        self,
        key: Uri,
        raise_on_nonexist: Literal[True],
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> V:
        pass

    # Explicit raise_on_nonexist=False always returns V | None
    @overload
    def get(
        self,
        key: Uri,
        raise_on_nonexist: Literal[False],
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> V | None:
        pass

    # Store configured with raise_on_nonexist=True, param is None -> returns V
    @overload
    def get(
        self: "Store[V, Literal[True]]",
        key: Uri,
        raise_on_nonexist: None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> V:
        pass

    # Default case (store configured with False or unknown) -> returns V | None
    @overload
    def get(
        self,
        key: Uri,
        raise_on_nonexist: None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> V | None:
        pass

    def get(
        self,
        key: Uri,
        raise_on_nonexist: bool | None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> V | None:
        """
        Get a value from the store for the given key

        Args:
            key: Key relative to store base uri
            raise_on_nonexist: Raise `DoesNotExist` if key doesn't exist or stay
                silent, overrides store settings
            serialization_mode: Serialize result ("auto", "raw", "pickle",
                "json"), overrides store settings
            deserialization_func: Specific function to use (ignores
                `serialization_mode`), overrides store settings
            model: Pydantic serialization model (ignores `serialization_mode`
                and `deserialization_func`), overrides store settings

        Returns:
            The (optionally serialized) value for the key
        """
        serialization_mode = serialization_mode or self.serialization_mode
        deserialization_func = deserialization_func or self.deserialization_func
        model = model or self.model
        if raise_on_nonexist is None:
            raise_on_nonexist = self.raise_on_nonexist
        kwargs = self.ensure_kwargs(**kwargs)
        kwargs.pop("mode", None)
        key = self._keys.to_fs_key(key)
        self._check_ttl(key, raise_on_nonexist)
        try:
            return from_store(
                self._fs.cat_file(key, **kwargs),
                serialization_mode,
                deserialization_func=deserialization_func,
                model=model,
            )
        except FileNotFoundError:  # fsspec
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    # Explicit raise_on_nonexist=True always returns V
    @overload
    def pop(
        self,
        key: Uri,
        raise_on_nonexist: Literal[True],
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> V:
        pass

    # Explicit raise_on_nonexist=False always returns V | None
    @overload
    def pop(
        self,
        key: Uri,
        raise_on_nonexist: Literal[False],
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> V | None:
        pass

    # Store configured with raise_on_nonexist=True, param is None -> returns V
    @overload
    def pop(
        self: "Store[V, Literal[True]]",
        key: Uri,
        raise_on_nonexist: None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> V:
        pass

    # Default case (store configured with False or unknown) -> returns V | None
    @overload
    def pop(
        self,
        key: Uri,
        raise_on_nonexist: None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> V | None:
        pass

    def pop(
        self,
        key: Uri,
        raise_on_nonexist: bool | None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> V | None:
        """
        Retrieve the value for the given key and remove it from the store.

        Args:
            key: Key relative to store base uri
            raise_on_nonexist: Raise `DoesNotExist` if key doesn't exist or stay
                silent, overrides store settings
            serialization_mode: Serialize result ("auto", "raw", "pickle",
                "json"), overrides store settings
            deserialization_func: Specific function to use (ignores
                `serialization_mode`), overrides store settings
            model: Pydantic serialization model (ignores `serialization_mode`
                and `deserialization_func`), overrides store settings
            **kwargs: Any valid arguments for the stores `get` function

        Returns:
            The (optionally serialized) value for the key
        """
        value = self.get(
            key,
            raise_on_nonexist=raise_on_nonexist,
            serialization_mode=serialization_mode,
            deserialization_func=deserialization_func,
            model=model,
            **kwargs,
        )
        self.delete(key)
        return value

    def delete(self, key: Uri, ignore_errors: bool = False) -> None:
        """
        Delete the content at the given key.

        Args:
            key: Key relative to store base uri
            ignore_errors: Ignore exceptions if deletion fails
        """
        key = self._keys.to_fs_key(key)
        try:
            self._fs.rm_file(key)
        except Exception as e:
            if not ignore_errors:
                raise e

    # Explicit raise_on_nonexist=True always returns Generator
    @overload
    def stream(
        self,
        key: Uri,
        raise_on_nonexist: Literal[True],
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Generator[V, None, None]:
        pass

    # Explicit raise_on_nonexist=False always returns Generator | None
    @overload
    def stream(
        self,
        key: Uri,
        raise_on_nonexist: Literal[False],
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Generator[V, None, None] | None:
        pass

    # Store configured with raise_on_nonexist=True, param is None -> returns Generator
    @overload
    def stream(
        self: "Store[V, Literal[True]]",
        key: Uri,
        raise_on_nonexist: None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Generator[V, None, None]:
        pass

    # Default case (store configured with False or unknown) -> returns Generator | None
    @overload
    def stream(
        self,
        key: Uri,
        raise_on_nonexist: None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Generator[V, None, None] | None:
        pass

    def stream(
        self,
        key: Uri,
        raise_on_nonexist: bool | None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Generator[V, None, None] | None:
        """
        Stream a value line by line from the store for the given key

        Args:
            key: Key relative to store base uri
            raise_on_nonexist: Raise `DoesNotExist` if key doesn't exist or stay
                silent, overrides store settings
            serialization_mode: Serialize result ("auto", "raw", "pickle",
                "json"), overrides store settings
            deserialization_func: Specific function to use (ignores
                `serialization_mode`), overrides store settings
            model: Pydantic serialization model (ignores `serialization_mode`
                and `deserialization_func`), overrides store settings

        Yields:
            The (optionally serialized) values line by line

        Raises:
            anystore.exceptions.DoesNotExists: If key doesn't exist and
                raise_on_nonexist=True
        """
        model = model or self.model
        extra_kwargs = {
            "serialization_mode": serialization_mode or self.serialization_mode,
            "deserialization_func": deserialization_func or self.deserialization_func,
            "model": model,
        }
        try:
            with self.open(key) as i:
                for line in iter_lines(i):
                    yield from_store(line, **extra_kwargs)
        except FileNotFoundError:
            if raise_on_nonexist is True or self.raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    def put(
        self,
        key: Uri,
        value: V,
        serialization_mode: Mode | None = None,
        serialization_func: Callable | None = None,
        model: Model | None = None,
        ttl: int | None = None,
        **kwargs,
    ):
        """
        Store a value at the given key

        Args:
            key: Key relative to store base uri
            value: The content
            serialization_mode: Serialize value prior to storing ("auto", "raw",
                "pickle", "json"), overrides store settings
            serialization_func: Specific function to use (ignores
                `serialization_mode`), overrides store settings
            model: Pydantic serialization model (ignores `serialization_mode`
                and `deserialization_func`), overrides store settings
            ttl: Time to live (in seconds) for that key if the backend supports
                it (e.g. redis, sql)
        """
        if value is None and not self.store_none_values:
            return
        serialization_mode = serialization_mode or self.serialization_mode
        serialization_func = serialization_func or self.serialization_func
        model = model or self.model
        kwargs = self.ensure_kwargs(**kwargs)
        ttl = ttl or self.default_ttl or None
        key = self._keys.to_fs_key(key)
        self.ensure_parent(key)
        with self._fs.open(key, "wb", ttl=ttl) as o:
            o.write(
                to_store(
                    value,
                    serialization_mode,
                    serialization_func=serialization_func,
                    model=model,
                )
            )

    def _check_ttl(self, fs_key: str, raise_on_nonexist: bool | None = True) -> bool:
        """Check if key is expired by TTL; delete and return False if so."""
        if not self.default_ttl:
            return True
        try:
            info = Info(**self._fs.info(fs_key))
            if info.created_at is None:
                return True
            now = datetime.now(timezone.utc)
            if (now - info.created_at).total_seconds() > self.default_ttl:
                self._fs.rm_file(fs_key)
                return False
            return True
        except FileNotFoundError:  # fsspec
            if raise_on_nonexist:
                raise DoesNotExist(
                    f"Key does not exist: `{self._keys.to_fs_key(fs_key)}`"
                )
            return False

    def exists(self, key: Uri) -> bool:
        """Check if the given `key` exists"""
        key = self._keys.to_fs_key(key)
        if not self._check_ttl(key, raise_on_nonexist=False):
            return False
        return self._fs.exists(key)

    def info(self, key: Uri) -> Stats:
        """
        Get metadata for the given `key`.

        Returns:
            Key metadata
        """
        fs_key = self._keys.to_fs_key(key)
        info = self._fs.info(fs_key)
        name = Path(info.get("name", key)).name
        return Stats(
            **{
                **info,
                "name": name,
                "store": str(self.uri),
                "key": str(key),
            }
        )

    def ensure_kwargs(self, **kwargs) -> dict[str, Any]:
        config = clean_dict(self.backend_config)
        return {**config, **clean_dict(kwargs)}

    def iterate_keys(
        self,
        prefix: str | None = None,
        exclude_prefix: str | None = None,
        glob: str | None = None,
    ) -> Generator[str, None, None]:
        """
        Iterate through all the keys in the store based on given criteria.
        Criteria can be combined (e.g. include but exclude a subset).

        Example:
            ```python
            for key in store.iterate_keys(prefix="dataset1", glob="*.pdf"):
                data = store.get(key, mode="raw")
                parse(data)
            ```

        Args:
            prefix: Include only keys with the given prefix (e.g. "foo/bar")
            exclude_prefix: Exclude keys with this prefix
            glob: Path-style glob pattern for keys to filter (e.g. "foo/**/*.json")

        Returns:
            The matching keys as a generator of strings
        """
        if prefix:
            base = self._keys.to_fs_key(prefix)
        else:
            base = self._keys.key_prefix

        if hasattr(self._fs, "iter_find"):
            keys = self._fs.iter_find(base, glob=glob)
        elif glob:
            keys = self._fs.glob(f"{base}/{glob}")
        else:
            try:
                keys = self._fs.find(base)
            except FileNotFoundError:
                return
        for key in keys:
            rel = self._keys.from_fs_key(key)
            if exclude_prefix and rel.startswith(exclude_prefix):
                continue
            yield rel

    def iterate_values(
        self,
        prefix: str | None = None,
        exclude_prefix: str | None = None,
        glob: str | None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
    ) -> Generator[V, None, None]:
        """
        Iterate through all the values in the store based on given criteria.
        Criteria can be combined (e.g. include but exclude a subset).

        Example:
            ```python
            yield from store.iterate_values(prefix="dataset1", glob="*.pdf", model=MyModel)
            ```

        Args:
            prefix: Include only keys with the given prefix (e.g. "foo/bar")
            exclude_prefix: Exclude keys with this prefix
            glob: Path-style glob pattern for keys to filter (e.g. "foo/**/*.json")
            serialization_mode: Serialize result ("auto", "raw", "pickle",
                "json"), overrides store settings
            deserialization_func: Specific function to use (ignores
                `serialization_mode`), overrides store settings
            model: Pydantic serialization model (ignores `serialization_mode`
                and `deserialization_func`), overrides store settings

        Returns:
            The matching values as a generator of any (serialized) type
        """
        for key in self.iterate_keys(prefix, exclude_prefix, glob):
            value = self.get(
                key,
                serialization_mode=serialization_mode,
                deserialization_func=deserialization_func,
                model=model,
            )
            if value is not None:
                yield value

    def checksum(
        self, key: Uri, algorithm: str | None = DEFAULT_HASH_ALGORITHM, **kwargs: Any
    ) -> str:
        """
        Get the checksum for the value at the given key

        Args:
            key: Key relative to store base uri
            algorithm: Checksum algorithm from `hashlib` (default: "sha1")
            **kwargs: Pass through arguments to content retrieval

        Returns:
            The computed checksum
        """
        kwargs = self.ensure_kwargs(**kwargs)
        kwargs["mode"] = "rb"
        key = self._keys.to_fs_key(key)
        with self._fs.open(key, **kwargs) as io:
            return make_checksum(io, algorithm or DEFAULT_HASH_ALGORITHM)

    def open(
        self, key: Uri, mode: str | None = DEFAULT_MODE, **kwargs: Any
    ) -> ContextManager[IO]:
        """
        Open the given key similar to built-in `open()`

        Example:
            ```python
            from anystore import get_store

            store = get_store()
            with store.open("foo/bar.txt") as fh:
                return fh.read()
            ```

        Args:
            key: Key relative to store base uri
            mode: Open mode ("rb", "wb", "r", "w")
            **kwargs: Pass through arguments to backend

        Returns:
            The open handler
        """
        mode = mode or DEFAULT_MODE
        kwargs = self.ensure_kwargs(**kwargs)
        key = self._keys.to_fs_key(key)
        if "w" in mode:
            self.ensure_parent(key)
        return self._fs.open(key, mode=mode, **kwargs)

    def touch(self, key: Uri, **kwargs: Any) -> datetime:
        """
        Store the current timestamp at the given key

        Args:
            key: Key relative to store base uri
            **kwargs: Any valid arguments for the stores `put` function

        Returns:
            The timestamp
        """
        now = datetime.now(timezone.utc)
        self.put(key, now, **kwargs)
        return now

    def ensure_parent(self, fs_key: Uri) -> None:
        """Ensure existence of parent path. This mostly only is relevant for
        stores on local filesystem"""
        if self.is_local:
            parent = Path(fs_key).parent
            self._fs.mkdirs(parent, exist_ok=True)

    def to_uri(self, key: Uri) -> str:
        return self._keys.to_absolute_uri(key)

    @contextlib.contextmanager
    def local_path(self, key: Uri) -> Generator[Path, None, None]:
        """
        Download the resource at `key` for temporary local processing and get
        its local path. If the file itself is already on the local filesystem,
        the actual file will be used. The file is cleaned up when leaving the
        context, unless it was a local file, it won't be deleted in any case.

        Example:
            ```python
            store = get_store("s3://bucket")
            with store.local_path("data.json") as path:
                do_something(path)
            ```
        Yields:
            The absolute temporary `path` as a `pathlib.Path` object
        """
        uri = self._keys.to_fs_key(key)
        tmp = None
        if self.is_local:
            path = uri_to_path(uri)
        else:
            from anystore.store.virtual import get_virtual_store

            tmp = get_virtual_store()
            tmp_store = tmp.__enter__()
            stream_bytes(str(key), self, tmp_store)
            path = uri_to_path(tmp_store._keys.to_fs_key(key))
        try:
            yield path
        finally:
            if tmp is not None:
                tmp.__exit__(None, None, None)

    @contextlib.contextmanager
    def local_open(self, key: Uri) -> Generator[VirtualIO, None, None]:
        """
        Download a file for temporary local processing and get its checksum and
        an open handler. If the file itself is already on the local filesystem,
        the actual file will be used. The file is cleaned up when leaving the
        context, except if it was a local file, it won't be deleted in any case.

        Example:
            ```python
            store = get_store("http://example.org")
            with r.local_open("test.txt") as fh:
                smart_write(uri=f"./local/{fh.checksum}", fh.read())
            ```

        Yields:
            A generic file-handler like context object. It has 3 extra attributes:
                - `checksum`
                - the absolute temporary `path` as a `pathlib.Path` object
                - [`info`][anystore.model.Stats] object
        """
        with self.local_path(key) as path:
            with path.open("rb") as fh:
                checksum = make_checksum(fh)
                fh.seek(0)
                yield VirtualIO(fh, checksum=checksum, path=path, info=self.info(key))
