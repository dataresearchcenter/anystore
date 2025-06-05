"""
# Base store interface

The store class provides the top-level interface regardless for the storage
backend.
"""

import contextlib
from datetime import datetime
from io import BytesIO, StringIO
from pathlib import Path
from typing import IO, Any, Callable, ContextManager, Generator
from urllib.parse import unquote

from anystore.exceptions import DoesNotExist, ReadOnlyError
from anystore.io import DEFAULT_MODE
from anystore.logging import get_logger
from anystore.model import Stats, StoreModel
from anystore.serialize import Mode, from_store, to_store
from anystore.settings import Settings
from anystore.store.abstract import AbstractBackend
from anystore.types import Model, Uri
from anystore.util import DEFAULT_HASH_ALGORITHM, clean_dict, make_checksum

settings = Settings()

log = get_logger(__name__)


def check_readonly(func: Callable):
    """Guard for read-only store. Write functions should be decorated with it"""

    def _check(store: "BaseStore", *args, **kwargs):
        if store.readonly:
            raise ReadOnlyError(f"Store `{store.uri}` is configured readonly!")
        return func(store, *args, **kwargs)

    return _check


class BaseStore(StoreModel, AbstractBackend):
    def get(
        self,
        key: Uri,
        raise_on_nonexist: bool | None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Any:
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
        key = self.get_key(key)
        try:
            return from_store(
                self._read(key, raise_on_nonexist, **kwargs),
                serialization_mode,
                deserialization_func=deserialization_func,
                model=model,
            )
        except FileNotFoundError:  # fsspec
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    @check_readonly
    def pop(self, key: Uri, *args: Any, **kwargs: Any) -> Any:
        """
        Retrieve the value for the given key and remove it from the store.

        Args:
            key: Key relative to store base uri
            *args: Any valid arguments for the stores `get` function
            **kwargs: Any valid arguments for the stores `get` function

        Returns:
            The (optionally serialized) value for the key
        """
        value = self.get(key, *args, **kwargs)
        self._delete(self.get_key(key))
        return value

    @check_readonly
    def delete(self, key: Uri, ignore_errors: bool = False) -> None:
        """
        Delete the content at the given key.

        Args:
            key: Key relative to store base uri
            ignore_errors: Ignore exceptions if deletion fails
        """
        try:
            self._delete(self.get_key(key))
        except Exception as e:
            if not ignore_errors:
                raise e

    def stream(
        self,
        key: Uri,
        raise_on_nonexist: bool | None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Generator[Any, None, None]:
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
        key = self.get_key(key)
        model = model or self.model
        extra_kwargs = {
            "serialization_mode": serialization_mode or self.serialization_mode,
            "deserialization_func": deserialization_func or self.deserialization_func,
            "model": model,
        }
        try:
            for line in self._stream(key, **kwargs):
                yield from_store(line, **extra_kwargs)
        except (FileNotFoundError, DoesNotExist):
            if raise_on_nonexist:
                raise DoesNotExist(f"Key does not exist: `{key}`")
            return None

    @check_readonly
    def put(
        self,
        key: Uri,
        value: Any,
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
        key = self.get_key(key)
        ttl = ttl or self.default_ttl or None
        self._write(
            key,
            to_store(
                value,
                serialization_mode,
                serialization_func=serialization_func,
                model=model,
            ),
            ttl=ttl,
        )

    def exists(self, key: Uri) -> bool:
        """Check if the given `key` exists"""
        return self._exists(self.get_key(key))

    def info(self, key: Uri) -> Stats:
        """
        Get metadata for the given `key`.

        Returns:
            Key metadata
        """
        try:
            stats = self._info(self.get_key(key))
            stats = stats.model_dump()
        except Exception as e:
            log.warn(
                f"Cannot get info: `{e.__class__.__name__}`: {e}",
                key=key,
                store=self.uri,
            )
            stats = {"size": 0}
        key = str(key)
        return Stats(
            **stats,
            name=Path(key).name,
            store=str(self.uri),
            key=key,
        )

    def ensure_kwargs(self, **kwargs) -> dict[str, Any]:
        config = clean_dict(self.backend_config)
        return {**config, **clean_dict(kwargs)}

    def get_key(self, key: Uri) -> str:
        key = str(key)
        key = f"{self._get_key_prefix()}/{key}".strip("/")
        return unquote(key)

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
        for key in self._iterate_keys(prefix, exclude_prefix, glob):
            yield unquote(key)

    def iterate_values(
        self,
        prefix: str | None = None,
        exclude_prefix: str | None = None,
        glob: str | None = None,
        serialization_mode: Mode | None = None,
        deserialization_func: Callable | None = None,
        model: Model | None = None,
        **kwargs,
    ) -> Generator[Any, None, None]:
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
        for key in self._iterate_keys(prefix, exclude_prefix, glob):
            yield self.get(
                key,
                serialization_mode=serialization_mode,
                deserialization_func=deserialization_func,
                model=model,
            )

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
        key = self.get_key(key)
        with self._open(key, **kwargs) as io:
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
        if self.readonly and ("w" in mode or "a" in mode):
            raise ReadOnlyError(f"Store `{self.uri}` is configured readonly!")
        kwargs = self.ensure_kwargs(**kwargs)
        key = self.get_key(key)
        return self._open(key, mode=mode, **kwargs)

    @check_readonly
    def touch(self, key: Uri, **kwargs: Any) -> datetime:
        """
        Store the current timestamp at the given key

        Args:
            key: Key relative to store base uri
            **kwargs: Any valid arguments for the stores `put` function

        Returns:
            The timestamp
        """
        now = datetime.now()
        self.put(key, now, **kwargs)
        return now


class VirtualIOMixin:
    """
    Fake `open()` method for non file-like backends
    """

    @contextlib.contextmanager
    def _open(self, key: str, **kwargs) -> Generator[BytesIO | StringIO, None, None]:
        mode = kwargs.get("mode", DEFAULT_MODE)
        writer = "w" in mode
        if not writer:
            content = self._read(key, **kwargs)
            if "b" in mode:
                handler = BytesIO(content)
            else:
                handler = StringIO(content)
        else:
            if "b" in mode:
                handler = BytesIO()
            else:
                handler = StringIO()
        try:
            yield handler
        finally:
            if writer:
                self._write(key, handler.getvalue(), **kwargs)
            handler.close()
