import contextlib
from typing import IO, Any, Generator

from anystore.model import BaseStats
from anystore.types import AnyStrGenerator, Value


class AbstractBackend:
    """Base backend class with methods that all backends need to implement"""

    def _write(self, key: str, value: Value, **kwargs) -> None:
        """
        Write value with key to actual backend
        """
        raise NotImplementedError

    def _read(self, key: str, raise_on_nonexist: bool | None = True, **kwargs) -> Any:
        """
        Read key from actual backend
        """
        raise NotImplementedError

    def _delete(self, key: str) -> None:
        """
        Delete key from actual backend
        """
        raise NotImplementedError

    def _stream(self, key: str, **kwargs) -> AnyStrGenerator:
        """
        Stream key line by line from actual backend
        """
        if kwargs.get("mode") not in ["rb", "r"]:
            kwargs["mode"] = "rb"
        with self._open(key, **kwargs) as i:
            while line := i.readline():
                yield line

    def _exists(self, key: str) -> bool:
        """
        Check if the given key exists
        """
        raise NotImplementedError

    def _info(self, key: str) -> BaseStats:
        """
        Get metadata about key and its value
        """
        raise NotImplementedError

    def _get_key_prefix(self) -> str:
        """
        Get backend specific key prefix
        """
        return self.key_prefix or ""

    def _iterate_keys(
        self,
        prefix: str | None = None,
        exclude_prefix: str | None = None,
        glob: str | None = None,
    ) -> Generator[str, None, None]:
        """
        Backend specific key iterator
        """
        raise NotImplementedError

    def _get_relpath(self, key: str) -> str:
        """
        Get relative path to the given key (backend specific)
        """
        return self.get_key(key).replace(self.uri, "").strip("/")

    @contextlib.contextmanager
    def _open(self, key: str, **kwargs) -> Generator[IO, None, None]:
        """
        Get a io handler
        """
        raise NotImplementedError
