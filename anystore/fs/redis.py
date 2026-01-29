"""
fsspec-compatible filesystem backed by Redis (or fakeredis / Kvrocks).
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

import redis
from fsspec.spec import AbstractFileSystem

from anystore.functools import weakref_cache as cache
from anystore.logging import get_logger
from anystore.settings import Settings
from anystore.util import mask_uri

if TYPE_CHECKING:
    import fakeredis

log = get_logger(__name__)


@cache
def _get_redis(uri: str) -> fakeredis.FakeStrictRedis | redis.Redis:
    settings = Settings()
    if settings.redis_debug:
        import fakeredis

        con = fakeredis.FakeStrictRedis()
        con.ping()
        log.info("Redis connected: `fakeredis`")
        return con
    pool = redis.ConnectionPool.from_url(uri)
    con = redis.Redis(connection_pool=pool)
    con.ping()
    log.info(f"Redis connected: `{mask_uri(uri)}`")
    return con


class RedisFileSystem(AbstractFileSystem):
    """A flat key-value filesystem stored in Redis.

    Directories are emulated: any key containing ``/`` implicitly creates
    parent "directories".

    Parameters
    ----------
    url : str
        Redis connection URL (e.g. ``redis://localhost:6379/0``).
    """

    protocol = "redis"
    root_marker = ""

    def __init__(self, url: str = "redis://localhost:6379/0", **storage_options):
        super().__init__(url=url, **storage_options)
        self._con = _get_redis(url)

    # ------------------------------------------------------------------
    # ls
    # ------------------------------------------------------------------

    def ls(self, path: str, detail: bool = True, **kwargs) -> list:  # type: ignore[override]
        path = self._strip_protocol(path).strip("/")
        prefix = f"{path}/" if path else ""

        entries: dict[str, dict] = {}
        pattern = f"{prefix}*" if prefix else "*"
        for raw_key in self._con.scan_iter(pattern):
            key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            key = key.strip("/")

            if prefix:
                relative = key[len(prefix) :]
            else:
                relative = key

            if "/" in relative:
                child = relative.split("/", 1)[0]
                child_path = f"{prefix}{child}" if prefix else child
                if child_path not in entries:
                    entries[child_path] = {
                        "name": child_path,
                        "size": 0,
                        "type": "directory",
                    }
            else:
                size = self._con.strlen(raw_key)
                entries[key] = {
                    "name": key,
                    "size": size,
                    "type": "file",
                }

        result = list(entries.values())
        if not detail:
            return [e["name"] for e in result]
        return result

    # ------------------------------------------------------------------
    # info
    # ------------------------------------------------------------------

    def info(self, path: str, **kwargs) -> dict:
        path = self._strip_protocol(path).strip("/")
        if not path:
            return {"name": "", "size": 0, "type": "directory"}

        if self._con.exists(path):
            size = self._con.strlen(path)
            return {
                "name": path,
                "size": size,
                "type": "file",
            }

        # Check for implicit directory
        pattern = f"{path}/*"
        for _ in self._con.scan_iter(pattern, count=1):
            return {"name": path, "size": 0, "type": "directory"}

        raise FileNotFoundError(path)

    # ------------------------------------------------------------------
    # _open
    # ------------------------------------------------------------------

    def _open(  # type: ignore[override]
        self,
        path: str,
        mode: str = "rb",
        **kwargs,
    ) -> io.BytesIO | RedisFileWriter:
        path = self._strip_protocol(path).strip("/")
        if "r" in mode:
            data: bytes | None = self._con.get(path)  # type: ignore[assignment]
            if data is None:
                raise FileNotFoundError(path)
            return io.BytesIO(data)
        else:
            return RedisFileWriter(self, path)

    # ------------------------------------------------------------------
    # cat_file / pipe_file
    # ------------------------------------------------------------------

    def cat_file(self, path: str, start=None, end=None, **kwargs) -> bytes:
        path = self._strip_protocol(path).strip("/")
        data: bytes | None = self._con.get(path)  # type: ignore[assignment]
        if data is None:
            raise FileNotFoundError(path)
        if start is not None or end is not None:
            data = data[start:end]
        return data

    def pipe_file(self, path: str, value: bytes, mode="overwrite", **kwargs) -> None:
        path = self._strip_protocol(path).strip("/")
        if mode == "create" and self._con.exists(path):
            raise FileExistsError(path)
        self._con.set(path, value)

    # ------------------------------------------------------------------
    # rm_file
    # ------------------------------------------------------------------

    def rm_file(self, path: str) -> None:
        path = self._strip_protocol(path).strip("/")
        self._con.delete(path)

    def _rm(self, path: str) -> None:
        self.rm_file(path)

    # ------------------------------------------------------------------
    # mkdir / makedirs – no-op for flat store
    # ------------------------------------------------------------------

    def mkdir(self, path: str, create_parents: bool = True, **kwargs) -> None:
        pass

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        pass

    # ------------------------------------------------------------------
    # Protocol helpers
    # ------------------------------------------------------------------

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        if path.startswith("redis://"):
            path = path[len("redis://") :]
        return path.strip("/")


class RedisFileWriter(io.BytesIO):
    """Write buffer that flushes to Redis on close."""

    def __init__(self, fs: RedisFileSystem, path: str):
        super().__init__()
        self._fs = fs
        self._path = path

    def close(self) -> None:
        if not self.closed:
            self._fs._con.set(self._path, self.getvalue())
        super().close()
