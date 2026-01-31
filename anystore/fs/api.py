"""
fsspec-compatible filesystem backed by an anystore REST API over HTTP.

Subclasses ``HTTPFileSystem`` so that standard HTTP operations (range reads,
seekable files) are handled natively by fsspec.  Only the API-specific
endpoints (listing, writes, deletes) are overridden.
"""

from __future__ import annotations

import asyncio
import re
from typing import AsyncIterator

from fsspec.asyn import sync
from fsspec.implementations.http import HTTPFileSystem
from fsspec.spec import AbstractBufferedFile

from anystore.logging import get_logger

log = get_logger(__name__)

_ANYSTORE_PREFIX_RE = re.compile(r"^anystore\+")


class ApiFileSystem(HTTPFileSystem):
    """A flat key-value filesystem backed by a remote anystore API.

    Parameters
    ----------
    url : str
        API base URL, e.g. ``anystore+http://localhost:8000``.
    """

    protocol = ("anystore+http", "anystore+https")
    root_marker = ""

    def __init__(self, url: str | None = None, **storage_options):
        super().__init__(**storage_options)

    @staticmethod
    def _base_url(url: str) -> str:
        """Extract API base (scheme + host) from a full URL."""
        from urllib.parse import urlparse

        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}"

    # ------------------------------------------------------------------
    # Protocol helpers
    # ------------------------------------------------------------------

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        """``anystore+http://host/key`` → ``http://host/key``"""
        return _ANYSTORE_PREFIX_RE.sub("", path)

    def encode_url(self, url):
        return super().encode_url(self._strip_protocol(url))

    # ------------------------------------------------------------------
    # exists – use HEAD via _info instead of HTTPFileSystem's GET
    # ------------------------------------------------------------------

    async def _info(self, url, **kwargs):
        try:
            return await super()._info(url, **kwargs)
        except FileNotFoundError:
            # Check for implicit directory via prefix listing
            _, _, flat_urls = await self._ls_flat(url)
            if flat_urls:
                url = self._strip_protocol(url)
                return {"name": url, "size": 0, "type": "directory"}
            raise

    async def _exists(self, path, **kwargs):
        try:
            await self._info(path, **kwargs)
            return True
        except FileNotFoundError:
            return False

    # ------------------------------------------------------------------
    # ls
    # ------------------------------------------------------------------

    async def _ls_flat(self, url, **kwargs):
        """Return all keys under *url* as full URLs (no directory grouping)."""
        url = self._strip_protocol(url)
        base = self._base_url(url)
        path = url[len(base) :].strip("/")
        params = {}
        if path:
            params["prefix"] = path
        session = await self.set_session()
        async with session.post(base + "/_list", params=params, **self.kwargs) as resp:
            resp.raise_for_status()
            text = await resp.text()
            keys = [k for k in text.splitlines() if k]
        return base, path, [f"{base}/{k}" for k in keys]

    async def _ls_real(self, url, detail=True, **kwargs):
        base, path, flat_urls = await self._ls_flat(url, **kwargs)
        prefix = f"{base}/{path}/" if path else f"{base}/"

        entries: dict[str, dict] = {}
        for full_url in flat_urls:
            relative = full_url[len(prefix) :]
            if "/" in relative:
                child = relative.split("/", 1)[0]
                child_url = f"{prefix}{child}"
                if child_url not in entries:
                    entries[child_url] = {
                        "name": child_url,
                        "size": 0,
                        "type": "directory",
                    }
            else:
                if detail:
                    entries[full_url] = await self._info(full_url, **kwargs)
                else:
                    entries[full_url] = {"name": full_url}

        result = list(entries.values())
        if not detail:
            return [e["name"] for e in result]
        return result

    # ------------------------------------------------------------------
    # find
    # ------------------------------------------------------------------

    async def _find(
        self, path="", maxdepth=None, withdirs=False, detail=False, **kwargs
    ):
        _, _, flat_urls = await self._ls_flat(path, **kwargs)
        if detail:
            entries = [await self._info(u, **kwargs) for u in flat_urls]
            return {e["name"]: e for e in entries}
        return flat_urls

    # ------------------------------------------------------------------
    # _open
    # ------------------------------------------------------------------

    def _open(self, path, mode="rb", **kwargs):
        if "w" in mode:
            return ApiFileWriter(self, path, mode="wb", **kwargs)
        return super()._open(path, mode=mode, **kwargs)

    # ------------------------------------------------------------------
    # pipe_file / rm_file
    # ------------------------------------------------------------------

    async def _pipe_file(self, url, data, **kwargs):
        url = self._strip_protocol(url)
        session = await self.set_session()
        async with session.put(url, data=data, **self.kwargs) as resp:
            resp.raise_for_status()

    async def _rm_file(self, url, **kwargs):
        url = self._strip_protocol(url)
        session = await self.set_session()
        async with session.delete(url, **self.kwargs) as resp:
            resp.raise_for_status()

    def _rm(self, path):
        self.rm_file(path)

    # ------------------------------------------------------------------
    # mkdir / makedirs – no-op for flat store
    # ------------------------------------------------------------------

    def mkdir(self, path, create_parents=True, **kwargs):
        pass

    def makedirs(self, path, exist_ok=False):
        pass


class ApiFileWriter(AbstractBufferedFile):
    """Streaming write file backed by a chunked PUT to the anystore API.

    Uses an ``asyncio.Queue`` to feed an async generator into a single
    ``PUT /{key}?stream=true`` request so that data is streamed without
    buffering the entire blob in memory.
    """

    def __init__(self, fs: ApiFileSystem, path: str, mode: str = "wb", **kwargs):
        kwargs.pop("ttl", None)
        super().__init__(fs, path, mode=mode, **kwargs)
        self._queue: asyncio.Queue[bytes | None] | None = None
        self._upload_task: asyncio.Task | None = None

    async def _body_generator(self) -> AsyncIterator[bytes]:
        """Yield chunks from the queue until ``None`` sentinel."""
        assert self._queue is not None
        while True:
            chunk = await self._queue.get()
            if chunk is None:
                break
            yield chunk

    async def _do_upload(self) -> None:
        """Run the PUT request, streaming from ``_body_generator``."""
        session = await self.fs.set_session()
        url = self.fs._strip_protocol(self.path)
        async with session.put(url, data=self._body_generator()) as resp:
            resp.raise_for_status()

    def _initiate_upload(self) -> None:
        """Start the streaming PUT in the background."""
        self._queue = asyncio.Queue()
        loop = self.fs.loop
        self._upload_task = asyncio.run_coroutine_threadsafe(self._do_upload(), loop)

    def _upload_chunk(self, final: bool = False) -> bool:
        """Push buffered data into the streaming PUT request."""
        data = self.buffer.getvalue()
        if data:
            sync(self.fs.loop, self._queue.put, data)
        if final:
            # Send sentinel to close the async generator
            sync(self.fs.loop, self._queue.put, None)
            # Wait for the PUT to complete
            self._upload_task.result()
        return True

    def _fetch_range(self, start: int, end: int) -> bytes:
        raise NotImplementedError("ApiFileWriter is write-only")
