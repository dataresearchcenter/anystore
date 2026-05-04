from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, Request, Response
from fastapi.responses import StreamingResponse
from rigour.mime import DEFAULT
from starlette.concurrency import run_in_threadpool

from anystore.api.util import (
    is_file,
    iter_chunks,
    iter_range,
    parse_range,
    stats_headers,
)
from anystore.store.base import Store as _Store
from anystore.util.checksum import DEFAULT_HASH_ALGORITHM


def get_store(request: Request) -> _Store:
    return request.app.state.store


Store = Annotated[_Store, Depends(get_store)]


router = APIRouter()


@router.get("/{key:path}")
def get(
    store: Store,
    key: str,
    exclude_prefix: str | None = None,
    glob: str | None = None,
    range: str | None = Header(default=None),
) -> Response:
    if key.endswith("/") or not key:
        # listing endpoint: we return relative child paths here to mimic
        # expected behaviour for clients. Our anystore fs later prepends the
        # base path again for the expected `iterate_keys()` return format.`
        prefix = key.rstrip("/") or None
        strip = len(prefix) + 1 if prefix else 0
        # exclude_prefix is interpreted relative to the listed prefix
        # (matches putf.sh); iterate_keys compares against store-root-
        # relative paths so qualify the user-supplied filter here.
        if prefix and exclude_prefix:
            qualified_exclude = f"{prefix}/{exclude_prefix.lstrip('/')}"
        else:
            qualified_exclude = exclude_prefix
        data = (
            f"{k[strip:]}\n".encode()
            for k in store.iterate_keys(
                prefix=prefix,
                exclude_prefix=qualified_exclude,
                glob=glob,
            )
        )
        return StreamingResponse(data, media_type=DEFAULT)

    if not is_file(store, key):
        raise FileNotFoundError(key)
    stats = store.info(key)
    headers = stats_headers(stats)
    if range is not None:
        total = stats.size
        start, end = parse_range(range, total)
        length = end - start + 1
        headers["Content-Range"] = f"bytes {start}-{end}/{total}"
        headers["Content-Length"] = str(length)

        def _range_iter():
            with store.open(key, "rb") as fh:
                yield from iter_range(fh, start, length)

        return StreamingResponse(
            _range_iter(),
            status_code=206,
            media_type=stats.mimetype,
            headers=headers,
        )

    def _full_iter():
        with store.open(key, "rb") as fh:
            yield from iter_chunks(fh)

    return StreamingResponse(
        _full_iter(),
        media_type=stats.mimetype,
        headers=headers,
    )


@router.head("/{key:path}")
def head(
    store: Store,
    key: str,
    checksum: bool = False,
    algorithm: str = DEFAULT_HASH_ALGORITHM,
) -> Response:
    if not is_file(store, key):
        return Response(status_code=404)
    stats = store.info(key)
    headers = stats_headers(stats)
    if checksum:
        headers["x-anystore-checksum"] = store.checksum(key, algorithm=algorithm)
    return Response(status_code=200, headers=headers)


@router.put("/{key:path}")
async def put(store: Store, key: str, request: Request) -> Response:
    # Sync fsspec I/O wrapped in a threadpool so it doesn't block the event
    # loop while we await the next chunk from the request body.
    fh = await run_in_threadpool(store.open, key, "wb")
    try:
        async for chunk in request.stream():
            await run_in_threadpool(fh.write, chunk)
    finally:
        await run_in_threadpool(fh.close)
    return Response(status_code=201)


@router.delete("/{key:path}")
def delete(store: Store, key: str) -> Response:
    if not store.exists(key):
        return Response(status_code=404)
    store.delete(key)
    return Response(status_code=204)


@router.patch("/{key:path}")
def touch(store: Store, key: str) -> Response:
    ts = store.touch(key)
    return Response(ts.isoformat())
