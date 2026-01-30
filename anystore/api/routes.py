from __future__ import annotations

from fastapi import APIRouter, Depends, Header, Request, Response
from fastapi.responses import StreamingResponse
from rigour.mime import DEFAULT

from anystore.api.util import iter_chunks, iter_range, parse_range
from anystore.store.base import Store


def get_store(request: Request) -> Store:
    return request.app.state.store


def _is_file(store: Store, key: str) -> bool:
    """Return True only if *key* is an actual stored value (not a directory prefix)."""
    fs_key = store._keys.to_fs_key(key)
    try:
        info = store._fs.info(fs_key)
        return info.get("type") != "directory"
    except FileNotFoundError:
        return False


router = APIRouter()


@router.post("/_list")
def iterate_keys(
    prefix: str | None = None,
    exclude_prefix: str | None = None,
    glob: str | None = None,
    store: Store = Depends(get_store),
) -> StreamingResponse:
    data = (
        f"{key}\n".encode()
        for key in store.iterate_keys(
            prefix=prefix, exclude_prefix=exclude_prefix, glob=glob
        )
    )
    return StreamingResponse(data, media_type=DEFAULT)


@router.get("/{key:path}")
def get(
    key: str,
    range: str | None = Header(default=None),
    store: Store = Depends(get_store),
) -> Response:
    if not _is_file(store, key):
        raise FileNotFoundError(key)
    stats = store.info(key)
    if range is not None:
        total = stats.size
        start, end = parse_range(range, total)
        length = end - start + 1

        def _range_iter():
            with store.open(key, "rb") as fh:
                yield from iter_range(fh, start, length)

        return StreamingResponse(
            _range_iter(),
            status_code=206,
            media_type=stats.mimetype,
            headers={
                "Content-Range": f"bytes {start}-{end}/{total}",
                "Content-Length": str(length),
                "Accept-Ranges": "bytes",
            },
        )

    def _full_iter():
        with store.open(key, "rb") as fh:
            yield from iter_chunks(fh)

    return StreamingResponse(
        _full_iter(),
        media_type=stats.mimetype,
        headers={
            "Content-Length": str(stats.size),
            "Accept-Ranges": "bytes",
        },
    )


@router.head("/{key:path}")
def head(
    key: str,
    checksum: bool = False,
    algorithm: str = "sha1",
    store: Store = Depends(get_store),
) -> Response:
    if not _is_file(store, key):
        return Response(status_code=404)
    stats = store.info(key)
    headers: dict[str, str] = {}
    # Standard HTTP headers (for fsspec HTTPFileSystem compatibility)
    headers["Content-Length"] = str(stats.size)
    headers["Content-Type"] = stats.mimetype
    headers["Accept-Ranges"] = "bytes"
    ts = stats.updated_at or stats.created_at
    if ts is not None:
        headers["Last-Modified"] = ts.strftime("%a, %d %b %Y %H:%M:%S GMT")
    # anystore-specific headers
    for field, value in stats.model_dump(mode="json").items():
        if value is None:
            continue
        headers[f"x-anystore-{field.replace('_', '-')}"] = str(value)
    if checksum:
        headers["x-anystore-checksum"] = store.checksum(key, algorithm=algorithm)
    return Response(status_code=200, headers=headers)


@router.put("/{key:path}")
async def put(
    key: str,
    request: Request,
    store: Store = Depends(get_store),
) -> Response:
    with store.open(key, "wb") as fh:
        async for chunk in request.stream():
            fh.write(chunk)
    return Response(status_code=204)


@router.delete("/{key:path}")
def delete(
    key: str,
    store: Store = Depends(get_store),
) -> Response:
    if not store.exists(key):
        return Response(status_code=404)
    store.delete(key)
    return Response(status_code=204)


@router.patch("/{key:path}")
def touch(
    key: str,
    store: Store = Depends(get_store),
) -> Response:
    ts = store.touch(key)
    return Response(ts.isoformat())
