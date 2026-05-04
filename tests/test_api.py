import asyncio
import datetime

import pytest
from fastapi.testclient import TestClient

from anystore.api import create_app
from anystore.store import get_store


@pytest.fixture
def client():
    store = get_store("memory://")
    app = create_app(store=store)
    return TestClient(app)


@pytest.fixture
def populated_client():
    store = get_store("memory://")
    store.put("hello", b"world", serialization_mode="raw")
    store.put("foo/bar", b"baz", serialization_mode="raw")
    store.put("data.json", b'{"a":1}', serialization_mode="raw")
    app = create_app(store=store)
    return TestClient(app)


def test_api_put_and_get(client):
    res = client.put("/mykey", content=b"myvalue")
    assert res.status_code == 201
    res = client.get("/mykey")
    assert res.status_code == 200
    assert res.content == b"myvalue"


def test_api_get_nonexistent(client):
    res = client.get("/nonexistent")
    assert res.status_code == 404


def test_api_delete(client):
    client.put("/todelete", content=b"data")
    res = client.delete("/todelete")
    assert res.status_code == 204
    res = client.get("/todelete")
    assert res.status_code == 404


def test_api_delete_nonexistent(client):
    res = client.delete("/nonexistent")
    assert res.status_code == 404


def test_api_head_exists(populated_client):
    res = populated_client.head("/hello")
    assert res.status_code == 200
    # Standard HTTP headers
    assert res.headers["content-length"] == "5"
    assert res.headers["accept-ranges"] == "bytes"
    # anystore-specific headers
    assert res.headers["x-anystore-name"] == "hello"
    assert res.headers["x-anystore-size"] == "5"
    assert "x-anystore-store" in res.headers
    assert "x-anystore-key" in res.headers
    assert "x-anystore-mimetype" in res.headers
    assert res.headers["content-type"] == res.headers["x-anystore-mimetype"]


def test_api_head_nonexistent(client):
    res = client.head("/nonexistent")
    assert res.status_code == 404


def test_api_head_checksum(populated_client):
    res = populated_client.head("/hello?checksum=true")
    assert res.status_code == 200
    assert "x-anystore-checksum" in res.headers
    assert (
        res.headers["x-anystore-checksum"]
        == "486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7"
    )
    res = populated_client.head("/hello?checksum=true&algorithm=sha1")
    assert res.status_code == 200
    assert "x-anystore-checksum" in res.headers
    assert (
        res.headers["x-anystore-checksum"] == "7c211433f02071597741e6ff5a8ea34789abbf43"
    )


def test_api_list_keys(populated_client):
    res = populated_client.get("/")
    assert res.status_code == 200
    keys = [k for k in res.text.splitlines() if k]
    assert "hello" in keys
    assert "foo/bar" in keys


def test_api_list_keys_prefix(populated_client):
    res = populated_client.get("/foo/")
    assert res.status_code == 200
    keys = [k for k in res.text.splitlines() if k]
    assert "bar" in keys  # raw api returns child paths
    assert "hello" not in keys


def test_api_list_exclude_prefix_qualified():
    """exclude_prefix is interpreted relative to the listed prefix."""
    store = get_store("memory:///")
    store.put("dataset/keep.txt", b"keep", serialization_mode="raw")
    store.put("dataset/temp/skip.txt", b"skip", serialization_mode="raw")
    client = TestClient(create_app(store=store))

    res = client.get("/dataset/", params={"exclude_prefix": "temp/"})
    assert res.status_code == 200
    keys = [k for k in res.text.splitlines() if k]
    assert "keep.txt" in keys
    assert not any("skip.txt" in k for k in keys)


def test_api_pop(populated_client):
    res = populated_client.get("/hello")
    assert res.status_code == 200
    assert res.content == b"world"
    res = populated_client.delete("/hello")
    assert res.status_code == 204
    res = populated_client.get("/hello")
    assert res.status_code == 404


def test_api_touch(client):
    res = client.patch("/touchkey")
    assert res.status_code == 200
    data = res.content.decode()
    assert datetime.datetime.fromisoformat(data)


def test_api_put(client):
    res = client.put("/streamkey", content=b"streamed data")
    assert res.status_code == 201
    res = client.get("/streamkey")
    assert res.status_code == 200
    assert res.content == b"streamed data"


def test_api_get_streamed(populated_client):
    res = populated_client.get("/hello")
    assert res.status_code == 200
    assert res.content == b"world"


def test_api_head_mimetype(populated_client):
    res = populated_client.head("/data.json")
    assert res.status_code == 200
    assert res.headers["x-anystore-mimetype"] == "application/json"
    assert res.headers["content-type"] == "application/json"


def test_api_get_content_type(populated_client):
    res = populated_client.get("/data.json")
    assert res.status_code == 200
    assert res.headers["content-type"] == "application/json"


def test_api_get_range(populated_client):
    res = populated_client.get("/hello", headers={"Range": "bytes=1-3"})
    assert res.status_code == 206
    assert res.content == b"orl"
    assert res.headers["content-range"] == "bytes 1-3/5"


def test_api_get_range_suffix(populated_client):
    res = populated_client.get("/hello", headers={"Range": "bytes=-3"})
    assert res.status_code == 206
    assert res.content == b"rld"


def test_api_get_range_open_end(populated_client):
    res = populated_client.get("/hello", headers={"Range": "bytes=3-"})
    assert res.status_code == 206
    assert res.content == b"ld"


def test_api_get_accept_ranges(populated_client):
    res = populated_client.get("/hello")
    assert res.status_code == 200
    assert res.headers["accept-ranges"] == "bytes"


def test_api_invalid_range_returns_400(populated_client):
    """Malformed Range header → ValueError → 400 via exception handler."""
    res = populated_client.get("/hello", headers={"Range": "weird"})
    assert res.status_code == 400


def _asgi_request(
    app, method: str, raw_path: bytes, body: bytes = b""
) -> tuple[int, bytes]:
    """Drive ASGI directly so we can send paths httpx would normalise away."""
    chunks: list[bytes] = []
    status_holder: dict[str, int] = {}

    async def receive() -> dict:
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(msg: dict) -> None:
        if msg["type"] == "http.response.start":
            status_holder["s"] = msg["status"]
        elif msg["type"] == "http.response.body":
            chunks.append(msg["body"])

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "server": ("test", 80),
        "client": ("127.0.0.1", 0),
        "path": raw_path.decode(),
        "raw_path": raw_path,
        "query_string": b"",
        "headers": [
            (b"host", b"test"),
            (b"content-length", str(len(body)).encode()),
        ],
        "root_path": "",
    }
    asyncio.run(app(scope, receive, send))
    return status_holder["s"], b"".join(chunks)


@pytest.mark.parametrize("method", ["PUT", "GET", "DELETE", "HEAD"])
def test_api_path_traversal_returns_400(method):
    """Path traversal raises ValueError from validate_relative_uri → 400.

    httpx normalises `..` segments client-side, so we drive ASGI directly.
    """
    store = get_store("memory:///")
    app = create_app(store=store)
    body = b"x" if method == "PUT" else b""
    status, _ = _asgi_request(app, method, b"/foo/../bar", body=body)
    assert status == 400
