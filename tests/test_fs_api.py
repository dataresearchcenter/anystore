import threading

import pytest
import uvicorn

from anystore.api import create_app
from anystore.fs.api import ApiFileSystem
from anystore.store import get_store


@pytest.fixture
def api_server():
    """Start a real uvicorn server backed by a memory store, return the base URL."""
    store = get_store("memory://")
    app = create_app(store=store)
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        pass
    host, port = server.servers[0].sockets[0].getsockname()
    base = f"http://{host}:{port}"
    yield base
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def fs(api_server):
    return ApiFileSystem(url=f"anystore+{api_server}")


def _url(base, key):
    return f"{base}/{key}"


def test_pipe_and_cat(fs, api_server):
    fs.pipe_file(_url(api_server, "hello"), b"world")
    assert fs.cat_file(_url(api_server, "hello")) == b"world"


def test_cat_file_slice(fs, api_server):
    fs.pipe_file(_url(api_server, "slice"), b"abcdef")
    assert fs.cat_file(_url(api_server, "slice"), start=1, end=4) == b"bcd"


def test_cat_file_not_found(fs, api_server):
    with pytest.raises(FileNotFoundError):
        fs.cat_file(_url(api_server, "nonexistent"))


def test_open_read(fs, api_server):
    fs.pipe_file(_url(api_server, "readkey"), b"data")
    with fs._open(_url(api_server, "readkey"), "rb") as fh:
        assert fh.read() == b"data"


def test_open_write(fs, api_server):
    with fs._open(_url(api_server, "writekey"), "wb") as fh:
        fh.write(b"written")
    assert fs.cat_file(_url(api_server, "writekey")) == b"written"


def test_rm_file(fs, api_server):
    fs.pipe_file(_url(api_server, "delme"), b"x")
    assert fs.exists(_url(api_server, "delme"))
    fs.rm_file(_url(api_server, "delme"))
    assert not fs.exists(_url(api_server, "delme"))


def test_exists(fs, api_server):
    assert not fs.exists(_url(api_server, "nope"))
    fs.pipe_file(_url(api_server, "yep"), b"1")
    assert fs.exists(_url(api_server, "yep"))


def test_info(fs, api_server):
    fs.pipe_file(_url(api_server, "info_key"), b"hello")
    info = fs.info(_url(api_server, "info_key"))
    assert info["size"] == 5
    assert info["type"] == "file"


def test_info_not_found(fs, api_server):
    with pytest.raises(FileNotFoundError):
        fs.info(_url(api_server, "gone"))


def test_ls(fs, api_server):
    fs.pipe_file(_url(api_server, "a"), b"1")
    fs.pipe_file(_url(api_server, "b"), b"2")
    names = fs.ls(api_server, detail=False)
    assert _url(api_server, "a") in names
    assert _url(api_server, "b") in names


def test_ls_detail(fs, api_server):
    fs.pipe_file(_url(api_server, "d1"), b"x")
    entries = fs.ls(api_server, detail=True)
    assert any(e["name"].endswith("/d1") for e in entries)


def test_find(fs, api_server):
    fs.pipe_file(_url(api_server, "dir/a"), b"1")
    fs.pipe_file(_url(api_server, "dir/b"), b"2")
    fs.pipe_file(_url(api_server, "other"), b"3")
    found = fs.find(_url(api_server, "dir"))
    assert _url(api_server, "dir/a") in found
    assert _url(api_server, "dir/b") in found
    assert _url(api_server, "other") not in found


def test_mkdir_noop(fs):
    fs.mkdir("whatever")
    fs.makedirs("whatever/nested")


def test_open_seek_read(fs, api_server):
    fs.pipe_file(_url(api_server, "seekdata"), b"abcdefghij")
    with fs._open(_url(api_server, "seekdata"), "rb") as fh:
        fh.seek(3)
        assert fh.read(4) == b"defg"


def test_open_read_chunks(fs, api_server):
    fs.pipe_file(_url(api_server, "chunks"), b"0123456789")
    with fs._open(_url(api_server, "chunks"), "rb") as fh:
        assert fh.read(3) == b"012"
        assert fh.read(3) == b"345"
        assert fh.read(3) == b"678"
        assert fh.read(3) == b"9"


def test_cat_file_range_server_side(fs, api_server):
    fs.pipe_file(_url(api_server, "ranged"), b"abcdefghij")
    assert fs.cat_file(_url(api_server, "ranged"), start=2, end=6) == b"cdef"


def test_strip_protocol():
    assert (
        ApiFileSystem._strip_protocol("anystore+http://host:8000/foo/bar")
        == "http://host:8000/foo/bar"
    )
    assert (
        ApiFileSystem._strip_protocol("anystore+https://host/key") == "https://host/key"
    )
    assert ApiFileSystem._strip_protocol("plain/key") == "plain/key"
