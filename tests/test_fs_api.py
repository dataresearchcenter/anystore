import threading

import pytest
import uvicorn

from anystore.api import create_app
from anystore.fs.api import ApiFileSystem
from anystore.store import get_store
from tests.fs_shared import (
    test_cat_file_range,
    test_cat_file_slice,
    test_exists,
    test_find,
    test_info_directory,
    test_info_file,
    test_info_not_found,
    test_ls_root,
    test_ls_subdir,
    test_mkdir_noop,
    test_open_read,
    test_open_read_chunks,
    test_open_read_not_found,
    test_open_seek_read,
    test_open_write,
    test_pipe_and_cat,
    test_rm_file,
    test_upsert_overwrites,
)


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


@pytest.fixture
def key(api_server):
    return lambda k: f"{api_server}/{k}"


# -- shared tests (imported above) are collected by pytest automatically --


def test_api_strip_protocol():
    assert (
        ApiFileSystem._strip_protocol("anystore+http://host:8000/foo/bar")
        == "http://host:8000/foo/bar"
    )
    assert (
        ApiFileSystem._strip_protocol("anystore+https://host/key") == "https://host/key"
    )
    assert ApiFileSystem._strip_protocol("plain/key") == "plain/key"
