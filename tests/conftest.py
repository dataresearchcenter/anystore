import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import boto3
import pytest
import requests
from fsspec.implementations.memory import MemoryFileSystem
from moto.server import ThreadedMotoServer

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()


@pytest.fixture(scope="module")
def fixtures_path():
    return FIXTURES_PATH


# https://pawamoy.github.io/posts/local-http-server-fake-files-testing-purposes/
def spawn_and_wait_server():
    process = subprocess.Popen(
        [sys.executable, "-m", "http.server", "-d", FIXTURES_PATH]
    )
    while True:
        try:
            requests.get("http://localhost:8000")
        except Exception:
            time.sleep(1)
        else:
            break
    return process


@pytest.fixture(scope="session", autouse=True)
def http_server():
    process = spawn_and_wait_server()
    yield process
    process.kill()
    process.wait()
    return


# http://docs.getmoto.org/en/latest/docs/server_mode.html
@pytest.fixture(scope="session", autouse=True)
def moto_server():
    """Fixture to run a mocked AWS server for testing."""
    server = ThreadedMotoServer(port=8888)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


PUTFS_HOST = "127.0.0.1"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind((PUTFS_HOST, 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope="module")
def putfs_server(tmp_path_factory):
    """Spawn a real putfs server and yield its base url.

    `putfs.api` reads its `Settings` and resolves `ROOT` at import time, so
    `PUTFS_ROOT` has to be in the child environment - hence a subprocess
    running granian (a non-optional putfs dependency) rather than an
    in-process thread.
    """
    pytest.importorskip("putfs")
    granian = shutil.which("granian") or str(Path(sys.executable).parent / "granian")
    port = _free_port()
    root = tmp_path_factory.mktemp("putfs-root")
    process = subprocess.Popen(
        [granian, "putfs.api:app"],
        env={
            **os.environ,
            "GRANIAN_HOST": PUTFS_HOST,
            "GRANIAN_PORT": str(port),
            "GRANIAN_INTERFACE": "asgi",
            "GRANIAN_WORKERS": "1",
            "PUTFS_ROOT": str(root),
            "PUTFS_UDS": "",  # never inherit a socket bind
        },
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://{PUTFS_HOST}:{port}"
    try:
        for _ in range(100):
            if process.poll() is not None:
                raise RuntimeError(
                    f"putfs server exited at startup (code {process.returncode})"
                )
            try:
                # a listing key answers 405 -- any answer means it is up
                requests.get(url, timeout=1)
                break
            except requests.exceptions.ConnectionError:
                time.sleep(0.1)
        else:
            raise RuntimeError(f"putfs server did not become ready at {url}")
        yield url
    finally:
        process.terminate()
        process.wait(timeout=5)


@pytest.fixture
def supports_ranges() -> bool:
    """Whether the filesystem under test serves http range requests.

    Overridden to `False` by backends that don't -- putfs delegates ranged
    reads to nginx, so its own app ignores the `Range` header.
    """
    return True


@pytest.fixture(autouse=True)
def _clear_memory_fs():
    """Reset the fsspec MemoryFileSystem between tests."""
    yield
    MemoryFileSystem.store.clear()


def setup_s3():
    s3 = boto3.resource("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="anystore")
