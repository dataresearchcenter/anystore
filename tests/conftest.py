import subprocess
import sys
import time
from pathlib import Path

import boto3
import pytest
import requests
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


def setup_s3():
    s3 = boto3.resource("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="anystore")
