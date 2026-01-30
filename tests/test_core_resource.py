from datetime import datetime, timezone
from pathlib import Path

import pytest

from anystore.core.resource import UriResource
from anystore.exceptions import DoesNotExist
from anystore.io import smart_read
from anystore.logic.uri import CURRENT
from anystore.model import Stats
from anystore.store.virtual import VirtualIO

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()


@pytest.fixture
def tmp_uri(tmp_path):
    """Return a base uri inside a temp directory."""
    return str(tmp_path)


def _make(tmp_uri: str, name: str) -> UriResource:
    return UriResource(f"{tmp_uri}/{name}")


def test_core_resource_put_get(tmp_uri):
    r = _make(tmp_uri, "test.txt")
    r.put("hello")
    assert r.get() == "hello"


def test_core_resource_get_nonexist_raises(tmp_uri):
    r = _make(tmp_uri, "missing")
    with pytest.raises(DoesNotExist):
        r.get(raise_on_nonexist=True)


def test_core_resource_get_nonexist_silent(tmp_uri):
    r = _make(tmp_uri, "missing")
    assert r.get(raise_on_nonexist=False) is None


def test_core_resource_exists(tmp_uri):
    r = _make(tmp_uri, "ex")
    assert r.exists() is False
    r.put("data")
    assert r.exists() is True


def test_core_resource_overwrite(tmp_uri):
    r = _make(tmp_uri, "ow")
    r.put("first")
    assert r.get() == "first"
    r.put("second")
    assert r.get() == "second"


def test_core_resource_put_false(tmp_uri):
    r = _make(tmp_uri, "boolval")
    r.put(False)
    assert r.get() is False


def test_core_resource_delete(tmp_uri):
    r = _make(tmp_uri, "del")
    r.put(1)
    assert r.exists()
    r.delete()
    assert not r.exists()


def test_core_resource_delete_ignore_errors(tmp_uri):
    r = _make(tmp_uri, "no_such")
    r.delete(ignore_errors=True)  # should not raise


def test_core_resource_pop(tmp_uri):
    r = _make(tmp_uri, "popped")
    r.put(42)
    assert r.pop() == 42
    assert r.get(raise_on_nonexist=False) is None


def test_core_resource_touch(tmp_uri):
    r = _make(tmp_uri, "touched")
    ts = r.touch()
    assert isinstance(ts, datetime)
    assert r.exists()


def test_core_resource_info(tmp_uri):
    lorem = smart_read(FIXTURES_PATH / "lorem.txt")
    r = _make(tmp_uri, "info_test.pdf")
    r.put(lorem)
    info = r.info()
    assert isinstance(info, Stats)
    assert info.name == "info_test.pdf"
    assert info.key == "info_test.pdf"
    assert info.size == len(lorem)
    if info.created_at is not None:
        assert info.created_at.date() == datetime.now(timezone.utc).date()


def test_core_resource_checksum(tmp_uri):
    lorem = smart_read(FIXTURES_PATH / "lorem.txt")
    r = _make(tmp_uri, "cksum.txt")
    r.put(lorem)
    sha1 = r.checksum()
    assert isinstance(sha1, str)
    assert len(sha1) == 40
    md5 = r.checksum(algorithm="md5")
    assert len(md5) == 32
    assert sha1 != md5


def test_core_resource_open_read_write(tmp_uri):
    r = _make(tmp_uri, "io_test.txt")
    with r.open("wb") as fh:
        fh.write(b"binary data")
    with r.open("rb") as fh:
        assert fh.read() == b"binary data"


def test_core_resource_serialization_func(tmp_uri):
    r = _make(tmp_uri, "seri")
    r.put("HELLO", serialization_func=lambda x: x.lower().encode())
    assert r.get() == "hello"


def test_core_resource_repr_str(tmp_uri):
    r = _make(tmp_uri, "repr")
    assert "UriResource" in repr(r)
    assert tmp_uri in str(r)


def test_core_resource_current():
    r = UriResource("http://localhost:8000")
    assert r.key == CURRENT
    assert r.get().startswith("<!DOCTYPE")


def test_core_resource_local_path(fixtures_path):
    # remote
    r = UriResource("http://localhost:8000/lorem.txt")
    with r.local_path() as p:
        assert p.exists()
        assert p.read_text() == r.get()
    assert not p.exists()

    # local
    r = UriResource(fixtures_path / "lorem.txt")
    with r.local_path() as p:
        assert p.exists()
        assert p.read_text() == r.get()
    # still exists
    assert p.exists()


def test_core_resource_local_open(fixtures_path):
    # remote
    r = UriResource("http://localhost:8000/lorem.txt")
    with r.local_open() as fh:
        assert isinstance(fh, VirtualIO)
        assert fh.path.exists()
        assert len(fh.checksum) == 40
        assert fh.read() == r.get(serialization_mode="raw")
    assert not fh.path.exists()

    # local
    r = UriResource(fixtures_path / "lorem.txt")
    with r.local_open() as fh:
        assert isinstance(fh, VirtualIO)
        assert fh.path.exists()
        assert len(fh.checksum) == 40
        assert fh.read() == r.get(serialization_mode="raw")
    assert fh.path.exists()
