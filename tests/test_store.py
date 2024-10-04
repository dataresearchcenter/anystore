from datetime import datetime
import os
import time
from moto import mock_aws
import pytest

from anystore.exceptions import DoesNotExist, WriteError
from anystore.io import smart_read
from anystore.store import Store, get_store, get_store_for_uri
from anystore.store.base import BaseStore
from anystore.store.memory import MemoryStore
from anystore.store.redis import RedisStore
from anystore.store.sql import SqlStore
from anystore.store.virtual import get_virtual, open_virtual
from anystore.store.zip import ZipStore
from anystore.util import DEFAULT_HASH_ALGORITHM
from tests.conftest import setup_s3


def _test_store(fixtures_path, uri: str, can_delete: bool | None = True) -> bool:
    # generic store test
    store = get_store(uri=uri)
    assert isinstance(store, BaseStore)
    key = "test"
    store.put(key, "foo")
    assert store.get(key) == "foo"
    assert store.get(key, mode="r") == "foo"

    store.put("seri", "HELLO", serialization_func=lambda x: x.lower().encode())
    assert store.get("seri") == "hello"

    # overwrite
    if can_delete:
        store.put(key, False)
        assert store.get(key) is False

    store.put("other", None)
    assert store.get("other") is None
    store.put("foo/bar/baz", 1)
    assert store.get("foo/bar/baz") == 1
    assert store.exists("foo/bar/baz") is True

    # touch
    store.touch("touched")
    assert store.exists("touched")

    # non existing key
    with pytest.raises(DoesNotExist):
        store.get("nothing")
    assert store.get("nothing", raise_on_nonexist=False) is None
    assert store.exists("nothing") is False

    # iterate
    keys = [k for k in store.iterate_keys()]
    assert len(keys) == 5
    assert all(store.exists(k) for k in keys)
    keys = [k for k in store.iterate_keys("foo")]
    assert keys[0] == "foo/bar/baz"
    assert len(keys) == 1
    keys = [k for k in store.iterate_keys("foo/bar")]
    assert len(keys) == 1
    assert keys[0] == "foo/bar/baz"
    # exclude prefix
    # keys = [k for k in store.iterate_keys(exclude_prefix="test")]
    # assert len(keys) == 4
    # assert "foo/bar/baz" in keys
    # keys = [k for k in store.iterate_keys(exclude_prefix="foo/bar")]
    # assert len(keys) == 4
    # keys = [k for k in store.iterate_keys(prefix="foo", exclude_prefix="foo/bar")]
    # assert len(keys) == 0
    # # glob
    # keys = [k for k in store.iterate_keys(glob="*/bar/*")]
    # assert len(keys) == 1
    # assert keys[0] == "foo/bar/baz"
    # keys = [k for k in store.iterate_keys(glob="**/baz")]
    # assert len(keys) == 1
    # assert keys[0] == "foo/bar/baz"
    # keys = [k for k in store.iterate_keys(prefix="foo", glob="**/baz")]
    # assert len(keys) == 1
    # assert keys[0] == "foo/bar/baz"

    if can_delete:
        # pop
        store.put("popped", 1)
        assert store.pop("popped") == 1
        assert store.get("popped", raise_on_nonexist=False) is None

        # delete
        store.put("to_delete", 1)
        assert store.exists("to_delete")
        store.delete("to_delete")
        assert not store.exists("to_delete")
        assert (
            store.pop("seri", deserialization_func=lambda x: x.decode().upper())
            == "HELLO"
        )

    # ttl
    if isinstance(store, (RedisStore, SqlStore, MemoryStore)):
        store.put("expired", 1, ttl=1)
        assert store.get("expired") == 1
        time.sleep(1)
        assert store.get("expired", raise_on_nonexist=False) is None

    # checksum
    assert DEFAULT_HASH_ALGORITHM == "sha1"
    md5sum = "6d484beb4162b026abc7cfea019acbd1"
    sha1sum = "ed3141878ed32d8a1d583e7ce7de323118b933d3"
    lorem = smart_read(fixtures_path / "lorem.txt")
    store.put("lorem", lorem)
    assert store.checksum("lorem") == sha1sum
    assert store.checksum("lorem", "md5") == md5sum

    # info (stats)
    store.put("lorem2/ipsum.pdf", lorem)
    info = store.info("lorem2/ipsum.pdf")
    assert info.name == "ipsum.pdf"
    assert info.store == store.uri
    assert info.key == "lorem2/ipsum.pdf"
    assert info.path == "/".join((store.prefix, info.key)) if store.prefix else info.key
    assert info.size == 296
    if not store.is_sql:
        assert info.uri.startswith(store.uri)

    # FIXME sql / s3 timezone!
    if info.created_at is not None:
        assert info.created_at.date() == datetime.now().date()
    if info.updated_at is not None:
        assert info.updated_at.date() == datetime.now().date()

    # path-like inheritance
    new_store = store / "child-path"
    new_store.put("foo", "bar")
    assert store.get("child-path/foo") == "bar"

    # streaming io
    lorem = smart_read(fixtures_path / "lorem.txt", mode="r")
    with store.open("lorem.txt", "wb") as o:
        with open(fixtures_path / "lorem.txt", "rb") as i:
            o.write(i.read())
    assert store.get("lorem.txt") == lorem
    with store.open("lorem.txt", "r") as i:
        assert i.read() == lorem
    with store.open("lorem.txt", "rb") as i:
        assert i.read() == lorem.encode()
    with store.open("lorem2.txt", "w") as o:
        o.write(lorem)
    assert store.get("lorem2.txt") == lorem
    tested = False
    for ix, line in enumerate(store.stream("lorem.txt", mode="r")):
        if ix == 1:
            assert line.startswith("tempor")
            tested = True
            break
    assert tested

    return True


@mock_aws
def test_store_s3(fixtures_path):
    setup_s3()
    assert _test_store(fixtures_path, "s3://anystore")


def test_store_redis(fixtures_path):
    assert _test_store(fixtures_path, "redis://localhost")


def test_store_sql(fixtures_path, tmp_path):
    assert _test_store(fixtures_path, f"sqlite:///{tmp_path}/db.sqlite")


def test_store_memory(fixtures_path):
    assert _test_store(fixtures_path, "memory://")


def test_store_zip(tmp_path, fixtures_path):
    assert _test_store(fixtures_path, tmp_path / "store.zip", can_delete=False)


def test_store_fs(tmp_path, fixtures_path):
    assert _test_store(fixtures_path, tmp_path)

    # don't pickle "external" data
    store = Store(uri=fixtures_path)
    content = store.get("lorem.txt", mode="r")
    assert content.startswith("Lorem")

    # put into not yet existing sub paths
    store = Store(uri=tmp_path / "foo", raise_on_nonexist=False)
    store.put("/bar/baz", 1)
    assert (tmp_path / "foo/bar/baz").exists()
    assert store.get("/bar/baz") == 1


def test_store_initialize(tmp_path, fixtures_path):
    # initialize (take env vars into account)
    get_store.cache_clear()
    store = get_store()
    assert store.uri == "s3://anystore/another-store"
    assert get_store(uri="foo").uri.endswith("foo")

    store = Store.from_json_uri(fixtures_path / "store.json")
    assert store.uri == "file:///tmp/cache"
    assert store.is_local

    store = Store(uri="s3://anystore", raise_on_nonexist=False)
    assert store.raise_on_nonexist is False

    # store implementations
    assert isinstance(get_store("memory://"), MemoryStore)
    assert isinstance(get_store(), Store)
    assert isinstance(get_store("./data"), Store)
    assert isinstance(get_store("/data"), Store)
    assert isinstance(get_store("file:///data"), Store)
    assert isinstance(get_store("s3://bucket"), Store)
    # assert isinstance(get_store("gcs://bucket"), Store)
    assert isinstance(get_store("http://example.org/files"), Store)
    assert isinstance(get_store("redis://localhost"), RedisStore)
    assert isinstance(get_store(f"sqlite:///{tmp_path}/db"), SqlStore)
    # assert isinstance(get_store("postgresql:///db"), SqlStore)
    # assert isinstance(get_store("mysql:///db"), SqlStore)
    assert isinstance(get_store("./store.zip"), ZipStore)


def test_store_virtual(fixtures_path):
    lorem = smart_read(fixtures_path / "lorem.txt")
    tmp = get_virtual()
    key = tmp.download(fixtures_path / "lorem.txt")
    assert tmp.store.pop(key) == lorem

    store = get_store(uri=fixtures_path)
    key = tmp.download("lorem.txt", store)
    assert tmp.store.get(key) == lorem

    assert tmp.store.exists(key)
    tmp.cleanup()
    assert not tmp.store.exists(key)

    with get_virtual() as tmp:
        tmp.download(fixtures_path / "lorem.txt")
        path = tmp.path
        assert os.path.exists(tmp.path)
    assert not os.path.exists(path)

    with open_virtual(fixtures_path / "lorem.txt") as i:
        assert i.read().decode().startswith("Lorem")


def test_store_readonly(tmp_path):
    store = get_store(tmp_path / "readonly-store", readonly=True)
    with pytest.raises(WriteError):
        store.put("foo", "bar")
    with pytest.raises(WriteError):
        store.pop("foo")
    with pytest.raises(WriteError):
        store.delete("foo")
    with pytest.raises(WriteError):
        store.open("foo", mode="w")
    with pytest.raises(WriteError):
        store.touch("foo")


def test_store_for_uri(tmp_path):
    store, uri = get_store_for_uri(tmp_path / "foo/bar.txt")
    assert isinstance(store, Store)
    assert store.uri.endswith("foo")
    assert uri == "bar.txt"

    store, uri = get_store_for_uri("http://example.org/foo/bar.txt")
    assert isinstance(store, Store)
    assert store.uri.endswith("foo")
    assert uri == "bar.txt"

    with pytest.raises(NotImplementedError):
        store, uri = get_store_for_uri("memory://foo/bar.txt")
    with pytest.raises(NotImplementedError):
        store, uri = get_store_for_uri("redis://foo/bar.txt")
    with pytest.raises(NotImplementedError):
        store, uri = get_store_for_uri(f"sqlite:///{tmp_path}/db.sqlite/foo/bar.txt")
