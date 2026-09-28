import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem
from s3fs.core import S3FileSystem

from anystore.fs.redis import RedisFileSystem
from anystore.fs.sql import SqlFileSystem
from anystore.store import get_store
from anystore.store.keys import Keys


def test_core_key_handler():

    # LocalFileSystem
    for uri in (
        "foo",
        "/foo",
        "/tmp/foo/",
        "file:///tmp/foo",
        # "file://foo",
        "~/Data/foo",
    ):
        keys = Keys(uri)
        assert isinstance(keys.fs, LocalFileSystem)
        assert keys.key_prefix.endswith("foo")
        assert keys.to_fs_key("bar").endswith("foo/bar")
        assert keys.to_fs_key("bar").startswith("/")
        assert keys.from_fs_key(keys.to_fs_key("bar")) == "bar"

    # MemoryFileSystem
    for uri in ("memory://foo", "memory:///foo"):
        keys = Keys(uri)
        assert isinstance(keys.fs, MemoryFileSystem)
        assert keys.key_prefix == "foo"
        assert keys.to_fs_key("bar") == "foo/bar"
        assert keys.from_fs_key(keys.to_fs_key("bar")) == "bar"

    # S3FileSystem
    keys = Keys("s3://anystore/foo")
    assert isinstance(keys.fs, S3FileSystem)
    assert keys.to_fs_key("bar") == "anystore/foo/bar"
    assert keys.from_fs_key(keys.to_fs_key("bar")) == "bar"
    assert keys.key_prefix == "anystore/foo"

    # HTTPFileSystem
    keys = Keys("https://anystore/foo")
    assert isinstance(keys.fs, HTTPFileSystem)
    assert keys.to_fs_key("bar") == "https://anystore/foo/bar"
    assert keys.from_fs_key(keys.to_fs_key("bar")) == "bar"
    assert keys.key_prefix == "https://anystore/foo"

    # RedisFileSystem
    keys = Keys("redis://anystore/foo")
    assert isinstance(keys.fs, RedisFileSystem)
    assert keys.to_fs_key("bar") == "foo/bar"
    assert keys.from_fs_key(keys.to_fs_key("bar")) == "bar"
    assert keys.key_prefix == "foo"

    # RedisFileSystem: a numeric first path segment is the db selector,
    # not part of the key prefix
    keys = Keys("redis://localhost/1/foo")
    assert keys.key_prefix == "foo"
    assert keys.to_fs_key("bar") == "foo/bar"
    assert keys.from_fs_key(keys.to_fs_key("bar")) == "bar"

    keys = Keys("redis://localhost/1")
    assert keys.key_prefix == ""
    assert keys.to_fs_key("bar") == "bar"
    assert keys.from_fs_key(keys.to_fs_key("bar")) == "bar"

    # SqlFileSystem
    for uri in (
        "sqlite:///:memory:",
        "sql:///:memory:",
    ):
        keys = Keys(uri)
        assert isinstance(keys.fs, SqlFileSystem)
        assert keys.to_fs_key("bar") == "bar"
        assert keys.from_fs_key(keys.to_fs_key("bar")) == "bar"
        assert keys.key_prefix == ""

    # CURRENT "."
    assert Keys("https://example.org").to_fs_key(".") == "https://example.org"
    assert Keys("/tmp/foo/bar").to_fs_key(".") == "/tmp/foo/bar"


def test_core_key_invalid():
    keys = Keys("foo")
    with pytest.raises(ValueError):
        keys.to_fs_key("")
    with pytest.raises(ValueError):
        keys.to_fs_key("/foo")
    with pytest.raises(ValueError):
        keys.to_fs_key("memory://foo")
    with pytest.raises(ValueError):
        keys.to_fs_key("foo/../bar")


def test_core_key_literal_percent(tmp_path):
    # keys are not unquoted
    for uri in (tmp_path, "memory://foo", "s3://anystore/foo", "redis://localhost"):
        keys = Keys(uri)
        assert keys.to_fs_key("a%20b").endswith("a%20b")
        assert keys.from_fs_key(keys.to_fs_key("a%20b")) == "a%20b"
        assert keys.from_fs_key(keys.to_fs_key("a b")) == "a b"

    # http listings are percent-encoded hrefs
    keys = Keys("http://localhost:8000")
    assert keys.from_fs_key("http://localhost:8000/sub%20dir/a.txt") == "sub dir/a.txt"


def test_core_key_prefix_special_chars(tmp_path):
    # a base path with "#" or "?" is not cut at that character
    for name in ("a#b", "a?b", "a%20b"):
        base = tmp_path / name
        keys = Keys(base)
        assert keys.key_prefix == str(base)
        assert keys.to_fs_key("c") == f"{base}/c"
        store = get_store(base)
        store.put("c", 1)
        assert (base / "c").is_file()
        assert list(store.iterate_keys()) == ["c"]

    # the same for other backends
    assert Keys("s3://anystore/a#b?c").key_prefix == "anystore/a#b?c"
    assert Keys("memory://a#b?c").key_prefix == "a#b?c"
    assert Keys("redis://localhost/1/a#b?c").key_prefix == "a#b?c"
    assert Keys("https://example.org/a%20b").key_prefix == "https://example.org/a%20b"
