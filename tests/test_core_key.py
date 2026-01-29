import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem
from s3fs.core import S3FileSystem

from anystore.core.keys import Keys
from anystore.fs.redis import RedisFileSystem
from anystore.fs.sql import SqlFileSystem


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

    # SqlFileSystem
    for uri in (
        "sqlite:///:memory:",
        "sqlite:///:memory:/foo",
        "sqlite:////tmp/foo",
        "sqlite:////tmp/foo.db",
        "sqlite:///data/foo",
        "postgresql://user:password@localhost/data",
        "postgresql://user:password@localhost/data/foo",
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
