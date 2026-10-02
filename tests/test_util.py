import os
import time
from datetime import timedelta
from pathlib import Path, PosixPath
from uuid import uuid4

import pytest
from pydantic import BaseModel

from anystore import smart_read
from anystore.logic.uri import (
    ensure_uri,
    join_relpaths,
    join_uri,
    name_from_uri,
    path_from_uri,
    split_uri,
    uri_to_path,
    validate_relative_uri,
    validate_uri,
)
from anystore.util.checksum import make_checksum, make_data_checksum, make_uri_key
from anystore.util.data import clean_dict, dict_merge, pydantic_merge
from anystore.util.misc import Took, ensure_uuid, format_bytes, mask_uri


def test_util_clean_dict():
    assert clean_dict({}) == {}
    assert clean_dict(None) == {}
    assert clean_dict("") == {}
    assert clean_dict({"a": "b"}) == {"a": "b"}
    assert clean_dict({1: 2}) == {"1": 2}
    assert clean_dict({"a": None}) == {}
    assert clean_dict({"a": ""}) == {}
    assert clean_dict({"a": {1: 2}}) == {"a": {"1": 2}}
    assert clean_dict({"a": {"b": ""}}) == {}
    assert clean_dict({"a": 0}) == {"a": 0}
    assert clean_dict({"a": 0, "b": ""}) == {"a": 0}
    assert clean_dict({"a": 0.0}) == {"a": 0.0}
    assert clean_dict({"a": False}) == {"a": False}
    assert clean_dict({"a": False, "b": 0, "c": None}) == {"a": False, "b": 0}


def test_util_ensure_uri():
    assert ensure_uri("https://example.com") == "https://example.com"
    assert ensure_uri("s3://example.com") == "s3://example.com"
    assert ensure_uri("foo://example.com") == "foo://example.com"
    assert ensure_uri("-") == "-"
    assert ensure_uri("./foo").startswith("file:///")
    assert ensure_uri(Path("./foo")).startswith("file:///")
    assert ensure_uri("/foo") == "file:///foo"
    assert ensure_uri("memory://foo") == "memory://foo"
    assert ensure_uri("sqlite:///foo") == "sqlite:///foo"
    assert ensure_uri("sqlite:////foo") == "sqlite:////foo"
    assert ensure_uri("memory://") == "memory://"

    with pytest.raises(ValueError):
        assert ensure_uri("")
    with pytest.raises(ValueError):
        assert ensure_uri(None)
    with pytest.raises(ValueError):
        assert ensure_uri(" ")


def test_util_uris():
    assert join_uri("http://example.org", "foo") == "http://example.org/foo"
    assert join_uri("http://example.org/", "foo") == "http://example.org/foo"
    assert join_uri("/tmp", "foo") == "file:///tmp/foo"
    assert join_uri("/tmp", Path("foo")) == "file:///tmp/foo"
    assert join_uri(Path("./foo"), "bar").startswith("file:///")
    assert join_uri(Path("./foo"), "bar").endswith("foo/bar")
    assert join_uri("s3://foo/bar", "./baz.txt") == "s3://foo/bar/baz.txt"
    assert join_uri("redis://foo/bar.pdf", "baz.txt") == "redis://foo/bar.pdf/baz.txt"
    assert join_uri("memory://", "bar") == "memory://bar"
    assert join_uri("memory://foo/bar", "/baz.txt") == "memory://foo/bar/baz.txt"
    assert join_uri("/tmp/bar", ".") == "file:///tmp/bar"

    assert join_relpaths("/a/b/c/", "d/e") == "a/b/c/d/e"
    assert join_relpaths("/a/b/c/", ".") == "a/b/c"

    assert path_from_uri("/foo/bar") == PosixPath("/foo/bar")
    assert path_from_uri("file:///foo/bar") == PosixPath("/foo/bar")
    assert path_from_uri("https://foo/bar") == PosixPath("/foo/bar")
    assert path_from_uri("s3://foo") == PosixPath("/foo")

    assert name_from_uri("foo/bar") == "bar"
    assert name_from_uri("s3://foo/bar") == "bar"

    with pytest.raises(ValueError):
        join_uri("/tmp/foo", "../bar")


def test_util_checksum(tmp_path, fixtures_path):
    assert (
        make_data_checksum("stable", algorithm="sha1")
        == "4fbacc2fa0ffdbb11bf1ad6925b886ebd08dd15f"
    )
    assert (
        make_data_checksum("stable")
        == "f379ccb92b9116442dc65bdc35648a85d3786b34779db7f704a901fa07b00cb6"
    )
    assert len(make_data_checksum("a", algorithm="sha1")) == 40
    assert len(make_data_checksum("a")) == 64
    assert len(make_data_checksum({"foo": "bar"})) == 64
    assert len(make_data_checksum(True)) == 64
    assert make_data_checksum(["a", 1]) != make_data_checksum(["a", "1"])

    os.system(f"sha1sum {fixtures_path / 'lorem.txt'} > {tmp_path / 'ch'}")
    sys_ch = smart_read(tmp_path / "ch", mode="r").split()[0]
    with open(fixtures_path / "lorem.txt", "rb") as i:
        ch = make_checksum(i)
    assert ch == "edd1eae3d9703439752a14e742afd563739988fe057ff10843cc61542065e3b1"
    with open(fixtures_path / "lorem.txt", "rb") as i:
        ch = make_checksum(i, algorithm="sha1")
    assert ch == "ed3141878ed32d8a1d583e7ce7de323118b933d3"
    assert sys_ch == ch


def test_util_dict_merge():
    d1 = {"a": 1, "b": 2}
    d2 = {"c": 3}
    assert dict_merge(d1, d2) == {"a": 1, "b": 2, "c": 3}

    d1 = {"a": 1, "b": 2}
    d2 = {"a": 3}
    assert dict_merge(d1, d2) == {"a": 3, "b": 2}

    d1 = {"a": {"b": 1}}
    d2 = {"a": {"c": "e"}}
    assert dict_merge(d1, d2) == {"a": {"b": 1, "c": "e"}}

    d1 = {"a": {"b": 1, "c": 2}, "f": "foo", "g": False}
    d2 = {"a": {"b": 2}, "e": 4, "f": None}
    assert dict_merge(d1, d2) == {
        "a": {"b": 2, "c": 2},
        "e": 4,
        "f": "foo",
        "g": False,
    }

    d1 = {
        "read": {"options": {"skiprows": 1}, "uri": "-", "handler": "read_excel"},
        "operations": [],
        "write": {"options": {"foo": False}, "uri": "-", "handler": None},
    }
    d2 = {
        "read": {"options": {"skiprows": 2}, "uri": "-", "handler": None},
        "operations": [],
        "write": {"options": {}, "uri": "-", "handler": None},
    }
    assert dict_merge(d1, d2) == {
        "read": {"options": {"skiprows": 2}, "uri": "-", "handler": "read_excel"},
        "operations": [],
        "write": {"options": {"foo": False}, "uri": "-"},
    }


def test_util_pydantic_merge():
    class Config(BaseModel):
        name: str
        base_path: str | None = None

    c1 = Config(name="test")
    c2 = Config(name="test", base_path="/tmp/")
    c = pydantic_merge(c1, c2)
    assert str(c.base_path) == "/tmp/"


def test_util_uri_to_path():
    path = Path("/tmp/foo")
    assert uri_to_path("/tmp/foo") == path
    assert uri_to_path("/tmp/a%20b") == Path("/tmp/a%20b")
    assert uri_to_path(Path("/tmp/a%20b")) == Path("/tmp/a%20b")
    assert uri_to_path("file:///tmp/a%20b") == Path("/tmp/a%20b")


@pytest.mark.parametrize("name", ["a%20b", "a%2520b", "a b", "a&b", "a#b", "a?b"])
def test_util_uri_literal_percent(name):
    # local paths keep their names as they are, applying `ensure_uri` twice
    # gives the same result
    path = Path("/data") / name
    for p in (path, str(path)):
        uri = ensure_uri(p)
        assert uri == f"file:///data/{name}"
        assert ensure_uri(uri) == uri
        assert uri_to_path(uri) == path
        assert name_from_uri(uri) == name


def test_util_uri_unquote():
    # uris are never unquoted
    assert ensure_uri("s3://b/a%20b") == "s3://b/a%20b"
    assert ensure_uri("memory://a%20b") == "memory://a%20b"
    assert ensure_uri("https://x/a%20b") == "https://x/a%20b"
    assert ensure_uri("https://x/a b") == "https://x/a b"

    assert validate_uri("a%20b") == "a%20b"
    # whitespace is never stripped
    assert validate_uri(" a/b /c ") == " a/b /c "
    assert validate_relative_uri("a/b ") == "a/b "
    assert ensure_uri("/a/b ") == "file:///a/b "
    for uri in ("", " ", "\n"):
        with pytest.raises(ValueError):
            validate_uri(uri)
    assert validate_relative_uri("a%20b/c%25/") == "a%20b/c%25"
    # only "scheme://" makes a key absolute
    assert validate_relative_uri("note:1") == "note:1"
    with pytest.raises(ValueError):
        validate_relative_uri("s3://b/a")
    # a colon without "://" is a local path
    assert ensure_uri("foo:bar").startswith("file:///")
    assert ensure_uri("foo:bar").endswith("/foo:bar")
    # path traversal, also when encoded
    for uri in ("../x", "..%2Fx", "a/..%2fx", "%2E%2E%2Fx"):
        with pytest.raises(ValueError):
            validate_uri(uri)


def test_util_uri_parent_segment(monkeypatch):
    # only a whole ".." segment is a traversal, dots within a segment are not
    for uri in ("c.../d.txt", "a/b/c.../d.txt", ".../x", "..x/y", "x../y", "a..b"):
        assert validate_uri(uri) == uri
        assert validate_relative_uri(uri) == uri
        assert join_uri("s3://b", uri) == f"s3://b/{uri}"
    for uri in (
        "..",
        "../a",
        "a/..",
        "a/../b",
        "%2E%2E",
        ".%2E",
        "..%2F",
        "a%2F..%2Fb",
    ):
        with pytest.raises(ValueError):
            validate_uri(uri)
        with pytest.raises(ValueError):
            validate_relative_uri(uri)
        with pytest.raises(ValueError):
            join_uri("s3://b", uri)
    # store roots, too
    for uri in ("..", "a/..", "s3://b/..", "https://x/a/../b"):
        with pytest.raises(ValueError):
            ensure_uri(uri)

    # explicit opt-out
    monkeypatch.setattr("anystore.logic.uri.settings.unsafe_uris", True)
    for uri in ("..", "a/../b", "%2E%2E"):
        assert validate_uri(uri) == uri


def test_util_split_uri():
    assert split_uri("s3://bucket/a%20b#c?d") == ("s3", "bucket/a%20b#c?d")
    assert split_uri("file:///tmp/a#b") == ("file", "/tmp/a#b")
    assert split_uri("memory://") == ("memory", "")
    assert split_uri("/tmp/foo") == ("", "/tmp/foo")


def test_util_make_uri_key():
    # ensure stability
    assert (
        make_uri_key("https://example.org/foo/bar#fragment?a=b&c")
        == "example.org/foo/bar/440a5ce5231c534bd3f1e4fbf9d053e3d9c9913b8d5b76199d54ebeace312740"
    )


def test_util_uuid():
    assert isinstance(ensure_uuid(), str)
    uid = str(uuid4())
    assert ensure_uuid(uid) == uid


def test_util_format_bytes():
    assert format_bytes(0) == "0.0B"
    assert format_bytes(512) == "512.0B"
    assert format_bytes(1024) == "1.0KB"
    assert format_bytes(1024 * 1024) == "1.0MB"
    assert format_bytes(1024**5) == "1.0PB"
    assert format_bytes(1024**6) == "1.0EB"
    assert format_bytes(1024 * 1024 * 2.5, "B/s") == "2.5MB/s"


def test_util_took():
    with Took() as t:
        time.sleep(1)
        assert t.took > timedelta(seconds=1)


def test_util_mask_uri():
    uris = [
        (
            "postgresql://user:password@localhost:5432/mydb",
            "postgresql://***:***@localhost:5432/mydb",
        ),
        (
            "mysql://admin:secret@db.example.com:3306/app",
            "mysql://***:***@db.example.com:3306/app",
        ),
        (
            "sqlite://user:pass@/path/to/db.sqlite",
            "sqlite://***:***@/path/to/db.sqlite",
        ),
        (
            "oracle://dbuser:dbpass@oracle-server:1521/xe",
            "oracle://***:***@oracle-server:1521/xe",
        ),
        (
            "mongodb://username:password@mongo.example.com:27017/database",
            "mongodb://***:***@mongo.example.com:27017/database",
        ),
        (
            "redis://user:auth@redis.example.com:6379/0",
            "redis://***:***@redis.example.com:6379/0",
        ),
    ]

    for input_uri, expected_output in uris:
        assert mask_uri(input_uri) == expected_output

    # Test URIs without credentials (should remain unchanged)
    unchanged_uris = [
        "postgresql://localhost:5432/mydb",
        "mysql://db.example.com:3306/app",
        "sqlite:///path/to/db.sqlite",
    ]

    for uri in unchanged_uris:
        assert mask_uri(uri) == uri
