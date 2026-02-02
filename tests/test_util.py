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
    uri_to_path,
)
from anystore.util.checksum import make_checksum, make_data_checksum, make_uri_key
from anystore.util.data import clean_dict, dict_merge, pydantic_merge
from anystore.util.misc import Took, ensure_uuid, mask_uri


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
    assert make_data_checksum("stable") == "4fbacc2fa0ffdbb11bf1ad6925b886ebd08dd15f"
    assert len(make_data_checksum("a")) == 40
    assert len(make_data_checksum({"foo": "bar"})) == 40
    assert len(make_data_checksum(True)) == 40
    assert make_data_checksum(["a", 1]) != make_data_checksum(["a", "1"])

    os.system(f"sha1sum {fixtures_path / 'lorem.txt'} > {tmp_path / 'ch'}")
    sys_ch = smart_read(tmp_path / "ch", mode="r").split()[0]
    with open(fixtures_path / "lorem.txt", "rb") as i:
        ch = make_checksum(i)
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


def test_util_make_uri_key():
    # ensure stability
    assert (
        make_uri_key("https://example.org/foo/bar#fragment?a=b&c")
        == "example.org/foo/bar/ecdb319854a7b223d72e819949ed37328fe034a0"
    )


def test_util_uuid():
    assert isinstance(ensure_uuid(), str)
    uid = str(uuid4())
    assert ensure_uuid(uid) == uid


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
