"""Shared filesystem tests for all custom AbstractFileSystem subclasses.

Each test function takes a ``fs`` (filesystem instance) and a ``key``
callable that maps a bare name to a backend-appropriate key.
"""

import pytest


def test_pipe_and_cat(fs, key):
    fs.pipe_file(key("hello"), b"world")
    assert fs.cat_file(key("hello")) == b"world"


def test_cat_file_slice(fs, key):
    fs.pipe_file(key("slice"), b"abcdef")
    assert fs.cat_file(key("slice"), start=1, end=4) == b"bcd"


def test_exists(fs, key):
    assert not fs.exists(key("nope"))
    fs.pipe_file(key("yep"), b"1")
    assert fs.exists(key("yep"))


def test_info_file(fs, key):
    fs.pipe_file(key("f.txt"), b"abc")
    info = fs.info(key("f.txt"))
    assert info["size"] == 3
    assert info["type"] == "file"


def test_info_not_found(fs, key):
    with pytest.raises(FileNotFoundError):
        fs.info(key("missing"))


def test_rm_file(fs, key):
    fs.pipe_file(key("k"), b"v")
    assert fs.exists(key("k"))
    fs.rm_file(key("k"))
    assert not fs.exists(key("k"))


def test_open_read(fs, key):
    fs.pipe_file(key("r.txt"), b"data")
    with fs.open(key("r.txt"), "rb") as f:
        assert f.read() == b"data"


def test_open_write(fs, key):
    with fs.open(key("w.txt"), "wb") as f:
        f.write(b"written")
    assert fs.cat_file(key("w.txt")) == b"written"


def test_upsert_overwrites(fs, key):
    fs.pipe_file(key("k"), b"v1")
    fs.pipe_file(key("k"), b"v2")
    assert fs.cat_file(key("k")) == b"v2"


def test_open_read_not_found(fs, key):
    with pytest.raises(FileNotFoundError):
        fs.open(key("nope"), "rb")


def test_info_directory(fs, key):
    fs.pipe_file(key("d/f.txt"), b"x")
    info = fs.info(key("d"))
    assert info["type"] == "directory"


def test_ls_root(fs, key):
    fs.pipe_file(key("a.txt"), b"1")
    fs.pipe_file(key("d/b.txt"), b"2")
    names = sorted(fs.ls(key(""), detail=False))
    assert key("a.txt") in names
    assert key("d") in names


def test_ls_subdir(fs, key):
    fs.pipe_file(key("d/x.txt"), b"1")
    fs.pipe_file(key("d/y.txt"), b"2")
    names = sorted(fs.ls(key("d"), detail=False))
    assert key("d/x.txt") in names
    assert key("d/y.txt") in names


def test_find(fs, key):
    fs.pipe_file(key("dir/a"), b"1")
    fs.pipe_file(key("dir/b"), b"2")
    fs.pipe_file(key("other"), b"3")
    found = fs.find(key("dir"))
    assert key("dir/a") in found
    assert key("dir/b") in found
    assert key("other") not in found


def test_open_seek_read(fs, key):
    fs.pipe_file(key("seekdata"), b"abcdefghij")
    with fs.open(key("seekdata"), "rb") as fh:
        fh.seek(3)
        assert fh.read(4) == b"defg"


def test_open_read_chunks(fs, key):
    fs.pipe_file(key("chunks"), b"0123456789")
    with fs.open(key("chunks"), "rb") as fh:
        assert fh.read(3) == b"012"
        assert fh.read(3) == b"345"
        assert fh.read(3) == b"678"
        assert fh.read(3) == b"9"


def test_cat_file_range(fs, key):
    fs.pipe_file(key("ranged"), b"abcdefghij")
    assert fs.cat_file(key("ranged"), start=2, end=6) == b"cdef"


def test_mkdir_noop(fs, key):
    fs.mkdir("whatever")
    fs.makedirs("a/b/c")
