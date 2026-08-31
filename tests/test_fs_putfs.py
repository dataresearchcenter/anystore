"""`putfs://` is provided by the external `putfs` package (extra: `putfs`).

These tests only assert that anystore resolves and drives it like any other
backend -- the filesystem itself is tested upstream.
"""

import fsspec
import pytest

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

PutFSFileSystem = pytest.importorskip("putfs.client.fs").PutFSFileSystem


@pytest.fixture
def fs():
    # `https` defaults to True via PUTFS_HTTPS, but the test server is plain http
    return PutFSFileSystem(https=False)


@pytest.fixture
def key(putfs_server):
    host = putfs_server.split("://", 1)[1]
    return lambda k: f"putfs://{host}/{k}"


@pytest.fixture
def supports_ranges() -> bool:
    # putfs delegates ranged reads to nginx, its own app ignores `Range`
    return False


# -- shared tests (imported above) are collected by pytest automatically --


def test_putfs_registered():
    assert fsspec.get_filesystem_class("putfs") is PutFSFileSystem


def test_iter_find_depth(fs, key):
    """`depth` is a key depth here: never a directory name.

    The wire protocol's `depth=1` mixes directory names into the listing (that
    is what `ls` wants), so putfs asks for `0` instead. Without that,
    `iterate_values(depth=1)` would call `get()` on a directory.
    """
    fs.pipe_file(key("dep/a.txt"), b"1")
    fs.pipe_file(key("dep/sub/b.txt"), b"2")
    fs.pipe_file(key("dep/sub/deeper/c.txt"), b"3")
    base = key("dep")

    def names(**kwargs):
        return sorted(k[len(base) :].lstrip("/") for k in fs.iter_find(base, **kwargs))

    assert names() == ["a.txt", "sub/b.txt", "sub/deeper/c.txt"]
    assert names(depth=1) == ["a.txt"]  # not "sub"
    assert names(depth=2) == ["a.txt", "sub/b.txt"]
    assert names(depth=3) == names()
