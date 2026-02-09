import os
import types

import fsspec
import pytest

from anystore.fs.local import AnyLocalFileSystem
from anystore.store import get_store


def test_fs_local_registration():
    fs, _ = fsspec.url_to_fs("file:///tmp")
    assert isinstance(fs, AnyLocalFileSystem)


@pytest.fixture
def tree(tmp_path):
    """Create a small directory tree for iter_find tests.

    tree/
      a.txt
      b.json
      sub/
        c.txt
        deep/
          d.txt
          e.json
    """
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "b.json").write_text("b")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "c.txt").write_text("c")
    deep = sub / "deep"
    deep.mkdir()
    (deep / "d.txt").write_text("d")
    (deep / "e.json").write_text("e")
    return tmp_path


# -- iter_find basic ----------------------------------------------------------


def test_iter_find_yields_all_files(tree):
    fs = AnyLocalFileSystem()
    results = list(fs.iter_find(str(tree)))
    assert len(results) == 5
    assert all(os.path.isabs(p) for p in results)


def test_iter_find_is_generator(tree):
    fs = AnyLocalFileSystem()
    gen = fs.iter_find(str(tree))
    assert isinstance(gen, types.GeneratorType)


def test_iter_find_empty_dir(tmp_path):
    fs = AnyLocalFileSystem()
    assert list(fs.iter_find(str(tmp_path))) == []


def test_iter_find_missing_dir(tmp_path):
    fs = AnyLocalFileSystem()
    assert list(fs.iter_find(str(tmp_path / "nonexistent"))) == []


def test_iter_find_single_file(tree):
    fs = AnyLocalFileSystem()
    target = str(tree / "a.txt")
    assert list(fs.iter_find(target)) == [target]


# -- iter_find with glob ------------------------------------------------------


def test_iter_find_glob_star_dot_txt(tree):
    fs = AnyLocalFileSystem()
    results = set(fs.iter_find(str(tree), glob="*.txt"))
    assert results == {str(tree / "a.txt")}


def test_iter_find_glob_doublestar_dot_txt(tree):
    fs = AnyLocalFileSystem()
    results = set(fs.iter_find(str(tree), glob="**/*.txt"))
    expected = {
        str(tree / "a.txt"),
        str(tree / "sub" / "c.txt"),
        str(tree / "sub" / "deep" / "d.txt"),
    }
    assert results == expected


def test_iter_find_glob_subdir_pattern(tree):
    fs = AnyLocalFileSystem()
    results = set(fs.iter_find(str(tree), glob="sub/*"))
    assert results == {str(tree / "sub" / "c.txt")}


def test_iter_find_glob_doublestar_name(tree):
    fs = AnyLocalFileSystem()
    results = set(fs.iter_find(str(tree), glob="**/e.json"))
    assert results == {str(tree / "sub" / "deep" / "e.json")}


def test_iter_find_glob_no_match(tree):
    fs = AnyLocalFileSystem()
    assert list(fs.iter_find(str(tree), glob="*.xml")) == []


def test_iter_find_glob_on_single_file(tree):
    fs = AnyLocalFileSystem()
    target = str(tree / "a.txt")
    assert list(fs.iter_find(target, glob="*.txt")) == [target]
    assert list(fs.iter_find(target, glob="*.json")) == []


# -- iterate_keys integration -------------------------------------------------


@pytest.fixture
def local_store(tree):
    return get_store(str(tree))


def test_iterate_keys_all(local_store):
    keys = set(local_store.iterate_keys())
    assert len(keys) == 5
    assert "a.txt" in keys
    assert "sub/c.txt" in keys


def test_iterate_keys_prefix(local_store):
    keys = set(local_store.iterate_keys(prefix="sub"))
    assert keys == {"sub/c.txt", "sub/deep/d.txt", "sub/deep/e.json"}


def test_iterate_keys_exclude_prefix(local_store):
    keys = set(local_store.iterate_keys(exclude_prefix="sub"))
    assert keys == {"a.txt", "b.json"}


def test_iterate_keys_glob(local_store):
    keys = set(local_store.iterate_keys(glob="**/*.json"))
    assert keys == {"b.json", "sub/deep/e.json"}


def test_iterate_keys_prefix_and_glob(local_store):
    keys = set(local_store.iterate_keys(prefix="sub", glob="**/*.txt"))
    assert keys == {"sub/c.txt", "sub/deep/d.txt"}


def test_iterate_keys_nonexistent_prefix(local_store):
    keys = list(local_store.iterate_keys(prefix="nonexistent"))
    assert keys == []
