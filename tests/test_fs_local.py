import os
import types

import fsspec
import pytest

from anystore.fs.local import AnyLocalFileSystem
from anystore.store import get_store
from tests.fs_shared import test_exists


@pytest.fixture
def fs():
    return AnyLocalFileSystem(skip_instance_cache=True)


@pytest.fixture
def key(tmp_path):
    return lambda k: str(tmp_path / k)


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


# -- iter_find with depth -----------------------------------------------------


def test_iter_find_depth(tree):
    """`depth` is the most path segments a yielded key may have."""
    fs = AnyLocalFileSystem()

    def names(**kwargs):
        return sorted(
            os.path.relpath(p, tree) for p in fs.iter_find(str(tree), **kwargs)
        )

    assert names(depth=1) == ["a.txt", "b.json"]
    assert names(depth=2) == ["a.txt", "b.json", os.path.join("sub", "c.txt")]
    assert names(depth=3) == names()
    assert len(names(depth=3)) == 5


def test_iter_find_depth_combines_with_glob(tree):
    fs = AnyLocalFileSystem()
    # the glob alone would also match sub/deep/e.json
    assert list(fs.iter_find(str(tree), glob="**/*.json", depth=1)) == [
        str(tree / "b.json")
    ]


def test_iter_find_depth_does_not_descend(tree, monkeypatch):
    """A shallow listing must not walk the deep tree it excludes."""
    fs = AnyLocalFileSystem()
    walked = []
    real_walk = os.walk

    def counting_walk(path, *args, **kwargs):
        for entry in real_walk(path, *args, **kwargs):
            walked.append(entry[0])
            yield entry

    monkeypatch.setattr(os, "walk", counting_walk)
    list(fs.iter_find(str(tree), depth=1))
    assert walked == [str(tree)]


def test_iter_find_depth_single_file(tree):
    fs = AnyLocalFileSystem()
    target = str(tree / "a.txt")
    assert list(fs.iter_find(target, depth=1)) == [target]


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


def test_iterate_keys_depth(local_store):
    assert set(local_store.iterate_keys(depth=1)) == {"a.txt", "b.json"}
    assert set(local_store.iterate_keys(depth=2)) == {"a.txt", "b.json", "sub/c.txt"}
    assert len(set(local_store.iterate_keys(depth=3))) == 5


def test_iterate_keys_depth_is_relative_to_prefix(local_store):
    # "sub/c.txt" is two segments from the store root but one from "sub"
    assert set(local_store.iterate_keys(prefix="sub", depth=1)) == {"sub/c.txt"}
    assert set(local_store.iterate_keys(prefix="sub", depth=2)) == {
        "sub/c.txt",
        "sub/deep/d.txt",
        "sub/deep/e.json",
    }


def test_iterate_keys_depth_and_glob(local_store):
    assert set(local_store.iterate_keys(glob="**/*.json", depth=1)) == {"b.json"}
