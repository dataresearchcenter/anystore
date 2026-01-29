import fsspec
import pytest

from anystore.fs.redis import RedisFileSystem


@pytest.fixture
def fs():
    # during pytest, REDIS_DEBUG=1
    return RedisFileSystem("redis://fake", skip_instance_cache=True)


def test_fs_redis_pipe_and_cat(fs):
    fs.pipe_file("key.txt", b"hello")
    assert fs.cat_file("key.txt") == b"hello"


def test_fs_redis_cat_slice(fs):
    fs.pipe_file("key.txt", b"hello world")
    assert fs.cat_file("key.txt", start=6) == b"world"
    assert fs.cat_file("key.txt", end=5) == b"hello"


def test_fs_redis_exists(fs):
    assert not fs.exists("nope")
    fs.pipe_file("yes", b"1")
    assert fs.exists("yes")


def test_fs_redis_info_file(fs):
    fs.pipe_file("f.txt", b"abc")
    info = fs.info("f.txt")
    assert info["name"] == "f.txt"
    assert info["type"] == "file"
    assert info["size"] == 3


def test_fs_redis_info_directory(fs):
    fs.pipe_file("d/f.txt", b"x")
    info = fs.info("d")
    assert info["type"] == "directory"


def test_fs_redis_info_not_found(fs):
    with pytest.raises(FileNotFoundError):
        fs.info("missing")


def test_fs_redis_ls_root(fs):
    fs.pipe_file("a.txt", b"1")
    fs.pipe_file("d/b.txt", b"2")
    names = sorted(fs.ls("", detail=False))
    assert names == ["a.txt", "d"]


def test_fs_redis_ls_subdir(fs):
    fs.pipe_file("d/x.txt", b"1")
    fs.pipe_file("d/y.txt", b"2")
    names = sorted(fs.ls("d", detail=False))
    assert names == ["d/x.txt", "d/y.txt"]


def test_fs_redis_rm_file(fs):
    fs.pipe_file("k", b"v")
    assert fs.exists("k")
    fs.rm_file("k")
    assert not fs.exists("k")


def test_fs_redis_open_read(fs):
    fs.pipe_file("r.txt", b"data")
    with fs.open("r.txt", "rb") as f:
        assert f.read() == b"data"


def test_fs_redis_open_read_not_found(fs):
    with pytest.raises(FileNotFoundError):
        fs.open("nope", "rb")


def test_fs_redis_open_write(fs):
    with fs.open("w.txt", "wb") as f:
        f.write(b"written")
    assert fs.cat_file("w.txt") == b"written"


def test_fs_redis_upsert_overwrites(fs):
    fs.pipe_file("k", b"v1")
    fs.pipe_file("k", b"v2")
    assert fs.cat_file("k") == b"v2"


def test_fs_redis_mkdir_noop(fs):
    fs.mkdir("whatever")
    fs.makedirs("a/b/c")


def test_fs_sql_fsspec_init():
    assert isinstance(fsspec.url_to_fs("redis://localhost")[0], RedisFileSystem)
