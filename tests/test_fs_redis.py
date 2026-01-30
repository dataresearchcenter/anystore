import fsspec
import pytest

from anystore.fs.redis import RedisFileSystem
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


@pytest.fixture
def fs():
    # during pytest, REDIS_DEBUG=1
    return RedisFileSystem("redis://fake", skip_instance_cache=True)


@pytest.fixture
def key():
    return lambda k: k


# -- shared tests (imported above) are collected by pytest automatically --


def test_fs_redis_fsspec_init():
    assert isinstance(fsspec.url_to_fs("redis://localhost")[0], RedisFileSystem)
