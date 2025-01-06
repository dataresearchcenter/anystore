import asyncio
from datetime import datetime

import pytest
from pydantic import BaseModel

from anystore.decorators import anycache, async_anycache
from anystore.store import get_store
from anystore.util import make_signature_key


def test_decorator(tmp_path):
    @anycache(uri=tmp_path)
    def get_data(*args, **kwargs):
        return "data"

    store = get_store(uri=tmp_path, raise_on_nonexist=False)
    key = make_signature_key("x")
    assert store.get(key) is None
    assert get_data("x") == "data"
    assert store.get(key) == "data"

    # store as arg, custom key func
    @anycache(store=store, key_func=lambda *args, **kwargs: args[0].upper())
    def get_data2(*args, **kwargs):
        return "data2"

    assert store.get("X") is None
    assert get_data2("x") == "data2"
    assert store.get("X") == "data2"

    # not yet existing store
    @anycache(uri=tmp_path / "foo", key_func=lambda x: x)
    def get_data3(data):
        return data

    assert get_data3("bar") == "bar"
    assert (tmp_path / "foo" / "bar").exists()

    # custom serialize function
    @anycache(uri=tmp_path, serialization_func=list)
    def get_data4(x):
        yield from range(x)

    assert get_data4(5) == [0, 1, 2, 3, 4]
    # now from cache:
    assert get_data4(5) == [0, 1, 2, 3, 4]

    # model
    class Model(BaseModel):
        data: int

    @anycache(uri=tmp_path, model=Model)
    def get_data5(x: int) -> Model:
        return Model(data=x)

    model = get_data5(1)
    cached = get_data5(1)
    assert model.data == cached.data == 1
    assert isinstance(cached, Model)

    @anycache(store=store, model=Model)
    def get_data6(x: int) -> Model:
        return Model(data=x)

    model = get_data6(1)
    cached = get_data6(1)
    assert model.data == cached.data == 1
    assert isinstance(cached, Model)


def test_decorator_no_args(monkeypatch):
    get_store.cache_clear()
    monkeypatch.delenv("ANYSTORE_YAML_URI")

    # without args
    @anycache
    def get_data5(x):
        return x

    assert get_data5(5) == 5
    # now from cache:
    assert get_data5(5) == 5


@pytest.mark.asyncio
async def test_decorator_async(monkeypatch):
    get_store.cache_clear()
    monkeypatch.delenv("ANYSTORE_YAML_URI")

    @async_anycache
    async def get_data6(x):
        await asyncio.sleep(1)
        return x

    assert await get_data6(6) == 6
    # now from cache:
    assert await get_data6(6) == 6


def test_decorator_cache_disabled(monkeypatch):
    get_store.cache_clear()
    monkeypatch.delenv("ANYSTORE_YAML_URI")

    @anycache
    def get_result(x):
        return datetime.now().isoformat()

    res = get_result(1)
    assert res == get_result(1)  # cached

    @anycache(use_cache=False)
    def get_result2(x):
        return datetime.now().isoformat()

    new_res = get_result2(1)
    assert res < new_res  # not cached
    assert new_res == get_result(1)  # but new value now stored in cache

    # same via env:
    monkeypatch.setenv("CACHE", "0")
    new_res = get_result(1)
    assert res < new_res

    monkeypatch.setenv("CACHE", "1")
    assert new_res == get_result(1)
