import pytest

from anystore.logic import serialize
from anystore.model.base import BaseModel


def test_serialize():
    # mode: auto
    assert serialize.from_store(serialize.to_store("hello")) == "hello"
    assert serialize.from_store(serialize.to_store(b"hello")) == "hello"
    assert serialize.from_store(serialize.to_store(1)) == 1
    assert serialize.from_store(serialize.to_store(1.1)) == 1.1
    assert serialize.from_store(serialize.to_store(None)) is None
    assert serialize.from_store(serialize.to_store(True)) is True
    assert serialize.from_store(serialize.to_store(False)) is False
    assert serialize.from_store(serialize.to_store("")) == ""

    assert serialize.from_store(serialize.to_store({"a": 1})) == {"a": 1}
    assert serialize.from_store(serialize.to_store({"a": "1"})) == {"a": "1"}
    assert serialize.from_store(serialize.to_store({"a": None})) == {"a": None}

    func = lambda x: x
    func2 = serialize.from_store(serialize.to_store(func))
    assert func(1) == func2(1)

    class Model(BaseModel):
        foo: str

    # in auto mode, pydantic models are serialized to json
    model = Model(foo="bar")
    assert serialize.from_store(serialize.to_store(model)) == {"foo": "bar"}

    # pass model to actually serialize to model instance:
    assert (
        serialize.from_store(serialize.to_store(model, model=Model), model=Model)
        == model
    )

    # mode: pickle
    assert (
        serialize.from_store(
            serialize.to_store(model, serialization_mode="pickle"),
            serialization_mode="pickle",
        )
        == model
    )

    # mode: json
    assert serialize.from_store(
        serialize.to_store({"a": 1}, serialization_mode="json"),
        serialization_mode="json",
    ) == {"a": 1}
    assert serialize.from_store(
        serialize.to_store({"a": "1"}, serialization_mode="json"),
        serialization_mode="json",
    ) == {"a": "1"}
    assert serialize.from_store(
        serialize.to_store({"a": None}, serialization_mode="json"),
        serialization_mode="json",
    ) == {"a": None}

    with pytest.raises(TypeError):
        serialize.to_store(func, serialization_mode="json")

    # mode: raw
    assert serialize.from_store(b"str value", serialization_mode="raw") == b"str value"
    # bytes enforced
    with pytest.raises(ValueError):
        serialize.to_store("str value", serialization_mode="raw")

    # explicit de-/serialization
    assert (
        serialize.to_store("value", serialization_func=lambda x: x.encode()) == b"value"
    )
    assert serialize.from_store(b"1", deserialization_func=int) == 1
