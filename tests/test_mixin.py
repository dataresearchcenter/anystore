from pathlib import Path

from anystore.model.base import BaseModel


def test_mixin(fixtures_path: Path):
    class Model(BaseModel):
        foo: str
        baz: str | None = None

    uri = "http://localhost:8000/model.json"
    model = Model.from_json_uri(uri)
    assert model.foo == "bar"
    assert model.baz is None
    assert isinstance(hash(model), int)
    assert len(set([model, model])) == 1

    model = Model._from_uri(uri)
    assert model.foo == "bar"

    uri = "http://localhost:8000/model.yaml"
    model = Model.from_yaml_uri(uri)
    assert model.foo == "bar"
    assert model.baz is None

    model = Model._from_uri(uri)
    assert model.foo == "bar"

    uri = fixtures_path / "model.json"
    model = Model.from_yaml_uri(uri)
    assert model.foo == "bar"
    assert model.baz is None

    p = fixtures_path
    assert (
        Model.from_json_uri(p / "model.json")
        == Model.from_yaml_uri(p / "model.yaml")
        == Model(foo="bar")
    )

    m = Model(foo="bar", baz="")
    assert m.baz is None
