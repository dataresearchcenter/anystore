from datetime import datetime
from io import BytesIO
from pathlib import Path

import pytest
from moto import mock_aws
from pydantic import BaseModel

from anystore.io import (
    ModelWriter,
    SmartHandler,
    Writer,
    _default_serializer,
    logged_items,
    smart_open,
    smart_read,
    smart_stream,
    smart_stream_csv,
    smart_stream_csv_models,
    smart_stream_data,
    smart_stream_json,
    smart_stream_json_models,
    smart_stream_models,
    smart_write,
    smart_write_csv,
    smart_write_data,
    smart_write_json,
    smart_write_model,
    smart_write_models,
    stream_bytes,
)
from anystore.store import get_store
from tests.conftest import setup_s3


def test_io_read(fixtures_path: Path):
    path = fixtures_path / "lorem.txt"
    txt = smart_read(path)
    assert isinstance(txt, bytes)
    assert txt.decode().startswith("Lorem")

    txt = smart_read(path, "r")
    assert isinstance(txt, str)
    assert txt.startswith("Lorem")

    tested = False
    for ix, line in enumerate(smart_stream(path, "r")):
        if ix == 1:
            assert line.startswith("tempor")
            tested = True
            break
    assert tested

    stream = BytesIO(b"hello")
    assert smart_read(stream) == b"hello"


def test_io_write(tmp_path: Path):
    path = tmp_path / "lorem.txt"
    smart_write(path, b"Lorem")
    assert path.exists() and path.is_file()
    assert smart_read(path, "r") == "Lorem"

    out = BytesIO()
    smart_write(out, b"hello")
    assert out.getvalue() == b"hello"


def test_io_write_stdout(capsys):
    smart_write("-", b"hello")
    captured = capsys.readouterr()
    assert captured.out == "hello"


def test_io_smart_open(tmp_path: Path, fixtures_path: Path):
    with smart_open(fixtures_path / "lorem.txt", "r") as f:
        assert f.read().startswith("Lorem")

    with smart_open(tmp_path / "foo.txt", "w") as f:
        f.write("bar")

    assert smart_read(tmp_path / "foo.txt", "r") == "bar"


@mock_aws
def test_io_generic():
    setup_s3()
    uri = "s3://anystore/foo"
    content = b"bar"
    smart_write(uri, content)
    assert smart_read(uri) == content

    url = "http://localhost:8000/lorem.txt"
    content = smart_read(url, mode="r")
    assert content.startswith("Lorem")

    tested = False
    for line in smart_stream(url, "r"):
        assert line.startswith("Lorem")
        tested = True
        break
    assert tested


@mock_aws
def test_io_smart_handler(fixtures_path: Path):
    with SmartHandler(fixtures_path / "lorem.txt") as h:
        line = h.readline()
        assert line.decode().startswith("Lorem")

    setup_s3()
    uri = "s3://anystore/content"
    content = b"foo"
    with SmartHandler(uri, mode="wb") as h:
        h.write(content)

    assert smart_read(uri) == content


def test_io_invalid():
    with pytest.raises(ValueError):
        smart_read("")
    with pytest.raises(ValueError):
        smart_read(None)


def test_io_json(tmp_path):
    data = [{"1": "a"}, {"foo": "foo"}]
    fp = tmp_path / "data.json"
    smart_write_json(fp, data)
    loaded = [d for d in smart_stream_json(fp)]
    assert data == loaded


def test_io_model_writer(tmp_path: Path):
    class MyModel(BaseModel):
        foo: int

    data = [MyModel(foo=1), MyModel(foo=2)]

    with ModelWriter(tmp_path / "data.csv", output_format="csv") as writer:
        for item in data:
            writer.write(item)

    res = [x for x in smart_stream_csv(tmp_path / "data.csv")]
    assert res[0] == {"foo": "1"}
    assert len(res) == 2

    with ModelWriter(tmp_path / "data.json", output_format="json") as writer:
        for item in data:
            writer.write(item)

    res = [x for x in smart_stream_json(tmp_path / "data.json")]
    assert res[0] == {"foo": 1}
    assert len(res) == 2


def test_io_default_serializer():
    dt = datetime(2024, 1, 15, 12, 0)
    assert _default_serializer(dt) == "2024-01-15T12:00:00"
    assert _default_serializer(42) == "42"


def test_io_smart_stream_http_binary():
    url = "http://localhost:8000/lorem.txt"
    line = next(smart_stream(url, "rb"))
    assert isinstance(line, bytes)
    assert line.startswith(b"Lorem")


def test_io_smart_stream_csv_models(tmp_path):
    class Row(BaseModel):
        name: str
        value: int

    fp = tmp_path / "data.csv"
    smart_write_csv(fp, [{"name": "a", "value": "1"}, {"name": "b", "value": "2"}])
    rows = list(smart_stream_csv_models(fp, Row))
    assert len(rows) == 2
    assert rows[0].name == "a"
    assert rows[0].value == 1


def test_io_smart_stream_json_models(tmp_path):
    class Row(BaseModel):
        x: int

    fp = tmp_path / "data.json"
    smart_write_json(fp, [{"x": 1}, {"x": 2}])
    rows = list(smart_stream_json_models(fp, Row))
    assert len(rows) == 2
    assert rows[1].x == 2


def test_io_smart_stream_data(tmp_path):
    fp_csv = tmp_path / "data.csv"
    fp_json = tmp_path / "data.json"
    items = [{"a": "1"}, {"a": "2"}]

    smart_write_csv(fp_csv, items)
    smart_write_json(fp_json, items)

    csv_rows = list(smart_stream_data(fp_csv, "csv"))
    assert len(csv_rows) == 2

    json_rows = list(smart_stream_data(fp_json, "json"))
    assert len(json_rows) == 2


def test_io_smart_stream_models(tmp_path):
    class Item(BaseModel):
        k: str

    fp_csv = tmp_path / "items.csv"
    fp_json = tmp_path / "items.json"

    smart_write_csv(fp_csv, [{"k": "a"}, {"k": "b"}])
    smart_write_json(fp_json, [{"k": "a"}, {"k": "b"}])

    csv_models = list(smart_stream_models(fp_csv, Item, "csv"))
    assert len(csv_models) == 2 and csv_models[0].k == "a"

    json_models = list(smart_stream_models(fp_json, Item, "json"))
    assert len(json_models) == 2 and json_models[0].k == "a"

    with pytest.raises(ValueError):
        list(smart_stream_models(fp_json, Item, "xml"))


def test_io_smart_write_stdout_str(capsys):
    smart_write("-", "hello text")
    captured = capsys.readouterr()
    assert captured.out == "hello text"


def test_io_smart_write_csv(tmp_path):
    fp = tmp_path / "out.csv"
    smart_write_csv(fp, [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}])
    rows = list(smart_stream_csv(fp))
    assert len(rows) == 2
    assert rows[0] == {"a": "1", "b": "2"}


def test_io_smart_write_data(tmp_path):
    items = [{"x": 1}]
    fp_json = tmp_path / "data.json"
    fp_csv = tmp_path / "data.csv"

    smart_write_data(fp_json, items, output_format="json")
    assert list(smart_stream_json(fp_json)) == [{"x": 1}]

    smart_write_data(fp_csv, items, output_format="csv")
    assert list(smart_stream_csv(fp_csv)) == [{"x": "1"}]


def test_io_smart_write_models(tmp_path):
    class M(BaseModel):
        v: int

    fp = tmp_path / "models.json"
    smart_write_models(fp, [M(v=1), M(v=2)])
    assert list(smart_stream_json(fp)) == [{"v": 1}, {"v": 2}]


def test_io_smart_write_model(tmp_path):
    class M(BaseModel):
        v: int

    fp = tmp_path / "single.json"
    smart_write_model(fp, M(v=42))
    assert list(smart_stream_json(fp)) == [{"v": 42}]


def test_io_writer_invalid_format(tmp_path):
    with pytest.raises(ValueError):
        Writer(tmp_path / "bad", output_format="xml")


def test_io_writer_json_clean(tmp_path):
    fp = tmp_path / "clean.json"
    with Writer(fp, output_format="json", clean=True) as w:
        w.write({"a": 1, "b": None})
    rows = list(smart_stream_json(fp))
    assert rows == [{"a": 1}]


def test_io_writer_json_text_mode(tmp_path):
    fp = tmp_path / "text.json"
    with Writer(fp, mode="w", output_format="json") as w:
        w.write({"k": "v"})
    content = smart_read(fp, "r")
    assert '"k"' in content


def test_io_logged_items():
    items = list(range(5))
    result = list(logged_items(items, "Test"))
    assert result == items


def test_io_stream_bytes(tmp_path):
    source = get_store(uri=str(tmp_path / "src"))
    target = get_store(uri=str(tmp_path / "dst"))
    source.put("file.txt", b"content")
    stream_bytes("file.txt", source, target)
    assert target.get("file.txt") == "content"
