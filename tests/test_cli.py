import orjson
from moto import mock_aws
from typer.testing import CliRunner

from anystore import __version__
from anystore.cli import cli
from anystore.io import smart_write
from anystore.store import get_store
from tests.conftest import setup_s3

runner = CliRunner()


@mock_aws
def test_cli(tmp_path, fixtures_path):
    setup_s3()
    res = runner.invoke(cli, "--help")
    assert res.exit_code == 0

    res = runner.invoke(cli, ["--store", str(tmp_path), "put", "foo", "bar"])
    assert res.exit_code == 0
    res = runner.invoke(cli, ["--store", str(tmp_path), "get", "foo"])
    assert res.exit_code == 0
    assert res.stdout == "bar"

    res = runner.invoke(cli, ["--store", str(tmp_path), "put", "test", "test"])
    res = runner.invoke(cli, ["--store", str(tmp_path), "keys"])
    assert res.exit_code == 0
    assert len(res.stdout.split()) == 2
    res = runner.invoke(cli, ["--store", str(tmp_path), "keys", "--prefix", "foo"])
    assert res.exit_code == 0
    assert len(res.stdout.split()) == 1

    res = runner.invoke(cli, ["--store", "s3://anystore", "put", "foo", "bar"])
    res = runner.invoke(cli, ["--store", "s3://anystore", "get", "foo"])
    assert res.exit_code == 0
    # assert res.stdout == "bar"  # FIXME

    res = runner.invoke(
        cli, ["mirror", "-i", str(fixtures_path), "-o", str(tmp_path / "mirror")]
    )
    assert res.exit_code == 0
    store = get_store(tmp_path / "mirror")
    assert len([k for k in store.iterate_keys()]) == 6

    res = runner.invoke(cli, ["io", "-i", str(fixtures_path / "lorem.txt")])
    assert res.exit_code == 0
    assert res.stdout.startswith("Lorem ")

    res = runner.invoke(cli, "--version")
    assert res.exit_code == 0
    assert res.stdout.strip() == __version__

    smart_write(tmp_path / "data.csv", "foo,bar\na,b", mode="w")
    res = runner.invoke(cli, ["csv2json", "-i", str(tmp_path / "data.csv")])
    assert res.exit_code == 0
    data = orjson.loads(res.stdout.strip())
    assert data == {"foo": "a", "bar": "b"}
