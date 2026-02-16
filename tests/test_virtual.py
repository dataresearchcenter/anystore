from pathlib import Path

from anystore.io import open_virtual
from anystore.logic.uri import uri_to_path
from anystore.store.base import Store
from anystore.store.virtual import get_virtual_store


def test_virtual(fixtures_path):
    uri = "http://localhost:8000/lorem.txt"
    with open_virtual(uri) as fh:
        assert (
            fh.checksum
            == "edd1eae3d9703439752a14e742afd563739988fe057ff10843cc61542065e3b1"
        )
        assert fh.read().startswith(b"Lorem ipsum")
        assert isinstance(fh.path, Path)
        assert str(fh.path).startswith("/tmp")
        assert fh.path.exists()
    assert not fh.path.exists()

    with get_virtual_store() as store:
        assert isinstance(store, Store)
        path = uri_to_path(store.uri)
        assert path.exists()
        assert str(path).startswith("/tmp")
    assert not path.exists()

    # actual local file
    uri = fixtures_path / "lorem.txt"
    with open_virtual(uri) as fh:
        assert (
            fh.checksum
            == "edd1eae3d9703439752a14e742afd563739988fe057ff10843cc61542065e3b1"
        )
        assert fh.read().startswith(b"Lorem ipsum")
        assert isinstance(fh.path, Path)
        assert fh.path.exists()
        assert str(fh.path).endswith("fixtures/lorem.txt")
    assert fh.path.exists()
