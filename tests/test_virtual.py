from pathlib import Path

from anystore.store.virtual import get_virtual_path, open_virtual


def test_virtual(fixtures_path):
    uri = "http://localhost:8000/lorem.txt"
    with open_virtual(uri) as fh:
        assert fh.checksum == "ed3141878ed32d8a1d583e7ce7de323118b933d3"
        assert fh.read().startswith(b"Lorem ipsum")
        assert isinstance(fh.path, Path)
        assert str(fh.path).startswith("/tmp")
    assert not fh.path.exists()

    with get_virtual_path(uri) as path:
        assert isinstance(path, Path)
        assert str(path).startswith("/tmp")
    assert not path.exists()

    with open_virtual(uri, checksum=None, keep=True) as fh:
        assert fh.checksum is None
        assert fh.read().startswith(b"Lorem ipsum")
    assert fh.path.exists()

    uri = fixtures_path / "lorem.txt"
    with open_virtual(uri) as fh:
        assert fh.checksum == "ed3141878ed32d8a1d583e7ce7de323118b933d3"
        assert fh.read().startswith(b"Lorem ipsum")
        assert isinstance(fh.path, Path)
        assert str(fh.path).endswith("fixtures/lorem.txt")  # the actual local file
    assert fh.path.exists()

    with get_virtual_path(uri) as path:
        assert isinstance(path, Path)
        assert str(path).endswith("fixtures/lorem.txt")  # the actual local file
    assert path.exists()
