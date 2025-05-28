from anystore.store.virtual import open_virtual


def test_virtual():
    uri = "http://localhost:8000/lorem.txt"
    with open_virtual(uri) as fh:
        assert fh.checksum == "ed3141878ed32d8a1d583e7ce7de323118b933d3"
        assert fh.read().startswith(b"Lorem ipsum")

    assert not fh.path.exists()

    with open_virtual(uri, checksum=None, keep=True) as fh:
        assert fh.checksum is None
        assert fh.read().startswith(b"Lorem ipsum")

    assert fh.path.exists()
