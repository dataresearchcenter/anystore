"""Compression as a store IO concern.

The load-bearing properties are *streaming* (no full-payload buffering, no
seekable source required), *handle ownership* (`own` decides how far a close
reaches, and an unclosed codec leaves a truncated frame), and *one funnel* –
`Store.open` applies the codec, so `smart_open` and `Writer` inherit it
rather than each sandwiching their own.
"""

import csv
import io

import pytest

from anystore.io import Writer, smart_open, smart_read
from anystore.logic.compress import CompressKind, binary_mode, open_codec
from anystore.store import get_store

MAGIC = {CompressKind.gz: b"\x1f\x8b", CompressKind.zst: b"\x28\xb5\x2f\xfd"}


class Recording(io.BytesIO):
    """A BytesIO that keeps its bytes readable after close."""

    value: bytes = b""

    def close(self) -> None:
        if not self.closed:
            self.value = self.getvalue()
        super().close()


class NonSeekable(io.RawIOBase):
    """A forward-only source, like an http response body."""

    def __init__(self, data: bytes) -> None:
        self._buffer = io.BytesIO(data)

    def read(self, size: int = -1) -> bytes:
        return self._buffer.read(size)

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_compress_roundtrip(algorithm):
    payload = b'{"id":"jane"}\n' * 1_000
    raw = io.BytesIO()
    with open_codec(raw, algorithm, "wb") as out:
        out.write(payload)
    data = raw.getvalue()
    assert data.startswith(MAGIC[algorithm])
    assert len(data) < len(payload)
    with open_codec(io.BytesIO(data), algorithm, "rb") as fh:
        assert fh.read() == payload


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_compress_streams_without_seeking(algorithm):
    """A frame decompresses off a forward-only source – an http body needs no
    temp file."""
    payload = b"x" * 100_000
    raw = io.BytesIO()
    with open_codec(raw, algorithm, "wb") as out:
        out.write(payload)
    with open_codec(NonSeekable(raw.getvalue()), algorithm, "rb") as fh:
        assert fh.read() == payload


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_compress_unclosed_frame_is_truncated(algorithm):
    """Why closing is mandatory rather than tidy."""
    raw = io.BytesIO()
    out = open_codec(raw, algorithm, "wb")
    out.write(b"payload" * 1000)
    out.flush()
    truncated = raw.getvalue()
    out.close()
    assert len(truncated) < len(raw.getvalue())


def test_compress_none_passes_the_handle_through():
    """No codec means no wrapper at all – so nothing takes ownership of a
    handle this call did not open."""
    raw = io.BytesIO()
    assert open_codec(raw, None, "wb") is raw
    assert open_codec(raw, None, "w") is raw


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_compress_ownership(algorithm):
    """`own` is what decides whether a close reaches the handle underneath."""
    borrowed = io.BytesIO()
    with open_codec(borrowed, algorithm, "wb") as out:
        out.write(b"data")
    assert not borrowed.closed  # the caller opened it, the caller closes it

    owned = io.BytesIO()
    with open_codec(owned, algorithm, "wb", own=True) as out:
        out.write(b"data")
    assert owned.closed


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_compress_text_mode_writes_the_frame_first(algorithm):
    """A text stream is a decoder *over* the codec, so closing it flushes the
    text buffer, then the frame, then the handle – in that order."""
    owned = Recording()
    with open_codec(owned, algorithm, "w", own=True) as out:
        writer = csv.DictWriter(out, ["a", "b"])
        writer.writeheader()
        writer.writerow({"a": "x", "b": "multi\nline"})
    assert owned.closed
    with open_codec(io.BytesIO(owned.value), algorithm, "r") as fh:
        rows = list(csv.DictReader(fh))
    # newline="" is load-bearing: the newline inside the quoted field survives
    assert rows == [{"a": "x", "b": "multi\nline"}]


def test_binary_mode():
    assert binary_mode("r") == "rb"
    assert binary_mode("rb") == "rb"
    assert binary_mode("w") == "wb"
    assert binary_mode("wb") == "wb"
    assert binary_mode("a") == "ab"
    assert binary_mode(None) == "rb"


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_store_open_compression(tmp_path, algorithm):
    """The funnel: one kwarg on `Store.open`, and the bytes on disk are a
    frame while the caller only ever saw the payload."""
    store = get_store(str(tmp_path))
    with store.open(f"out.json.{algorithm}", "wb", compression=algorithm) as fh:
        fh.write(b'{"id":"jane"}\n')
    with store.open(f"out.json.{algorithm}", "rb") as fh:
        assert fh.read().startswith(MAGIC[algorithm])
    with store.open(f"out.json.{algorithm}", "rb", compression=algorithm) as fh:
        assert fh.read() == b'{"id":"jane"}\n'


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_store_open_compression_text(tmp_path, algorithm):
    store = get_store(str(tmp_path))
    with store.open("out.csv", "w", compression=algorithm) as fh:
        fh.write("a,b\n1,2\n")
    with store.open("out.csv", "r", compression=algorithm) as fh:
        assert list(csv.DictReader(fh)) == [{"a": "1", "b": "2"}]


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_smart_open_compression(tmp_path, algorithm):
    """`smart_open` inherits it from `Store.open` – no second implementation,
    so the stream is encoded exactly once."""
    uri = str(tmp_path / "data.txt")
    with smart_open(uri, "wb", compression=algorithm) as fh:
        fh.write(b"hello")
    assert smart_read(uri).startswith(MAGIC[algorithm])
    with smart_open(uri, "rb", compression=algorithm) as fh:
        assert fh.read() == b"hello"


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_smart_open_compression_injected_handle(algorithm):
    """A handle the caller passed in stays the caller's to close, but the
    codec over it is still flushed."""
    handle = io.BytesIO()
    with smart_open(handle, "wb", compression=algorithm) as fh:
        fh.write(b"hello")
    assert not handle.closed
    assert handle.getvalue().startswith(MAGIC[algorithm])


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_writer_compression(tmp_path, algorithm):
    uri = str(tmp_path / "rows.json")
    with Writer(uri, output_format="json", compression=algorithm) as writer:
        writer.write({"id": "jane"})
    assert smart_read(uri).startswith(MAGIC[algorithm])
    with smart_open(uri, "rb", compression=algorithm) as fh:
        assert fh.read() == b'{"id":"jane"}\n'


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_writer_compression_csv(tmp_path, algorithm):
    uri = str(tmp_path / "rows.csv")
    with Writer(uri, output_format="csv", compression=algorithm) as writer:
        writer.write({"a": "1", "b": "2"})
    with smart_open(uri, "r", compression=algorithm) as fh:
        assert list(csv.DictReader(fh)) == [{"a": "1", "b": "2"}]


def test_writer_lazy(tmp_path):
    """Lazy defers the target's creation to the first row, so a run that
    writes nothing leaves nothing behind."""
    store = get_store(str(tmp_path))

    with Writer(str(tmp_path / "empty.csv"), output_format="csv", lazy=True):
        pass
    assert not store.exists("empty.csv")

    with Writer(str(tmp_path / "rows.csv"), output_format="csv", lazy=True) as writer:
        writer.write({"a": "1"})
    assert store.exists("rows.csv")

    # eager is the default: a header-only csv is a legitimate thing to want
    with Writer(str(tmp_path / "header.csv"), output_format="csv"):
        pass
    assert store.exists("header.csv")


def test_writer_without_a_context_manager(tmp_path):
    """`open` / `close` are public, so a writer can outlive one block – which
    is what a long-running export needs."""
    store = get_store(str(tmp_path))
    writer = Writer(str(tmp_path / "rows.json"), output_format="json")
    assert not store.exists("rows.json")
    writer.open()
    assert store.exists("rows.json")
    writer.write({"id": "jane"})
    writer.close()
    with store.open("rows.json", "rb") as fh:
        assert fh.read() == b'{"id":"jane"}\n'


def test_writer_close_without_open(tmp_path):
    """Closing a writer that never opened is a no-op, not an error."""
    store = get_store(str(tmp_path))
    Writer(str(tmp_path / "never.json"), output_format="json").close()
    assert not store.exists("never.json")
