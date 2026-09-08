"""(De-)compression as a store IO concern, not a caller concern.

A codec belongs *inside* the IO funnel: `Store.open` applies it, so
[`smart_open`][anystore.io.handler.smart_open], [`Writer`][anystore.io.write.Writer],
the `smart_stream_*` helpers and a direct `store.open(key)` all take the same
``compression=`` argument and nobody sandwiches a codec by hand:

```python
from anystore import get_store

store = get_store("s3://bucket")
with store.open("entities.json.zst", "wb", compression="zst") as fh:
    fh.write(b"...")
```

`CompressKind` names what can go in there: `gz`, `bz2`, `xz` and `zst` come
from the standard library, `lz4` from the optional ``lz4`` extra – a missing
one surfaces as an `ImportError` naming the extra, the same way an optional
storage backend does. The spellings other tools use are accepted as aliases,
so a codec read off a pandas or fsspec call (``"gzip"``, ``"zstd"``) or off a
file name (``".xz"``) needs no translation.

The codec is always **binary** – a frame is bytes – so the handle underneath
is opened ``"rb"`` / ``"wb"`` whatever the caller asked for, and a text mode
becomes a `io.TextIOWrapper` layered on *top* of the codec, which is what
``gzip.open()`` does internally. utf-8 is pinned rather than inheriting the
locale, and ``newline=""`` leaves line endings untranslated so newlines
inside quoted csv fields survive the round trip.

**Closing is mandatory.** An unclosed compressor leaves a truncated frame
behind – gzip loses its CRC / size trailer – so the result belongs in a
``with`` block. Ownership decides how far a close reaches: `open_codec`
closes the handle it was given when it opened it (`own`), and leaves it alone
when the caller did, which is what keeps an injected handle the caller's to
close.

The codec is never inferred from the key's extension. A file *named* ``.gz``
that is not gzipped would fail confusingly, and a caller that compresses
always knows it did.

Every codec class is imported on first use, not at module import: `lzma`
needs a liblzma the interpreter may have been built without, and `lz4` may
not be installed at all, neither of which should cost an unrelated caller
anything. ``compression.zstd`` only exists on Python 3.14+, so older
interpreters pull `backports.zstd` – the backport of the very same module,
guarded by a ``python_version<'3.14'`` marker – and get byte-identical
frames.
"""

from __future__ import annotations

import sys
from enum import StrEnum
from functools import cache
from gzip import GzipFile
from io import TextIOWrapper
from typing import IO, Any, Callable, cast

from anystore.logic.constants import DEFAULT_MODE


class CompressKind(StrEnum):
    """Codecs anystore can (de-)compress a stream with."""

    gz = "gz"
    bz2 = "bz2"
    xz = "xz"
    zst = "zst"
    lz4 = "lz4"

    @classmethod
    def _missing_(cls, value: object) -> CompressKind | None:
        """Accept how the rest of the world spells these: `gzip`, `.xz`, `zstd`."""
        if not isinstance(value, str):
            return None
        name = value.strip().lower().lstrip(".")
        member = cls._value2member_map_.get(ALIASES.get(name, name))
        return cast("CompressKind | None", member)


ALIASES = {
    "gzip": "gz",
    "bzip2": "bz2",
    "bz": "bz2",
    "lzma": "xz",
    "zstd": "zst",
    "zstandard": "zst",
    "lz4frame": "lz4",
}


class _GzipFrame(GzipFile):
    """`GzipFile` behind the `(handle, mode)` signature the other codecs share."""

    def __init__(self, fh: IO[bytes], mode: str) -> None:
        # mtime=0 and an empty filename keep the output byte-identical across
        # runs for identical payloads; the header would otherwise embed the
        # current time, and the name gzip reads off the handle it was given –
        # which for a local store is the file being written
        super().__init__(filename="", fileobj=fh, mode=mode, mtime=0)


def _load_gz() -> type[Any]:
    return _GzipFrame


def _load_bz2() -> type[Any]:
    from bz2 import BZ2File

    return BZ2File


def _load_xz() -> type[Any]:
    from lzma import LZMAFile

    return LZMAFile


def _load_zst() -> type[Any]:
    if sys.version_info >= (3, 14):
        from compression.zstd import ZstdFile
    else:
        from backports.zstd import ZstdFile

    return ZstdFile


def _load_lz4() -> type[Any]:
    try:
        from lz4.frame import LZ4FrameFile  # type: ignore[import-untyped]
    except ImportError:
        raise ImportError(
            'lz4 compression requires the "lz4" extra: `pip install anystore[lz4]`'
        ) from None

    return cast("type[Any]", LZ4FrameFile)


CODECS: dict[CompressKind, Callable[[], type[Any]]] = {
    CompressKind.gz: _load_gz,
    CompressKind.bz2: _load_bz2,
    CompressKind.xz: _load_xz,
    CompressKind.zst: _load_zst,
    CompressKind.lz4: _load_lz4,
}


def _owning(codec: type[Any]) -> type[Any]:
    """A codec class whose close reaches the handle it was opened on.

    Built on demand rather than eagerly per codec; `get_codec` memoizes the
    result, so a class is created once.
    """

    class Owning(codec):  # type: ignore[misc]
        def __init__(self, fh: IO[bytes], mode: str) -> None:
            self._source = fh
            super().__init__(fh, mode)

        def close(self) -> None:
            try:
                super().close()
            finally:
                self._source.close()

    Owning.__name__ = Owning.__qualname__ = f"Owning{codec.__name__}"
    return Owning


@cache
def get_codec(kind: CompressKind, own: bool = False) -> type[Any]:
    """The file class for a codec, constructed as ``cls(handle, binary_mode)``.

    Args:
        kind: The codec to get.
        own: Get the variant that closes the handle it was opened on.

    Returns:
        The class to layer over a binary handle.

    Raises:
        ImportError: The codec's optional dependency is not installed.
    """
    codec = CODECS[kind]()
    return _owning(codec) if own else codec


def binary_mode(mode: str | None = DEFAULT_MODE) -> str:
    """The mode a codec's underlying handle is opened in.

    A frame is bytes, so only the *direction* of the caller's mode carries
    over.
    """
    mode = mode or DEFAULT_MODE
    if "a" in mode:
        return "ab"
    if "w" in mode or "x" in mode:
        return "wb"
    return "rb"


def open_codec(
    fh: IO[bytes],
    compression: CompressKind | str | None = None,
    mode: str | None = DEFAULT_MODE,
    own: bool = False,
) -> IO[Any]:
    """Layer a codec – and, for a text mode, a decoder – over an open handle.

    Args:
        fh: Open **binary** handle, positioned at the start of the frame.
            Reading is forward-only, so a non-seekable source (an http
            response body) works.
        compression: Codec to apply, a `CompressKind` or one of its aliases.
            ``None`` layers nothing, so an uncompressed caller runs through
            the same call without branching.
        mode: The mode of the *returned* stream. Only its text-ness is read –
            ``"r"`` / ``"w"`` hand back ``str``, ``"rb"`` / ``"wb"`` ``bytes``;
            the direction comes from ``fh``.
        own: Close ``fh`` when the returned stream closes. Pass ``True`` when
            you opened it, ``False`` to leave it to whoever did.

    Returns:
        The stream to read or write through. It **must** be closed – use it
        as a context manager – or the codec's trailer is never written. With
        no ``compression`` and a binary ``mode`` this is ``fh`` itself.

    Raises:
        ValueError: `compression` doesn't name a codec.
        ImportError: The codec's optional dependency is not installed.
    """
    mode = mode or DEFAULT_MODE
    if compression is None:
        # nothing to layer – text-ness with no codec is the underlying
        # `open()`'s own business, and wrapping here would take ownership of a
        # handle this call did not open
        return cast(IO[Any], fh)
    codec = get_codec(CompressKind(compression), own)
    # the codec classes model as BufferedIOBase rather than IO[bytes], although
    # they implement its full surface (read / write / fileno / iteration)
    stream = cast(IO[Any], codec(fh, binary_mode(mode)))
    if "b" in mode:
        return stream
    # a TextIOWrapper closes what it wraps, so the codec's trailer is flushed
    # before the handle underneath it goes – and an owning codec then closes
    # that handle too, in the one order that leaves a valid frame
    return TextIOWrapper(stream, encoding="utf-8", newline="")
