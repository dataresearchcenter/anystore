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

``compression.zstd`` only exists on Python 3.14+, so older interpreters pull
`backports.zstd` – the backport of the very same module, guarded by a
``python_version<'3.14'`` marker – and get byte-identical frames.
"""

from __future__ import annotations

import sys
from enum import StrEnum
from gzip import GzipFile
from io import TextIOWrapper
from typing import IO, Any, cast

from anystore.logic.constants import DEFAULT_MODE

if sys.version_info >= (3, 14):
    from compression.zstd import ZstdFile
else:
    from backports.zstd import ZstdFile


class CompressKind(StrEnum):
    """Codecs anystore can (de-)compress a stream with."""

    gz = "gz"
    zst = "zst"


class _OwningGzipFile(GzipFile):
    """A gzip frame that closes the handle it was opened on."""

    def __init__(self, fh: IO[bytes], mode: str) -> None:
        self._fh = fh
        # mtime=0 keeps the output byte-identical across runs for identical
        # payloads; the header would otherwise embed the current time
        super().__init__(fileobj=fh, mode=mode, mtime=0)

    def close(self) -> None:
        try:
            super().close()
        finally:
            self._fh.close()


class _OwningZstdFile(ZstdFile):
    """A zstd frame that closes the handle it was opened on."""

    def __init__(self, fh: IO[bytes], mode: str) -> None:
        self._fh = fh
        super().__init__(fh, mode)

    def close(self) -> None:
        try:
            super().close()
        finally:
            self._fh.close()


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
        compression: Codec to apply. ``None`` layers nothing, so an
            uncompressed caller runs through the same call without branching.
        mode: The mode of the *returned* stream. Only its text-ness is read –
            ``"r"`` / ``"w"`` hand back ``str``, ``"rb"`` / ``"wb"`` ``bytes``;
            the direction comes from ``fh``.
        own: Close ``fh`` when the returned stream closes. Pass ``True`` when
            you opened it, ``False`` to leave it to whoever did.

    Returns:
        The stream to read or write through. It **must** be closed – use it
        as a context manager – or the codec's trailer is never written. With
        no ``compression`` and a binary ``mode`` this is ``fh`` itself.
    """
    mode = mode or DEFAULT_MODE
    if compression is None:
        # nothing to layer – text-ness with no codec is the underlying
        # `open()`'s own business, and wrapping here would take ownership of a
        # handle this call did not open
        return cast(IO[Any], fh)
    kind = CompressKind(compression)
    direction = binary_mode(mode)
    if kind == CompressKind.zst:
        codec = _OwningZstdFile(fh, direction) if own else ZstdFile(fh, direction)
    elif own:
        codec = _OwningGzipFile(fh, direction)
    else:
        codec = GzipFile(fileobj=fh, mode=direction, mtime=0)
    # both codecs model as BufferedIOBase rather than IO[bytes], although they
    # implement its full surface (read / write / fileno / iteration)
    stream = cast(IO[Any], codec)
    if "b" in mode:
        return stream
    # a TextIOWrapper closes what it wraps, so the codec's trailer is flushed
    # before the handle underneath it goes – and an owning codec then closes
    # that handle too, in the one order that leaves a valid frame
    return TextIOWrapper(stream, encoding="utf-8", newline="")
