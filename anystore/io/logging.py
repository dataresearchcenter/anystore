from __future__ import annotations

import logging
from typing import Generator, Iterable, TypeVar

from structlog.stdlib import BoundLogger

from anystore.io.progress import DEFAULT_COLUMNS, SyncProgressBar, ThroughputColumn
from anystore.logging import get_logger

log = get_logger(__name__)

T = TypeVar("T")

# items carry no byte size, so a throughput column would only ever show 0 B/s
COLUMNS = [c for c in DEFAULT_COLUMNS if not isinstance(c, ThroughputColumn)]


def logged_items(
    items: Iterable[T],
    action: str,
    chunk_size: int | None = 10_000,
    item_name: str | None = None,
    logger: logging.Logger | BoundLogger | None = None,
    total: int | None = None,
    **log_kwargs,
) -> Generator[T, None, None]:
    """
    Log process of iterating items for io operations.

    Example:
        ```python
        from anystore.io import logged_items

        items = [...]
        for item in logged_items(items, "Read", uri="/tmp/foo.csv"):
            yield item
        ```

    Args:
        items: Sequence of any items
        action: Action name to log
        chunk_size: Log on every chunk_size
        item_name: Name of item
        logger: Specific logger to use
        total: Number of items, if known, to render a progress bar instead
            of logging every chunk_size

    Yields:
        The input items
    """
    log_ = logger or log
    chunk_size = chunk_size or 10_000
    ix = 0
    item_name = item_name or "Item"
    if total:
        log_.info(f"{action} {total} `{item_name}s` ...", **log_kwargs)
        with SyncProgressBar(
            f"{action} {item_name}s", total=total, columns=COLUMNS
        ) as bar:
            for item in items:
                yield item
                bar.advance()
        ix = total
    else:
        for ix, item in enumerate(items, 1):
            if ix == 1:
                item_name = item_name or item.__class__.__name__.title()
            if ix % chunk_size == 0:
                item_name = item_name or item.__class__.__name__.title()
                log_.info(f"{action} `{item_name}` {ix} ...", **log_kwargs)
            yield item
    if ix:
        log_.info(f"{action} {ix} `{item_name}s`: Done.", **log_kwargs)
