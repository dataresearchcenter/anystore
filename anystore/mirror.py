"""
Mirror one store to another
"""

from anystore.logging import get_logger
from anystore.store.base import BaseStore

log = get_logger(__name__)


def mirror(
    source: BaseStore,
    target: BaseStore,
    glob: str | None = None,
    prefix: str | None = None,
    exclude_prefix: str | None = None,
    overwrite: bool = False,
    **kwargs,
) -> None:
    for key in source.iterate_keys(
        prefix=prefix, exclude_prefix=exclude_prefix, glob=glob
    ):
        if not overwrite and target.exists(key):
            log.info(
                f"Skipping already existing key `{key}` ...",
                source=source.uri,
                target=target.uri,
            )
            continue

        log.info(
            f"Mirroring key `{key}` ...",
            source=source.uri,
            target=target.uri,
        )
        with source.open(key, "rb") as i:
            with target.open(key, "wb") as o:
                o.write(i.read())
