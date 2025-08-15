import contextlib
from datetime import datetime
from typing import Generator

from anystore.functools import weakref_cache as cache
from anystore.store import get_store
from anystore.store.base import BaseStore
from anystore.types import Uri


class Tags:
    """
    A simple tag implementation inspired by `servicelayer.tags` database

    https://github.com/alephdata/servicelayer/blob/main/servicelayer/tags.py

    It inherites the `get` and `put` methods from the base store and adds
    deletion by key filter criterion and a `touch` contextmanager.
    """

    def __init__(self, store: BaseStore) -> None:
        self.store = store
        self.get = store.get
        self.put = store.put
        self.pop = store.pop
        self.exists = store.exists
        self.iterate_keys = store.iterate_keys
        self.iterate_values = store.iterate_values

    def delete(
        self,
        prefix: str | None = None,
        exclude_prefix: str | None = None,
        glob: str | None = None,
        ignore_errors: bool | None = False,
    ) -> None:
        """
        Delete values by given key criteria.

        Example:
            ```python
            tags.delete(prefix="runs/task1")
            ```

        Args:
            prefix: Include only keys with the given prefix (e.g. "foo/bar")
            exclude_prefix: Exclude keys with this prefix
            glob: Path-style glob pattern for keys to filter (e.g. "foo/**/*.json")
            ignore_errors: Ignore exceptions if deletion fails

        """
        for key in self.store.iterate_keys(
            prefix=prefix, exclude_prefix=exclude_prefix, glob=glob
        ):
            self.store.delete(key, ignore_errors)

    @contextlib.contextmanager
    def touch(self, key: Uri) -> Generator[datetime, None, None]:
        """
        Store the timestamp for a given tag but only at context leave.

        This is useful to tag a long running thing only after succeed but with a
        timestamp from when the action started.

        Example:
            ```python
            with tags.touch("run/1/succeed") as now:
                log.info(now)
                long_running_action_that_might_fail()
                # if it succeeds, the timestamp from the start (`now`) will be stored
            ```
        """
        now = datetime.now()
        try:
            yield now
        except Exception as e:
            raise e
        else:
            self.store.put(key, now)


@cache
def get_tags(*args, **kwargs) -> Tags:
    kwargs["raise_on_nonexist"] = False
    store = get_store(*args, **kwargs)
    return Tags(store)
