"""
A live progress display for long-running io jobs.

Where [`logged_items`][anystore.io.logging.logged_items] logs the progress of a
single iterator, this renders one bar per unit of work – a key prefix, a shard,
a file – each labelled, each removed when it completes, and each carrying the
byte throughput that actually says whether a transfer is getting anywhere.

Example:
    ```python
    from anystore import get_store
    from anystore.io import SyncProgressBar
    from anystore.logic.io import stream_bytes
    from anystore.util import format_bytes

    source, target = get_store("s3://from"), get_store("./to")

    # a single bar: describe it up front and advance the display itself
    with SyncProgressBar("copying", total=len(keys)) as bar:
        for key in keys:
            bar.advance(size=stream_bytes(key, source, target))

    # or one bar per unit of concurrent work
    with SyncProgressBar() as bar:
        # a task fed the bar's own throughput sums up all the others
        overall = bar.task(
            "all prefixes", total=len(prefixes), throughput=bar.throughput
        )
        for prefix in prefixes:  # from any number of worker threads
            keys = list(source.iterate_keys(prefix=prefix))
            with bar.task(prefix, total=len(keys)) as task:
                for key in keys:
                    task.advance(size=stream_bytes(key, source, target))
            overall.advance()

    log.info("Done.", moved=format_bytes(bar.throughput.total))
    ```

Note:
    While the display is running, log output is routed through the same rich
    console, which clears the bars before each line and redraws them after –
    otherwise the two write over each other. This works by swapping
    `sys.stderr`, which anystore's log handlers resolve on every emit. Pass
    `route_logging=False` to leave logging alone.
"""

from __future__ import annotations

import sys
import threading
import time
from collections import deque
from contextlib import ExitStack, contextmanager
from typing import Any, Iterator, Self, Sequence

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    ProgressColumn,
    Task,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.text import Text

from anystore.util import format_bytes

DEFAULT_WINDOW = 30


class Throughput:
    """
    Thread-safe byte counter exposing a rolling transfer rate.

    Args:
        window: Seconds to average the rate over
    """

    def __init__(self, window: int | None = DEFAULT_WINDOW) -> None:
        self.window = window or DEFAULT_WINDOW
        self.total = 0
        self._events: deque[tuple[float, int]] = deque()
        self._started = time.monotonic()
        self._lock = threading.Lock()

    def add(self, size: int) -> None:
        """Account `size` transferred bytes."""
        with self._lock:
            now = time.monotonic()
            self.total += size
            self._events.append((now, size))
            self._expire(now)

    @property
    def rate(self) -> float:
        """Bytes per second over the last `window` seconds."""
        with self._lock:
            now = time.monotonic()
            self._expire(now)
            span = min(now - self._started, self.window)
            if span <= 0:
                return 0.0
            return sum(size for _, size in self._events) / span

    def _expire(self, now: float) -> None:
        while self._events and now - self._events[0][0] > self.window:
            self._events.popleft()


class ThroughputColumn(ProgressColumn):
    """Renders the byte throughput of a task's `Throughput` field.

    The item count says how far along a task is; this says how fast bytes are
    actually moving.
    """

    def render(self, task: Task) -> Text:
        throughput: Throughput | None = task.fields.get("throughput")
        rate = throughput.rate if throughput is not None else 0.0
        return Text(format_bytes(rate, "B/s"), style="progress.data.speed")


DEFAULT_COLUMNS: tuple[str | ProgressColumn, ...] = (
    TextColumn("[progress.description]{task.description}"),
    TaskProgressColumn(show_speed=True),
    BarColumn(bar_width=None),
    MofNCompleteColumn(),
    ThroughputColumn(),
    TimeElapsedColumn(),
    TimeRemainingColumn(),
)


@contextmanager
def logging_through(console: Console) -> Iterator[None]:
    """
    Route log output through a rich console for the duration of the block, so
    log lines don't cut into whatever that console is rendering.

    anystore's log handlers resolve `sys.stderr` on every emit, so swapping it
    is enough to catch them – as well as warnings and any other library writing
    to stderr.

    Args:
        console: The console to print through
    """
    stderr = sys.stderr
    sys.stderr = _ConsoleStream(console)  # type: ignore[assignment]
    try:
        yield
    finally:
        sys.stderr = stderr


class _ConsoleStream:
    """Minimal file-like object that prints through a rich `Console`."""

    def __init__(self, console: Console) -> None:
        self.console = console

    def write(self, data: str) -> int:
        line = data.rstrip("\n")
        if line:
            # `from_ansi` keeps structlog's colors as rich styles instead of
            # leaking escape sequences into rich's width calculation
            self.console.print(Text.from_ansi(line), soft_wrap=True)
        return len(data)

    def flush(self) -> None:
        pass

    def isatty(self) -> bool:
        return self.console.is_terminal


class ProgressTask:
    """
    A labelled bar on a [`SyncProgressBar`][anystore.io.progress.SyncProgressBar].

    Use it as a context manager to have it disappear when its work is done:

    Example:
        ```python
        with bar.task("chunk 1", total=len(keys)) as task:
            for key in keys:
                task.advance(size=len(store.get(key)))
        ```
    """

    def __init__(
        self, bar: "SyncProgressBar", task_id: TaskID, throughput: Throughput
    ) -> None:
        self.bar = bar
        self.task_id = task_id
        self.throughput = throughput

    def advance(self, items: int | None = 1, size: int | None = None) -> None:
        """
        Advance the task, optionally accounting transferred bytes.

        Args:
            items: Number of items completed (default: 1)
            size: Bytes transferred for them, added to this task's throughput
                and to the overall throughput of the bar
        """
        if size:
            self.throughput.add(size)
            # a task can be fed the bar's own throughput to sum up the others
            if self.throughput is not self.bar.throughput:
                self.bar.throughput.add(size)
        self.bar.progress.advance(self.task_id, items or 0)

    def update(self, **kwargs: Any) -> None:
        """Update the underlying rich task, e.g. `total` or `description`"""
        self.bar.progress.update(self.task_id, **kwargs)

    def remove(self) -> None:
        """Remove the bar from the display"""
        self.bar.progress.remove_task(self.task_id)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> None:
        self.remove()


class SyncProgressBar:
    """
    A live display of one or more bars, with byte throughput.

    For a single bar, describe it up front and advance the display itself:

    Example:
        ```python
        with SyncProgressBar("download", total=len(keys)) as bar:
            for key in keys:
                bar.advance(size=len(store.get(key)))
        ```

    For concurrent work, add a bar per unit of it with
    [`task`][anystore.io.progress.SyncProgressBar.task] – they may be advanced
    from any thread, and all of them live in this one display, as wrapping each
    bar in a display of its own would leave all but the first invisible (rich
    only renders the outermost live display):

    Example:
        ```python
        with SyncProgressBar() as bar:
            for prefix in prefixes:
                with bar.task(prefix, total=len(keys)) as task:
                    for key in keys:
                        task.advance(size=len(store.get(key)))
        ```

    Args:
        description: Label for the display's own bar – leave it out for the
            multi-task case, where each task brings its own
        total: Number of items for that bar, if known
        console: Console to render on (default: a new one on the current
            `sys.stderr`)
        columns: Custom rich progress columns
        transient: Remove the whole display when it's done (default: yes)
        route_logging: Print log output through the same console while the
            display is running (default: yes)
        disable: Render nothing at all, e.g. for a `--quiet` flag
        window: Seconds to average throughput rates over
    """

    def __init__(
        self,
        description: str | None = None,
        total: int | None = None,
        console: Console | None = None,
        columns: Sequence[str | ProgressColumn] | None = None,
        transient: bool | None = True,
        route_logging: bool | None = True,
        disable: bool | None = False,
        window: int | None = DEFAULT_WINDOW,
    ) -> None:
        self.throughput = Throughput(window)
        self.description = description
        self.total = total
        self.console = console
        self.columns = columns or DEFAULT_COLUMNS
        self.transient = bool(transient)
        self.route_logging = bool(route_logging)
        self.disable = bool(disable)
        self.window = window
        self._progress: Progress | None = None
        self._task: ProgressTask | None = None
        self._stack = ExitStack()

    @property
    def progress(self) -> Progress:
        """The underlying rich `Progress`, once the display is started"""
        if self._progress is None:
            raise RuntimeError("Progress display not started")
        return self._progress

    @property
    def default_task(self) -> ProgressTask:
        """The display's own bar, for the single-bar case"""
        if self._task is None:
            raise RuntimeError(
                "No bar of its own, give the `SyncProgressBar` a description "
                "or add a `task()`"
            )
        return self._task

    def advance(self, items: int | None = 1, size: int | None = None) -> None:
        """
        Advance the display's own bar, optionally accounting transferred bytes.

        Only for the single-bar case – with several tasks, advance those.

        Args:
            items: Number of items completed (default: 1)
            size: Bytes transferred for them
        """
        self.default_task.advance(items, size)

    def update(self, **kwargs: Any) -> None:
        """Update the display's own bar, e.g. `total` or `description`"""
        self.default_task.update(**kwargs)

    def task(
        self,
        description: str,
        total: int | None = None,
        throughput: Throughput | None = None,
    ) -> ProgressTask:
        """
        Add a labelled bar to the display.

        Args:
            description: The label, e.g. the key prefix being worked on
            total: Number of items, if known – an unknown total pulses instead
                of filling up
            throughput: Byte counter to display, pass the bar's own
                `throughput` for a task that sums up all the others (default: a
                fresh one for this task)

        Returns:
            The task handle, removable and usable as a context manager
        """
        throughput = throughput if throughput is not None else Throughput(self.window)
        task_id = self.progress.add_task(
            description, total=total, throughput=throughput
        )
        return ProgressTask(self, task_id, throughput)

    def start(self) -> Self:
        """Start rendering (and, unless turned off, routing log output)"""
        # bind the console late: it holds on to the current `sys.stderr`, which
        # `logging_through` is about to replace
        console = self.console or Console(file=sys.stderr)
        self.console = console
        self._progress = Progress(
            *self.columns,
            console=console,
            transient=self.transient,
            disable=self.disable,
            # `logging_through` routes stderr through this console already, and
            # rich's own redirect would fight it
            redirect_stdout=False,
            redirect_stderr=False,
        )
        if self.route_logging:
            self._stack.enter_context(logging_through(console))
        self._stack.enter_context(self._progress)
        if self.description is not None or self.total is not None:
            # one counter for a single bar: the task shares the bar's own
            self._task = self.task(
                self.description or "", total=self.total, throughput=self.throughput
            )
        return self

    def stop(self) -> None:
        """Stop rendering and restore log output"""
        self._progress = None
        self._task = None
        self._stack.close()

    def __enter__(self) -> Self:
        return self.start()

    def __exit__(self, *args: Any) -> None:
        self.stop()
