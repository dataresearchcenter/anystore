import sys
import threading
from io import StringIO

import pytest
from rich.console import Console
from rich.progress import Task, TaskID

from anystore.io import (
    ProgressTask,
    SyncProgressBar,
    Throughput,
    ThroughputColumn,
    logging_through,
)
from anystore.logging import configure_logging, get_logger


def make_bar(**kwargs) -> tuple[SyncProgressBar, StringIO]:
    out = StringIO()
    console = Console(file=out, force_terminal=True, width=120)
    return SyncProgressBar(console=console, **kwargs), out


def test_io_progress_throughput():
    throughput = Throughput()
    assert throughput.total == 0
    assert throughput.rate == 0

    throughput.add(1024)
    throughput.add(1024)
    assert throughput.total == 2048
    assert throughput.rate > 0

    # only the events within the window count towards the rate
    throughput = Throughput(window=30)
    throughput.add(1024)
    throughput._events[0] = (throughput._events[0][0] - 60, 1024)
    assert throughput.rate == 0
    assert throughput.total == 1024


def test_io_progress_throughput_column():
    column = ThroughputColumn()
    throughput = Throughput()
    throughput.add(1024)

    task = Task(TaskID(0), "test", 100, 0, _get_time=lambda: 0.0)
    assert column.render(task).plain.endswith("B/s")  # no throughput field

    task.fields["throughput"] = throughput
    assert column.render(task).plain.endswith("B/s")


def test_io_progress_bar():
    bar, out = make_bar()
    with bar as b:
        assert b is bar
        overall = bar.task("total", total=2, throughput=bar.throughput)
        with bar.task("chunk", total=3) as task:
            task.advance(size=1024)
            task.advance(size=1024)
            task.advance()
            assert task.throughput.total == 2048
            assert len(bar.progress.tasks) == 2
        # the task is gone once its block ends, the total bar stays
        assert len(bar.progress.tasks) == 1
        overall.advance()

    # bytes are counted once for the task and once for the whole run
    assert bar.throughput.total == 2048
    # a task rendering the bar's own throughput doesn't double count
    assert overall.throughput is bar.throughput

    with pytest.raises(RuntimeError):
        bar.progress

    assert out.getvalue()  # something was rendered


def test_io_progress_bar_single():
    # a single bar: the display is the task
    out = StringIO()
    console = Console(file=out, force_terminal=True, width=120)
    with SyncProgressBar("download", total=3, console=console) as bar:
        bar.advance(size=1024)
        bar.advance(size=1024)
        bar.advance()
        assert len(bar.progress.tasks) == 1
        assert bar.progress.tasks[0].description == "download"
        assert bar.progress.tasks[0].completed == 3
        assert isinstance(bar.default_task, ProgressTask)
        # one counter, not two: the bar's own throughput backs its bar
        assert bar.default_task.throughput is bar.throughput
        bar.update(total=10, description="still going")
        assert bar.progress.tasks[0].total == 10
        assert bar.progress.tasks[0].description == "still going"
    assert bar.throughput.total == 2048

    # a total without a description gets an unlabelled bar
    bar, _ = make_bar(total=1)
    with bar:
        assert bar.progress.tasks[0].description == ""


def test_io_progress_bar_no_default_task():
    bar, _ = make_bar()
    with bar:
        with pytest.raises(RuntimeError):
            bar.advance()
        with pytest.raises(RuntimeError):
            bar.default_task
        # tasks can still be added by hand
        task = bar.task("chunk", total=1)
        task.advance(size=1)
    assert bar.throughput.total == 1


def test_io_progress_bar_threads():
    bar, _ = make_bar()
    with bar:
        overall = bar.task("total", total=4, throughput=bar.throughput)

        def work(ix: int) -> None:
            with bar.task(f"worker-{ix}", total=10) as task:
                for _ in range(10):
                    task.advance(size=100)
            overall.advance()

        threads = [threading.Thread(target=work, args=(i,)) for i in range(4)]
        [t.start() for t in threads]
        [t.join() for t in threads]

        assert len(bar.progress.tasks) == 1
        assert bar.progress.tasks[0].completed == 4

    assert bar.throughput.total == 4000


def test_io_progress_bar_task_update():
    bar, _ = make_bar()
    with bar:
        task = bar.task("unknown")
        assert bar.progress.tasks[0].total is None
        task.update(total=10, description="known")
        assert bar.progress.tasks[0].total == 10
        assert bar.progress.tasks[0].description == "known"
        task.remove()
        assert not bar.progress.tasks


def test_io_progress_bar_disable():
    bar, out = make_bar(disable=True)
    with bar:
        with bar.task("chunk", total=1) as task:
            task.advance(size=1)
    assert bar.throughput.total == 1
    assert not out.getvalue()


def test_io_progress_logging():
    # anystore's log handlers resolve `sys.stderr` on every emit, which is what
    # makes routing them through the console a matter of swapping it
    configure_logging()
    log = get_logger("test.progress")
    bar, out = make_bar()
    stderr = sys.stderr
    with bar:
        assert sys.stderr is not stderr
        log.error("a log line")
    assert sys.stderr is stderr
    assert "a log line" in out.getvalue()

    # opting out leaves logging alone
    bar, out = make_bar(route_logging=False)
    with bar:
        assert sys.stderr is stderr


def test_io_progress_logging_through():
    out = StringIO()
    console = Console(file=out, force_terminal=True, width=120)
    stderr = sys.stderr
    with logging_through(console):
        assert sys.stderr is not stderr
        print("through the console", file=sys.stderr)
    assert sys.stderr is stderr
    assert "through the console" in out.getvalue()


def test_io_progress_task_handle():
    bar, _ = make_bar()
    with bar:
        task = bar.task("chunk", total=1)
        assert isinstance(task, ProgressTask)
        assert task.bar is bar
        task.advance(items=0, size=512)
        assert bar.progress.tasks[0].completed == 0
        assert bar.throughput.total == 512
