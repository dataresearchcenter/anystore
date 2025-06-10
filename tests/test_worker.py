import random
import threading
import time
from datetime import timedelta
from typing import Any, Generator

from anystore.io import smart_read, smart_write
from anystore.worker import Worker, WorkerRun


def test_worker(tmp_path):
    class TestWorker(Worker):
        def get_tasks(self) -> Generator[int, None, None]:
            yield from range(5)

        def handle_task(self, task: int) -> None:
            print(f"Task: `{task}`, Thread: `{threading.get_native_id()}`")
            time.sleep(1)

        def done(self):
            smart_write(tmp_path / "done", b"yes")

    # parallel
    worker = TestWorker(heartbeat=1)
    res = worker.run()
    assert res.took
    assert res.took < timedelta(seconds=5)
    assert smart_read(tmp_path / "done") == b"yes"
    assert not res.pending

    # only 1 worker
    worker = TestWorker(threads=1, heartbeat=1)
    res = worker.run()
    assert res.took > timedelta(seconds=5)
    assert not res.pending


def test_worker_simple():
    # simple worker
    tasks = (i for i in range(5))
    worker = Worker(tasks=tasks, handle=lambda x: print(x), heartbeat=1)
    res = worker.run()
    assert res.took
    assert res.took < timedelta(seconds=5)
    assert not res.pending


def test_worker_errors():

    class TestWorker(Worker):
        def handle_task(self, i):
            v = random.random()
            time.sleep(v / 100)
            assert v < 0.5

        def exception(self, task: Any, e: Exception) -> None:
            pass

    worker = TestWorker(tasks=range(1_000), heartbeat=1)
    res = worker.run()
    assert res.done + res.errors == 1_000
    assert not res.pending


def test_worker_custom_status():
    class Status(WorkerRun):
        items: int = 0

    class TestWorker(Worker):
        def handle_task(self, i):
            self.count(items=-1)

    worker = TestWorker(tasks=range(100), status_model=Status)
    res = worker.run()
    assert isinstance(res, Status)
    assert res.items == -100
    assert not res.pending


def test_worker_requeue_during_run():

    class TestWorker(Worker):
        def handle_task(self, task: Any) -> Any:
            if task > 50:
                self.queue_task(1)

    worker = TestWorker(tasks=range(100))
    res = worker.run()
    assert res.done == 149
    assert res.pending == 0
