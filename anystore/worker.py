import sys
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta
from multiprocessing import cpu_count
from queue import Queue
from typing import Any, Callable, Generator, Self, Type

from pydantic import UUID4, BaseModel, field_validator

from anystore.logging import get_logger
from anystore.settings import Settings

settings = Settings()


class RaisingThread(threading.Thread):
    def run(self):
        self._exc = None
        try:
            super().run()
        except Exception as e:
            self._exc = e

    def join(self, timeout=None):
        super().join(timeout=timeout)
        if self._exc:
            raise self._exc


class WorkerRun(BaseModel):
    """A long running worker status. Can be subclassed to include more run
    specific metadata"""

    run_id: UUID4
    started: datetime | None = None
    stopped: datetime | None = None
    last_updated: datetime | None = None
    pending: int = 0
    done: int = 0
    errors: int = 0
    running: bool = False
    exc: str | None = None
    took: timedelta = timedelta()

    @field_validator("run_id", mode="before")
    @classmethod
    def ensure_run_id(cls, value: UUID4 | None = None) -> UUID4:
        """Give a manual uuid4 style id or create one"""
        return value or uuid.uuid4()

    def touch(self) -> None:
        self.last_updated = datetime.now()

    def stop(self, exc: Exception | None = None) -> None:
        self.running = False
        self.stopped = datetime.now()
        self.exc = str(exc)
        if self.started and self.stopped:
            self.took = self.stopped - self.started

    @classmethod
    def start(cls, **kwargs) -> Self:
        kwargs["run_id"] = cls.ensure_run_id(kwargs.get("run_id"))
        run = cls(**kwargs)
        run.started = datetime.now()
        run.running = True
        run.touch()
        return run


class Worker:
    def __init__(
        self,
        threads: int | None = settings.worker_threads,
        tasks: Generator[Any, None, None] | None = None,
        handle: Callable | None = None,
        handle_error: Callable | None = None,
        run_id: str | None = None,
        heartbeat: int | None = settings.worker_heartbeat,
        status_model: Type[WorkerRun] | None = WorkerRun,
    ) -> None:
        self.consumer_threads = max(2, threads or cpu_count()) - 1
        self.queue = Queue()
        self.consumers = []
        self.tasks = tasks
        self.handle = handle
        self.handle_error = handle_error
        self.lock = threading.Lock()
        self.counter = Counter()
        self.status_model = status_model or WorkerRun
        self.status: WorkerRun
        self.run_id = run_id
        self.heartbeat = heartbeat or settings.worker_heartbeat
        self.log = get_logger(f"{__name__}.{self.__class__.__name__}", run_id=run_id)

    def get_tasks(self) -> Generator[Any, None, None]:
        if self.tasks is None:
            raise NotImplementedError
        yield from self.tasks

    def handle_task(self, task: Any) -> Any:
        if self.handle is None:
            raise NotImplementedError
        self.handle(task)

    def exception(self, task: Any, e: Exception) -> None:
        if self.handle_error is None:
            if task is not None:
                raise e.__class__(f"{e} [Task: {task}]")
            raise e
        self.handle_error(task, e)

    def produce(self) -> None:
        for task in self.get_tasks():
            self.queue_task(task)
        self.queue.put(None)

    def queue_task(self, task: Any) -> None:
        self.count(pending=1)
        self.queue.put(task)

    def consume(self) -> None:
        while True:
            task = self.queue.get()
            if task is None:
                self.queue.put(task)  # notify other consumers
                if self.counter["pending"] < 1:
                    break
            else:
                try:
                    self.handle_task(task)
                    self.count(pending=-1)
                    self.count(done=1)
                except Exception as e:
                    self.count(pending=-1)
                    self.count(errors=1)
                    self.exception(task, e)

    def count(self, **kwargs) -> None:
        with self.lock:
            self.status.touch()
            self.counter.update(**kwargs)

    def beat(self) -> None:
        last_beat = time.time() - self.heartbeat
        while self.status.running:
            if time.time() - last_beat > self.heartbeat:
                self.log_status()
                last_beat = time.time()
                time.sleep(1)

    def log_status(self) -> None:
        status = self.get_status()
        self.log.info(f"[{self.status.run_id}] 💚 ", **status.model_dump(mode="json"))

    def get_status(self) -> WorkerRun:
        return self.status_model(**{**self.status.model_dump(), **self.counter})

    def exit(self, exc: Exception | None = None, status: int | None = 0):
        if exc is not None:
            self.log.error(f"{exc.__class__.__name__}: `{exc}`", exception=exc)
            if settings.debug:
                raise exc
        sys.exit(status)

    def done(self) -> None:
        pass

    def run(self) -> WorkerRun:
        try:
            self.log.info(f"Using `{self.consumer_threads}` consumer threads.")
            self.status = self.status_model.start(run_id=self.run_id)
            if self.heartbeat > 0:
                heartbeat = RaisingThread(target=self.beat)
                heartbeat.start()
            producer = RaisingThread(target=self.produce)
            for _ in range(self.consumer_threads):
                consumer = RaisingThread(target=self.consume)
                consumer.start()
                self.consumers.append(consumer)
            producer.start()
            for consumer in self.consumers:
                try:
                    consumer.join()
                except Exception as e:
                    self.exception(None, e)
            producer.join()
        except KeyboardInterrupt:
            self.status.stop()
            self.log_status()
            self.exit()
        except Exception as e:
            self.status.stop(exc=e)
            self.log_status()
            raise e
        finally:
            self.status.stop()
            self.log_status()
            self.done()
        return self.get_status()
