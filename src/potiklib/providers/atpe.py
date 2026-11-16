from __future__ import annotations

import asyncio
import contextvars
import inspect
import threading
from collections.abc import Awaitable, Callable, Coroutine
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")


class _WorkerState:
    def __init__(self, index: int) -> None:
        self.index = index
        self.loop: asyncio.AbstractEventLoop | None = None
        self.in_flight = 0
        self.ready = threading.Event()


class ATPEProvider:
    """Async execution provider backed by a ThreadPoolExecutor of event loops."""

    def __init__(self, workers: int) -> None:
        if workers < 1:
            raise ValueError("workers must be >= 1")

        self._max_workers = workers
        self._lock = threading.Lock()
        self._idle_cond = threading.Condition(self._lock)
        self._total_in_flight = 0
        self._closing = False
        self._shutdown_done = False
        self._local = threading.local()
        self._workers = [_WorkerState(i) for i in range(workers)]
        self._executor = ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="potik-atpe",
        )

        try:
            for worker in self._workers:
                self._executor.submit(self._worker_main, worker)
            for worker in self._workers:
                worker.ready.wait()
        except BaseException:
            self.shutdown(wait=False)
            raise

    def _worker_main(self, worker: _WorkerState) -> None:
        self._local.in_atpe = True
        self._local.worker_index = worker.index
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        worker.loop = loop
        loop.call_soon(worker.ready.set)
        try:
            loop.run_forever()
        finally:
            try:
                pending = asyncio.all_tasks(loop)
                if pending:
                    for task in pending:
                        task.cancel()
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                asyncio.set_event_loop(None)
                loop.close()
                self._local.in_atpe = False

    def _acquire_worker(self) -> _WorkerState:
        with self._lock:
            if self._closing:
                raise RuntimeError("ATPEProvider is shutting down")
            worker = min(self._workers, key=lambda w: w.in_flight)
            worker.in_flight += 1
            self._total_in_flight += 1
            return worker

    def _release_worker(self, worker: _WorkerState) -> None:
        with self._lock:
            worker.in_flight -= 1
            self._total_in_flight -= 1
            if self._total_in_flight == 0:
                self._idle_cond.notify_all()

    async def execute_awaitable(self, awaitable: Awaitable[R], /) -> R:
        if getattr(self._local, "in_atpe", False):
            return await awaitable

        if inspect.iscoroutine(awaitable):
            coro: Coroutine[Any, Any, R] = awaitable
        else:

            async def _wrap_awaitable() -> R:
                return await awaitable

            coro = _wrap_awaitable()

        return await self._dispatch_coro(coro)

    async def execute_callable(
        self,
        fn: Callable[P, Coroutine[Any, Any, R] | Awaitable[R]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        if getattr(self._local, "in_atpe", False):
            return await fn(*args, **kwargs)

        async def _invoke() -> R:
            return await fn(*args, **kwargs)

        return await self._dispatch_coro(_invoke())

    async def _dispatch_coro(self, coro: Coroutine[Any, Any, R]) -> R:
        ctx = contextvars.copy_context()
        worker = self._acquire_worker()
        loop = worker.loop
        if loop is None:
            self._release_worker(worker)
            coro.close()
            raise RuntimeError("Worker event loop is not initialized")

        cf_future: Future[R] = Future()
        task_holder: list[asyncio.Task[R] | None] = [None]

        def _on_task_done(task: asyncio.Task[R]) -> None:
            self._release_worker(worker)
            if task.cancelled():
                cf_future.cancel()
            else:
                exc = task.exception()
                if exc is not None:
                    if not cf_future.done():
                        cf_future.set_exception(exc)
                else:
                    if not cf_future.done():
                        cf_future.set_result(task.result())

        def _create_task_and_link() -> None:
            if cf_future.cancelled():
                coro.close()
                self._release_worker(worker)
                return
            try:
                task = loop.create_task(coro)
            except BaseException as exc:
                coro.close()
                if not cf_future.done():
                    cf_future.set_exception(exc)
                self._release_worker(worker)
                return

            task_holder[0] = task
            task.add_done_callback(_on_task_done)
            if cf_future.cancelled():
                task.cancel()

        def _cancel_worker_task() -> None:
            task = task_holder[0]
            if task is not None and not task.done():
                task.cancel()

        def _on_cf_done(fut: Future[R]) -> None:
            if fut.cancelled():
                try:
                    loop.call_soon_threadsafe(_cancel_worker_task)
                except RuntimeError:
                    pass

        cf_future.add_done_callback(_on_cf_done)

        try:
            loop.call_soon_threadsafe(ctx.run, _create_task_and_link)
        except BaseException:
            coro.close()
            self._release_worker(worker)
            raise

        return await asyncio.wrap_future(cf_future)

    def shutdown(self, wait: bool = True) -> None:
        with self._lock:
            if self._shutdown_done:
                return
            self._closing = True
            if wait:
                while self._total_in_flight > 0:
                    self._idle_cond.wait()
            self._shutdown_done = True
            workers = list(self._workers)

        for worker in workers:
            loop = worker.loop
            if loop is not None and not loop.is_closed():
                try:
                    loop.call_soon_threadsafe(loop.stop)
                except RuntimeError:
                    pass

        self._executor.shutdown(wait=wait)
