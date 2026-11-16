import asyncio
import contextvars
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, cast


class TPEProvider:
    """Synchronous execution provider backed by ThreadPoolExecutor."""

    def __init__(self, workers: int) -> None:
        if workers < 1:
            raise ValueError("workers must be >= 1")

        self._max_workers = workers
        self._lock = threading.Lock()
        self._active_tasks = 0
        self._closing = False
        self._local = threading.local()
        self._executor = ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="potik-tpe",
        )

    def _run_in_worker(
        self,
        ctx: contextvars.Context,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        release_once: Callable[[], None],
    ) -> Any:
        self._local.in_tpe = True
        try:
            return ctx.run(fn, *args, **kwargs)
        finally:
            self._local.in_tpe = False
            release_once()

    def _make_release_once(self) -> Callable[[], None]:
        released = False

        def _release() -> None:
            nonlocal released
            with self._lock:
                if not released:
                    released = True
                    self._active_tasks -= 1

        return _release

    def _should_run_locally_or_acquire(self) -> bool:
        with self._lock:
            if self._closing:
                raise RuntimeError("TPEProvider is shutting down")
            if getattr(self._local, "in_tpe", False) and self._active_tasks >= self._max_workers:
                return True
            self._active_tasks += 1
            return False

    def _submit_to_executor(
        self,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Future[Any]:
        ctx = contextvars.copy_context()
        release_once = self._make_release_once()
        try:
            future = self._executor.submit(self._run_in_worker, ctx, fn, args, kwargs, release_once)
        except BaseException:
            release_once()
            raise

        future.add_done_callback(lambda _fut: release_once())
        return future

    def submit[**P, R](self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        if self._should_run_locally_or_acquire():
            local_future: Future[R] = Future()
            try:
                result = fn(*args, **kwargs)
            except BaseException as exc:
                local_future.set_exception(exc)
            else:
                local_future.set_result(result)
            return local_future

        return cast(Future[R], self._submit_to_executor(fn, args, kwargs))

    def execute[**P, R](self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        if self._should_run_locally_or_acquire():
            return fn(*args, **kwargs)

        return cast(Future[R], self._submit_to_executor(fn, args, kwargs)).result()

    async def execute_async[**P, R](
        self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
    ) -> R:
        if self._should_run_locally_or_acquire():
            return fn(*args, **kwargs)

        future = cast(Future[R], self._submit_to_executor(fn, args, kwargs))
        return await asyncio.wrap_future(future)

    def shutdown(self, wait: bool = True) -> None:
        with self._lock:
            self._closing = True
        self._executor.shutdown(wait=wait)
