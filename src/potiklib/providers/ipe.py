import asyncio
import threading
from collections.abc import Callable
from concurrent.futures import Future, InterpreterPoolExecutor


class IPEProvider:
    """Synchronous execution provider backed by InterpreterPoolExecutor."""

    def __init__(self, workers: int) -> None:
        if workers < 1:
            raise ValueError("workers must be >= 1")

        self._lock = threading.Lock()
        self._closing = False
        self._executor = InterpreterPoolExecutor(max_workers=workers)

    def submit[**P, R](self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        with self._lock:
            if self._closing:
                raise RuntimeError("IPEProvider is shutting down")

        target: Callable[P, R] = (
            getattr(fn, "__wrapped__", fn) if getattr(fn, "_potik_task", False) else fn
        )
        return self._executor.submit(target, *args, **kwargs)

    def execute[**P, R](self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.submit(fn, *args, **kwargs).result()

    async def execute_async[**P, R](
        self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
    ) -> R:
        future = self.submit(fn, *args, **kwargs)
        return await asyncio.wrap_future(future)

    def shutdown(self, wait: bool = True) -> None:
        with self._lock:
            self._closing = True
        self._executor.shutdown(wait=wait)
