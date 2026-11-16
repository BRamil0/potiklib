from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from concurrent.futures import Future
from typing import ParamSpec, TypeVar

try:
    from concurrent.futures import InterpreterPoolExecutor
except ImportError:  # pragma: no cover
    InterpreterPoolExecutor = None  # type: ignore[assignment,misc]

P = ParamSpec("P")
R = TypeVar("R")


class IPEProvider:
    """Synchronous execution provider backed by InterpreterPoolExecutor."""

    def __init__(self, workers: int) -> None:
        if workers < 1:
            raise ValueError("workers must be >= 1")
        if InterpreterPoolExecutor is None:
            raise RuntimeError("InterpreterPoolExecutor requires Python 3.14 or newer")

        self._lock = threading.Lock()
        self._closing = False
        self._executor = InterpreterPoolExecutor(max_workers=workers)

    def submit(self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        with self._lock:
            if self._closing:
                raise RuntimeError("IPEProvider is shutting down")

        target: Callable[P, R] = getattr(fn, "__wrapped__", fn)
        return self._executor.submit(target, *args, **kwargs)

    def execute(self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.submit(fn, *args, **kwargs).result()

    async def execute_async(self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        future = self.submit(fn, *args, **kwargs)
        return await asyncio.wrap_future(future)

    def shutdown(self, wait: bool = True) -> None:
        with self._lock:
            self._closing = True
        self._executor.shutdown(wait=wait)
