import asyncio
import functools
import inspect
import os
import sys
import threading
from collections.abc import Awaitable, Callable, Coroutine
from contextvars import ContextVar
from typing import Any, overload

from potiklib.providers.atpe import ATPEProvider
from potiklib.providers.base import SyncProvider
from potiklib.providers.ipe import IPEProvider
from potiklib.providers.tpe import TPEProvider


class _Runtime:
    """Manages lazy provider creation and graceful shutdown for a potiklib.run() session."""

    def __init__(self, workers: int) -> None:
        self.workers = workers
        self._lock = threading.Lock()
        self._closing = False
        self._atpe: ATPEProvider | None = None
        self._tpe: TPEProvider | None = None
        self._ipe: IPEProvider | None = None

    def get_atpe(self) -> ATPEProvider:
        with self._lock:
            if self._closing:
                raise RuntimeError("potiklib runtime is shutting down")
            if self._atpe is None:
                self._atpe = ATPEProvider(self.workers)
            return self._atpe

    def get_tpe(self) -> TPEProvider:
        with self._lock:
            if self._closing:
                raise RuntimeError("potiklib runtime is shutting down")
            if self._tpe is None:
                self._tpe = TPEProvider(self.workers)
            return self._tpe

    def get_ipe(self) -> IPEProvider:
        with self._lock:
            if self._closing:
                raise RuntimeError("potiklib runtime is shutting down")
            if self._ipe is None:
                self._ipe = IPEProvider(self.workers)
            return self._ipe

    def get_sync_provider(self) -> SyncProvider:
        if sys._is_gil_enabled():
            return self.get_ipe()
        return self.get_tpe()

    def shutdown(self) -> None:
        with self._lock:
            atpe = self._atpe

        first_exc: BaseException | None = None
        if atpe is not None:
            try:
                atpe.shutdown(wait=True)
            except BaseException as exc:
                first_exc = exc

        with self._lock:
            self._closing = True
            tpe = self._tpe
            ipe = self._ipe

        for provider in (tpe, ipe):
            if provider is not None:
                try:
                    provider.shutdown(wait=True)
                except BaseException as exc:
                    if first_exc is None:
                        first_exc = exc

        if first_exc is not None:
            raise first_exc


_runtime_var: ContextVar[_Runtime | None] = ContextVar("potiklib_runtime", default=None)
_global_runtime: _Runtime | None = None


def _get_runtime() -> _Runtime | None:
    runtime = _runtime_var.get()
    if runtime is not None:
        return runtime
    return _global_runtime


@overload
def run[R](main: Awaitable[R], *, workers: int | None = None) -> R: ...


@overload
def run[R](main: Callable[[], Awaitable[R]], *, workers: int | None = None) -> R: ...


@overload
def run[R](main: Callable[[], R], *, workers: int | None = None) -> R: ...


def run[R](
    main: Awaitable[R] | Callable[[], Any],
    *,
    workers: int | None = None,
) -> Any:
    """Run main function or coroutine inside a potiklib runtime."""
    global _global_runtime

    resolved_workers = (os.process_cpu_count() or 1) if workers is None else workers
    if resolved_workers < 1:
        if inspect.iscoroutine(main):
            main.close()
        raise ValueError("workers must be >= 1")

    if not (inspect.isawaitable(main) or callable(main)):
        raise TypeError(f"run() expected a callable or coroutine, got {type(main).__name__}")

    runtime = _Runtime(resolved_workers)
    prev_global = _global_runtime
    _global_runtime = runtime
    token = _runtime_var.set(runtime)
    try:
        if inspect.iscoroutine(main):
            return asyncio.run(main)
        if inspect.iscoroutinefunction(main):
            return asyncio.run(main())
        if inspect.isawaitable(main):

            async def _await_main() -> Any:
                return await main

            return asyncio.run(_await_main())
        if callable(main):
            return main()
        raise TypeError(f"run() expected a callable or coroutine, got {type(main).__name__}")
    finally:
        _runtime_var.reset(token)
        _global_runtime = prev_global
        runtime.shutdown()


def potik[**P, R](fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
    """Execute a synchronous callable in the sync worker pool, or locally outside run()."""
    if getattr(fn, "_potik_task", False):
        fn = getattr(fn, "__wrapped__", fn)

    runtime = _get_runtime()
    if runtime is None:
        return fn(*args, **kwargs)
    return runtime.get_sync_provider().execute(fn, *args, **kwargs)


@overload
async def apotik[R](target: Awaitable[R], /) -> R: ...


@overload
async def apotik[**P, R](
    target: Callable[P, Awaitable[R]], /, *args: P.args, **kwargs: P.kwargs
) -> R: ...


@overload
async def apotik[**P, R](target: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R: ...


async def apotik(target: Any, /, *args: Any, **kwargs: Any) -> Any:
    """Execute an awaitable, async callable, or sync callable asynchronously in worker pools."""
    if inspect.isawaitable(target):
        if args or kwargs:
            raise TypeError("apotik() cannot accept arguments for an already-created Awaitable")
        runtime = _get_runtime()
        if runtime is None:
            return await target
        return await runtime.get_atpe().execute_awaitable(target)

    if callable(target):
        if getattr(target, "_potik_task", False):
            target = getattr(target, "__wrapped__", target)

        runtime = _get_runtime()
        if inspect.iscoroutinefunction(target):
            if runtime is None:
                return await target(*args, **kwargs)
            return await runtime.get_atpe().execute_callable(target, *args, **kwargs)

        if runtime is None:
            result = target(*args, **kwargs)
            if inspect.isawaitable(result):
                return await result
            return result
        return await runtime.get_sync_provider().execute_async(target, *args, **kwargs)

    raise TypeError(f"apotik() expected an awaitable or callable, got {type(target).__name__}")


@overload
def task[**P, R](
    fn: Callable[P, Coroutine[Any, Any, R]], /
) -> Callable[P, Coroutine[Any, Any, R]]: ...


@overload
def task[**P, R](fn: Callable[P, R], /) -> Callable[P, R]: ...


def task[**P](fn: Callable[P, Any], /) -> Callable[P, Any]:
    """Decorate a sync or async function to run automatically via potiklib workers inside run()."""
    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def _async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            runtime = _get_runtime()
            if runtime is None:
                return await fn(*args, **kwargs)
            return await runtime.get_atpe().execute_callable(fn, *args, **kwargs)

        setattr(_async_wrapper, "_potik_task", True)
        return _async_wrapper

    @functools.wraps(fn)
    def _sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
        runtime = _get_runtime()
        if runtime is None:
            return fn(*args, **kwargs)
        return runtime.get_sync_provider().execute(fn, *args, **kwargs)

    setattr(_sync_wrapper, "_potik_task", True)
    return _sync_wrapper
