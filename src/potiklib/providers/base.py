from __future__ import annotations

from collections.abc import Awaitable, Callable, Coroutine
from concurrent.futures import Future
from typing import Any, ParamSpec, Protocol, TypeVar

P = ParamSpec("P")
R = TypeVar("R")


class Provider(Protocol):
    """Base protocol for execution providers."""

    def shutdown(self, wait: bool = True) -> None: ...


class SyncProvider(Provider, Protocol):
    """Protocol for synchronous execution providers (TPE / IPE)."""

    def submit(self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> Future[R]: ...

    def execute(self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R: ...

    async def execute_async(
        self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
    ) -> R: ...


class AsyncProvider(Provider, Protocol):
    """Protocol for asynchronous execution providers (ATPE)."""

    async def execute_awaitable(self, awaitable: Awaitable[R], /) -> R: ...

    async def execute_callable(
        self,
        fn: Callable[P, Coroutine[Any, Any, R] | Awaitable[R]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R: ...
