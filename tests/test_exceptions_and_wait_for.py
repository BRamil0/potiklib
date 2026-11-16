import asyncio
import threading
import time

import pytest

from potiklib import apotik, potik, run, task
from potiklib.providers.atpe import ATPEProvider


class CustomTaskError(Exception):
    pass


@task
def fail_sync(msg: str) -> None:
    raise CustomTaskError(msg)


@task
async def fail_async(msg: str) -> None:
    await asyncio.sleep(0)
    raise CustomTaskError(msg)


@task
async def slow_async_task(delay: float) -> str:
    await asyncio.sleep(delay)
    return "done"


def test_exception_propagation_across_all_apis() -> None:
    async def main() -> None:
        with pytest.raises(CustomTaskError, match="sync-task"):
            fail_sync("sync-task")

        with pytest.raises(CustomTaskError, match="potik-call"):
            potik(fail_sync, "potik-call")

        with pytest.raises(CustomTaskError, match="async-task"):
            await fail_async("async-task")

        with pytest.raises(CustomTaskError, match="apotik-async-fn"):
            await apotik(fail_async, "apotik-async-fn")

        with pytest.raises(CustomTaskError, match="apotik-coro"):
            await apotik(fail_async("apotik-coro"))

        with pytest.raises(CustomTaskError, match="apotik-sync-fn"):
            await apotik(fail_sync, "apotik-sync-fn")

    run(main, workers=2)


def test_exception_in_main_still_shuts_down_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    shutdown_called = False
    orig_shutdown = ATPEProvider.shutdown

    def spy_shutdown(self: ATPEProvider, wait: bool = True) -> None:
        nonlocal shutdown_called
        shutdown_called = True
        orig_shutdown(self, wait=wait)

    monkeypatch.setattr(ATPEProvider, "shutdown", spy_shutdown)

    async def failing_main() -> None:
        await apotik(asyncio.sleep, 0)
        raise CustomTaskError("main-boom")

    with pytest.raises(CustomTaskError, match="main-boom"):
        run(failing_main, workers=2)

    assert shutdown_called is True


def test_asyncio_wait_for_cancels_atpe_task_without_hanging_shutdown() -> None:
    async def main() -> None:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(slow_async_task(2.0), timeout=0.05)

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(apotik(slow_async_task, 2.0), timeout=0.05)

    start = time.perf_counter()
    run(main, workers=2)
    elapsed = time.perf_counter() - start
    # If worker tasks were not cancelled cleanly, shutdown(wait=True) would block ~2.0s.
    assert elapsed < 0.8


def test_asyncio_wait_for_on_sync_apotik() -> None:
    release = threading.Event()

    def slow_sync() -> str:
        release.wait(timeout=2.0)
        return "finished"

    async def main() -> None:
        try:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(apotik(slow_sync), timeout=0.05)
        finally:
            release.set()

    start = time.perf_counter()
    run(main, workers=1)
    elapsed = time.perf_counter() - start
    assert elapsed < 1.0
