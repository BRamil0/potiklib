import asyncio
import sys
import threading

import pytest

import potiklib
from potiklib import _main, potik, run, task
from potiklib.providers.atpe import ATPEProvider
from potiklib.providers.ipe import IPEProvider
from potiklib.providers.tpe import TPEProvider


def test_run_accepts_sync_async_fn_and_coroutine() -> None:
    def sync_main() -> str:
        return "sync"

    async def async_main() -> str:
        await asyncio.sleep(0)
        return "async"

    assert run(sync_main, workers=2) == "sync"
    assert run(async_main, workers=2) == "async"
    assert run(async_main(), workers=2) == "async"


def test_run_validates_workers() -> None:
    async def noop() -> None:
        return None

    coro = noop()
    with pytest.raises(ValueError, match="workers must be >= 1"):
        run(coro, workers=0)


def test_lazy_initialization_and_selective_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[str] = []
    shutdown_calls: list[tuple[str, bool]] = []

    orig_atpe_init = ATPEProvider.__init__
    orig_atpe_shutdown = ATPEProvider.shutdown
    orig_tpe_init = TPEProvider.__init__
    orig_tpe_shutdown = TPEProvider.shutdown
    orig_ipe_init = IPEProvider.__init__
    orig_ipe_shutdown = IPEProvider.shutdown

    def spy_atpe_init(self: ATPEProvider, workers: int) -> None:
        created.append("atpe")
        orig_atpe_init(self, workers)

    def spy_atpe_shutdown(self: ATPEProvider, wait: bool = True) -> None:
        shutdown_calls.append(("atpe", wait))
        orig_atpe_shutdown(self, wait=wait)

    def spy_tpe_init(self: TPEProvider, workers: int) -> None:
        created.append("tpe")
        orig_tpe_init(self, workers)

    def spy_tpe_shutdown(self: TPEProvider, wait: bool = True) -> None:
        shutdown_calls.append(("tpe", wait))
        orig_tpe_shutdown(self, wait=wait)

    def spy_ipe_init(self: IPEProvider, workers: int) -> None:
        created.append("ipe")
        orig_ipe_init(self, workers)

    def spy_ipe_shutdown(self: IPEProvider, wait: bool = True) -> None:
        shutdown_calls.append(("ipe", wait))
        orig_ipe_shutdown(self, wait=wait)

    monkeypatch.setattr(ATPEProvider, "__init__", spy_atpe_init)
    monkeypatch.setattr(ATPEProvider, "shutdown", spy_atpe_shutdown)
    monkeypatch.setattr(TPEProvider, "__init__", spy_tpe_init)
    monkeypatch.setattr(TPEProvider, "shutdown", spy_tpe_shutdown)
    monkeypatch.setattr(IPEProvider, "__init__", spy_ipe_init)
    monkeypatch.setattr(IPEProvider, "shutdown", spy_ipe_shutdown)

    # 1. No tasks called -> nothing initialized or shut down
    run(lambda: 42, workers=2)
    assert created == []
    assert shutdown_calls == []

    # 2. Only sync task called -> only TPE initialized & shut down (on free-threaded build)
    monkeypatch.setattr(_main, "_is_gil_enabled", lambda: False)
    created.clear()
    shutdown_calls.clear()

    @task
    def sync_work(x: int) -> tuple[int, int]:
        return x + 1, threading.get_ident()

    def only_sync() -> int:
        main_tid = threading.get_ident()
        val, worker_tid = sync_work(5)
        assert worker_tid != main_tid
        return val

    assert run(only_sync, workers=2) == 6
    assert created == ["tpe"]
    assert shutdown_calls == [("tpe", True)]

    # 3. Only async task called -> only ATPE initialized & shut down
    created.clear()
    shutdown_calls.clear()

    @task
    async def async_work(x: int) -> tuple[int, int]:
        await asyncio.sleep(0)
        return x * 2, threading.get_ident()

    async def only_async() -> int:
        main_tid = threading.get_ident()
        val, worker_tid = await async_work(7)
        assert worker_tid != main_tid
        return val

    assert run(only_async, workers=2) == 14
    assert created == ["atpe"]
    assert shutdown_calls == [("atpe", True)]

    # 4. GIL enabled -> only IPE initialized & shut down for sync tasks
    if hasattr(sys, "_is_gil_enabled"):
        monkeypatch.setattr(_main, "_is_gil_enabled", lambda: True)
        created.clear()
        shutdown_calls.clear()

        def only_ipe() -> int:
            return potik(int, -99)

        assert run(only_ipe, workers=2) == -99
        assert created == ["ipe"]
        assert shutdown_calls == [("ipe", True)]


def test_default_workers_uses_process_cpu_count(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_workers: list[int] = []
    orig_tpe_init = TPEProvider.__init__

    def spy_tpe_init(self: TPEProvider, workers: int) -> None:
        seen_workers.append(workers)
        orig_tpe_init(self, workers)

    monkeypatch.setattr(TPEProvider, "__init__", spy_tpe_init)
    monkeypatch.setattr(_main, "_is_gil_enabled", lambda: False)
    monkeypatch.setattr(potiklib._main.os, "process_cpu_count", lambda: 3)

    assert run(lambda: potik(lambda: 1)) == 1
    assert seen_workers == [3]
