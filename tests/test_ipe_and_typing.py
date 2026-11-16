import asyncio
import sys

import pytest

from potiklib import _main, apotik, potik, run, task
from potiklib.providers.ipe import IPEProvider


@task
def ipe_square(x: int) -> int:
    return x * x


@task
def ipe_fail(msg: str) -> int:
    raise ValueError(msg)


class CustomAwaitable:
    def __init__(self, value: int) -> None:
        self._value = value

    def __await__(self):
        async def _inner() -> int:
            await asyncio.sleep(0)
            return self._value

        return _inner().__await__()


def test_ipe_provider_with_wrapped_task_function() -> None:
    provider = IPEProvider(workers=2)
    try:
        assert provider.execute(ipe_square, 9) == 81
        with pytest.raises(ValueError, match="ipe-error"):
            provider.execute(ipe_fail, "ipe-error")
    finally:
        provider.shutdown(wait=True)


def test_gil_enabled_routes_sync_tasks_to_ipe(monkeypatch: pytest.MonkeyPatch) -> None:
    if not hasattr(sys, "_is_gil_enabled"):
        pytest.skip("sys._is_gil_enabled not present")

    monkeypatch.setattr(_main, "_is_gil_enabled", lambda: True)

    async def main() -> tuple[int, int, int]:
        r1 = ipe_square(5)
        r2 = potik(ipe_square, 6)
        r3 = await apotik(ipe_square, 7)
        return r1, r2, r3

    assert run(main, workers=2) == (25, 36, 49)


def test_typing_overloads_and_inference() -> None:
    @task
    def typed_sync(a: int, b: str = "x") -> tuple[int, str]:
        return a, b

    @task
    async def typed_async(a: int) -> str:
        await asyncio.sleep(0)
        return str(a)

    async def main() -> int:
        s1: tuple[int, str] = typed_sync(1, b="y")
        s2: tuple[int, str] = potik(typed_sync, 2, "z")
        s3: tuple[int, str] = await apotik(typed_sync, 3)

        a1: str = await typed_async(4)
        a2: str = await apotik(typed_async, 5)
        a3: str = await apotik(typed_async(6))
        a4: int = await apotik(CustomAwaitable(7))

        t_task: asyncio.Task[str] = asyncio.create_task(apotik(typed_async, 8))
        a5: str = await asyncio.wait_for(apotik(typed_async, 9), timeout=1.0)
        a6: str = await t_task

        assert s1 == (1, "y")
        assert s2 == (2, "z")
        assert s3 == (3, "x")
        assert (a1, a2, a3, a4, a5, a6) == ("4", "5", "6", 7, "9", "8")
        return a4

    result: int = run(main, workers=2)
    assert result == 7
