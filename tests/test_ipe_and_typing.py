import asyncio
import concurrent.interpreters
import functools
import sys

import pytest

from potiklib import apotik, potik, run, task
from potiklib.providers.ipe import IPEProvider


@task
def ipe_square(x: int) -> tuple[int, int]:
    import concurrent.interpreters

    return x * x, concurrent.interpreters.get_current().id


@task
def ipe_fail(msg: str) -> int:
    raise ValueError(msg)


def _user_decorator(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)

    return wrapper


@_user_decorator
def user_decorated(x: int) -> int:
    return x + 1


class CustomAwaitable:
    def __init__(self, value: int) -> None:
        self._value = value

    def __await__(self):
        async def _inner() -> int:
            await asyncio.sleep(0)
            return self._value

        return _inner().__await__()


def test_ipe_provider_with_wrapped_task_function(monkeypatch: pytest.MonkeyPatch) -> None:
    main_interp_id = concurrent.interpreters.get_current().id
    provider = IPEProvider(workers=2)
    submitted_targets: list[object] = []
    orig_submit = provider._executor.submit

    def spy_submit(fn, /, *args, **kwargs):
        submitted_targets.append(fn)
        return orig_submit(fn, *args, **kwargs)

    monkeypatch.setattr(provider._executor, "submit", spy_submit)
    try:
        res, interp_id = provider.execute(ipe_square, 9)
        assert res == 81
        assert interp_id != main_interp_id
        assert submitted_targets[-1] is getattr(ipe_square, "__wrapped__")

        with pytest.raises(ValueError, match="ipe-error"):
            provider.execute(ipe_fail, "ipe-error")

        # User decorators without _potik_task are not silently unwrapped.
        try:
            provider.execute(user_decorated, 1)
        except concurrent.interpreters.NotShareableError:
            pass
        assert submitted_targets[-1] is user_decorated

        # A closure is never cross-interpreter shareable regardless of PYTHONPATH.
        offset = 1

        def local_closure(x: int) -> int:
            return x + offset

        with pytest.raises(concurrent.interpreters.NotShareableError):
            provider.execute(local_closure, 1)
    finally:
        provider.shutdown(wait=True)


def test_gil_enabled_routes_sync_tasks_to_ipe(monkeypatch: pytest.MonkeyPatch) -> None:
    main_interp_id = concurrent.interpreters.get_current().id
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: True)

    async def main() -> tuple[tuple[int, int], tuple[int, int], tuple[int, int]]:
        r1 = ipe_square(5)
        r2 = potik(ipe_square, 6)
        r3 = await apotik(ipe_square, 7)
        return r1, r2, r3

    (v1, id1), (v2, id2), (v3, id3) = run(main, workers=2)
    assert (v1, v2, v3) == (25, 36, 49)
    assert id1 != main_interp_id
    assert id2 != main_interp_id
    assert id3 != main_interp_id


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
