import asyncio
import threading

from potiklib import apotik, potik, task


@task
def sync_add(a: int, b: int) -> tuple[int, int]:
    """Add two integers and return result with thread id."""
    return a + b, threading.get_ident()


@task
async def async_add(a: int, b: int) -> tuple[int, int]:
    """Async add two integers and return result with thread id."""
    await asyncio.sleep(0)
    return a + b, threading.get_ident()


def test_task_preserves_functools_wraps_metadata() -> None:
    assert sync_add.__name__ == "sync_add"
    assert sync_add.__doc__ == "Add two integers and return result with thread id."
    assert hasattr(sync_add, "__wrapped__")

    assert async_add.__name__ == "async_add"
    assert async_add.__doc__ == "Async add two integers and return result with thread id."
    assert hasattr(async_add, "__wrapped__")


def test_sync_fallback_without_run() -> None:
    caller_tid = threading.get_ident()
    res, tid = sync_add(2, 3)
    assert res == 5
    assert tid == caller_tid

    res2, tid2 = potik(lambda x, y: (x * y, threading.get_ident()), 4, 5)
    assert res2 == 20
    assert tid2 == caller_tid


async def test_async_fallback_without_run() -> None:
    caller_tid = threading.get_ident()

    res, tid = await async_add(10, 20)
    assert res == 30
    assert tid == caller_tid

    async def inner_async(x: int) -> tuple[int, int]:
        await asyncio.sleep(0)
        return x + 1, threading.get_ident()

    res2, tid2 = await apotik(inner_async, 7)
    assert res2 == 8
    assert tid2 == caller_tid

    res3, tid3 = await apotik(inner_async(9))
    assert res3 == 10
    assert tid3 == caller_tid

    res4, tid4 = await apotik(lambda x: (x * 2, threading.get_ident()), 6)
    assert res4 == 12
    assert tid4 == caller_tid
