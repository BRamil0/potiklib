import asyncio
import threading

from potiklib import apotik, potik, run, task


def test_nested_sync_calls_single_worker_no_deadlock() -> None:
    @task
    def leaf(x: int) -> tuple[int, int]:
        return x * 2, threading.get_ident()

    @task
    def middle(x: int) -> tuple[int, int, int]:
        val, leaf_tid = potik(leaf, x + 1)
        return val, leaf_tid, threading.get_ident()

    @task
    def outer(x: int) -> tuple[int, int, int, int]:
        val, leaf_tid, mid_tid = middle(x)
        return val, leaf_tid, mid_tid, threading.get_ident()

    def main() -> None:
        main_tid = threading.get_ident()
        val, leaf_tid, mid_tid, outer_tid = outer(4)
        assert val == 10
        assert outer_tid != main_tid
        # With workers=1, no free thread exists, so nested calls execute locally.
        assert mid_tid == outer_tid
        assert leaf_tid == outer_tid

    run(main, workers=1)


def test_nested_sync_calls_with_free_worker_and_full_pool() -> None:
    @task
    def inner(x: int) -> tuple[int, int]:
        return x + 1, threading.get_ident()

    @task
    def outer(x: int) -> tuple[int, int, int]:
        val, inner_tid = potik(inner, x)
        return val, inner_tid, threading.get_ident()

    def main() -> None:
        val, inner_tid, outer_tid = outer(10)
        assert val == 11
        # With workers=2 and 1 outer task running, 1 worker is free -> inner uses 2nd thread.
        assert inner_tid != outer_tid

    run(main, workers=2)


def test_nested_sync_calls_when_all_workers_busy_no_deadlock() -> None:
    barrier_before = threading.Barrier(2, timeout=2.0)
    barrier_after = threading.Barrier(2, timeout=2.0)

    @task
    def leaf(x: int) -> tuple[int, int]:
        return x + 100, threading.get_ident()

    @task
    def busy_worker(x: int) -> tuple[int, int, int]:
        barrier_before.wait()
        val, leaf_tid = potik(leaf, x)
        barrier_after.wait()
        return val, leaf_tid, threading.get_ident()

    async def main() -> tuple[tuple[int, int, int], tuple[int, int, int]]:
        return await asyncio.gather(apotik(busy_worker, 1), apotik(busy_worker, 2))

    res = run(main, workers=2)
    assert res[0][0] == 101
    assert res[1][0] == 102
    # Both workers were busy at barrier, so nested potik() ran locally without deadlocking.
    assert res[0][1] == res[0][2]
    assert res[1][1] == res[1][2]


def test_nested_async_calls_single_worker_no_deadlock() -> None:
    @task
    async def inner_async(x: int) -> tuple[int, int]:
        await asyncio.sleep(0)
        return x + 5, threading.get_ident()

    @task
    async def outer_async(x: int) -> tuple[int, int, int]:
        val1, tid1 = await inner_async(x)
        val2, tid2 = await apotik(inner_async, val1)
        assert tid1 == tid2
        return val2, tid1, threading.get_ident()

    async def main() -> None:
        main_tid = threading.get_ident()
        val, inner_tid, outer_tid = await outer_async(3)
        assert val == 13
        assert outer_tid != main_tid
        assert inner_tid == outer_tid

    run(main, workers=1)


def test_cross_pool_sync_async_nesting_no_deadlock() -> None:
    @task
    async def async_leaf(x: int) -> int:
        await asyncio.sleep(0)
        return x + 1

    @task
    def sync_calling_async(x: int) -> int:
        return asyncio.run(async_leaf(x))

    async def main() -> int:
        return await apotik(sync_calling_async, 41)

    assert run(main, workers=1) == 42
