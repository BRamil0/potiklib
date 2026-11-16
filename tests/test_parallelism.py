import asyncio
import os
import threading
import time

import pytest

from potiklib import apotik, run, task


def _cpu_burn(iters: int) -> int:
    acc = 0
    for i in range(iters):
        acc = (acc * 1664525 + 1013904223 + i) & 0xFFFFFFFF
    return acc


def _calibrate_cpu_burn(target_seconds: float = 0.25) -> tuple[int, float]:
    # Warm up before measuring so calibration reflects steady-state CPU speed.
    _cpu_burn(50_000)
    iters = 200_000
    while True:
        start = time.perf_counter()
        _cpu_burn(iters)
        elapsed = time.perf_counter() - start
        if elapsed >= 0.05:
            break
        iters *= 4
    scaled = max(1, int(iters * (target_seconds / elapsed)))
    start = time.perf_counter()
    _cpu_burn(scaled)
    single_time = time.perf_counter() - start
    return scaled, single_time


@task
def cpu_task(iters: int) -> int:
    return _cpu_burn(iters)


def test_cpu_tasks_run_in_parallel_by_timing() -> None:
    cpu_count = os.process_cpu_count() or 1
    if cpu_count < 2:
        pytest.skip("Need at least 2 CPUs to verify parallel CPU speedup")

    workers = min(4, cpu_count)
    iters, single_time = _calibrate_cpu_burn(0.25)

    async def main() -> tuple[list[int], float]:
        # Warm up pool threads before timing parallel CPU work.
        await asyncio.gather(*(apotik(lambda: 0) for _ in range(workers)))
        t0 = time.perf_counter()
        results = await asyncio.gather(*(apotik(cpu_task, iters) for _ in range(workers)))
        return results, time.perf_counter() - t0

    results, parallel_time = run(main, workers=workers)

    expected = _cpu_burn(iters)
    assert results == [expected] * workers
    # Parallel execution across `workers` threads should be much faster than sequential.
    assert parallel_time < single_time * workers * 0.7


def test_atpe_least_loaded_avoids_blocked_worker() -> None:
    started = threading.Event()
    release = threading.Event()

    async def blocking_async() -> str:
        started.set()
        # Intentionally block the worker thread's event loop until fast tasks finish.
        release.wait(timeout=2.0)
        return "blocked_done"

    async def fast_async(x: int) -> int:
        await asyncio.sleep(0.005)
        return x * 2

    async def main() -> tuple[list[int], float, str]:
        slow_task = asyncio.create_task(apotik(blocking_async))
        while not started.is_set():
            await asyncio.sleep(0.005)

        t0 = time.perf_counter()
        fast_results: list[int] = []
        for i in range(5):
            fast_results.append(await apotik(fast_async, i))
        fast_elapsed = time.perf_counter() - t0

        release.set()
        slow_result = await slow_task
        return fast_results, fast_elapsed, slow_result

    fast_results, fast_elapsed, slow_result = run(main, workers=2)
    assert fast_results == [0, 2, 4, 6, 8]
    assert slow_result == "blocked_done"
    # Fast tasks must land on the idle worker rather than queueing behind the blocked worker.
    assert fast_elapsed < 0.25
