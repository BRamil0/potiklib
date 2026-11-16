import asyncio
import sys
import time

from potiklib import apotik, potik, run, task


@task
def heavy_compute(n: int) -> int:
    total = 0
    for i in range(n):
        total = (total + i * i) % 1_000_000_007
    return total


def plain_compute(n: int) -> int:
    total = 0
    for i in range(n):
        total = (total + i * i) % 1_000_000_007
    return total


async def parallel_sync_main(iterations: int, count: int) -> list[int]:
    return await asyncio.gather(*(apotik(heavy_compute, iterations) for _ in range(count)))


def main() -> None:
    print("GIL enable:", sys._is_gil_enabled())

    iterations = 3_000_000
    count = 4

    # 1. Fallback without run(): executes sequentially in the current thread
    t0 = time.perf_counter()
    seq_results = [heavy_compute(iterations) for _ in range(count)]
    t_seq = time.perf_counter() - t0
    print(f"Sequential (without run): {t_seq:.3f}s")

    # 2. With potiklib.run(): executes across TPE (free-threaded) or IPE (GIL enabled)
    t1 = time.perf_counter()
    par_results = run(parallel_sync_main(iterations, count), workers=4)
    t_par = time.perf_counter() - t1
    print(f"Parallel (with run, workers=4): {t_par:.3f}s")

    assert seq_results == par_results

    # 3. Direct potik() call inside sync main
    t2 = time.perf_counter()
    single = run(lambda: potik(plain_compute, iterations), workers=2)
    t_single = time.perf_counter() - t2
    print(f"Single potik() inside run(): {t_single:.3f}s (result={single})")


if __name__ == "__main__":
    main()
