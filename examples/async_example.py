import asyncio
import sys
import time

from potiklib import apotik, run, task


@task
async def async_cpu_work(n: int) -> int:
    await asyncio.sleep(0)
    total = 0
    for i in range(n):
        total = (total + i * i) % 1_000_000_007
    return total


async def benchmark(iterations: int, count: int) -> list[int]:
    return await asyncio.gather(*(async_cpu_work(iterations) for _ in range(count)))


async def main_coro() -> None:
    iterations = 3_000_000
    count = 4

    t0 = time.perf_counter()
    results = await benchmark(iterations, count)
    t_par = time.perf_counter() - t0
    print(f"Async parallel (@task async def, workers=4): {t_par:.3f}s (results={results})")

    t1 = time.perf_counter()
    single = await apotik(async_cpu_work, iterations)
    t_apotik = time.perf_counter() - t1
    print(f"Single apotik(async_cpu_work): {t_apotik:.3f}s (result={single})")


if __name__ == "__main__":
    print("GIL enable:", sys._is_gil_enabled())

    t_seq_start = time.perf_counter()
    seq_results = asyncio.run(benchmark(3_000_000, 4))
    t_seq = time.perf_counter() - t_seq_start
    print(f"Async fallback (without potiklib.run): {t_seq:.3f}s (results={seq_results})")

    t_run_start = time.perf_counter()
    run(main_coro, workers=4)
    print(f"Total potiklib.run() elapsed: {time.perf_counter() - t_run_start:.3f}s")
