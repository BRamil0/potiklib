# potiklib
[Українська версія (README.uk.md)](https://github.com/BRamil0/potiklib/blob/master/README.uk.md)

**potiklib** is a zero-dependency Python 3.15+ library that gives you a clean, transparent developer experience for parallel CPU and `asyncio` execution on both free-threaded (`No GIL`) and GIL-enabled Python builds.
## Installation
```bash
uv add potiklib

uv pip install potiklib
```
## Quick Start
```python
import asyncio
from potiklib import apotik, potik, run, task


@task
def cpu_square(x: int) -> int:
    return x * x


def cpu_cube(x: int) -> int:
    return x * x * x


async def main() -> None:
    squares = await asyncio.gather(*(apotik(cpu_square, i) for i in range(4)))
    cubes = potik(cpu_cube, 3)
    print("squares:", squares, "cube(3):", cubes)


if __name__ == "__main__":
    run(main)
```
## Usage Examples
### Parallel `async def` Tasks (`@task` + `asyncio.gather`)
```python
import asyncio
from potiklib import run, task


@task
async def async_cpu_work(n: int) -> int:
    await asyncio.sleep(0)
    total = 0
    for i in range(n):
        total = (total + i * i) % 1_000_000_007
    return total


async def main() -> list[int]:
    return await asyncio.gather(*(async_cpu_work(2_000_000) for _ in range(4)))


if __name__ == "__main__":
    print(run(main, workers=4))
```
### Synchronous `def` Functions (`potik` & `apotik`)
```python
import asyncio
from potiklib import apotik, potik, run


def compute_sum(limit: int) -> int:
    total = 0
    for i in range(limit):
        total += i
    return total


def sync_entry() -> int:
    return potik(compute_sum, 1_000_000)


async def async_entry() -> list[int]:
    return await asyncio.gather(
        apotik(compute_sum, 1_000_000),
        apotik(compute_sum, 2_000_000),
    )


if __name__ == "__main__":
    print("sync:", run(sync_entry, workers=2))
    print("async:", run(async_entry, workers=2))
```
## API Reference
| Symbol | Inside `run()` | Outside `run()` (fallback) |
| :--- | :--- | :--- |
| `run(main, *, workers=None)` | Starts the runtime (`workers` defaults to `os.process_cpu_count() or 1`), runs `def`, `async def`, coroutine, or `Awaitable`, and shuts down initialized pools in `finally`. | — |
| `@task` on `async def` | Runs in a worker thread's event loop (`ATPE`) with `Least-Loaded` balancing and `contextvars` propagation. | Awaits `fn(*args, **kwargs)` in the caller's event loop. |
| `@task` on `def` | Runs in `TPE` (No GIL) or `IPE` (GIL) and returns the result. | Calls `fn(*args, **kwargs)` directly. |
| `potik(fn, /, *args, **kwargs)` | Runs synchronous `fn` in `TPE` or `IPE` and waits for the result (with anti-deadlock fallback when no free threads remain). | Calls `fn(*args, **kwargs)` directly. |
| `await apotik(target, /, *args, **kwargs)` | Routes `Awaitable` / `async def` to `ATPE`, and synchronous `def` to `TPE` / `IPE`. | Awaits `target` / `target(*args, **kwargs)` or calls synchronous `target` directly. |

## `No GIL` vs `GIL` Routing
At runtime, `potiklib` checks `sys._is_gil_enabled()` for synchronous tasks and lazily initializes only the pools actually used:
- **Asynchronous tasks (`async def`, `Awaitable`):** Always routed to `ATPEProvider` (a `ThreadPoolExecutor` where each worker owns an `asyncio` event loop).
- **Synchronous tasks with No GIL (`sys._is_gil_enabled() == False`):** Routed to `TPEProvider` (`ThreadPoolExecutor` with `contextvars` copying and `Lock`-tracked anti-deadlock detection).
- **Synchronous tasks with GIL enabled (`sys._is_gil_enabled() == True`):** Routed to `IPEProvider` (`concurrent.futures.InterpreterPoolExecutor`) so CPU-bound work runs in parallel across subinterpreters.
> **Note:**
> When the GIL is enabled (`sys._is_gil_enabled() == True`), synchronous functions executed via `InterpreterPoolExecutor` must be picklable / cross-interpreter shareable at module top level: avoid `lambda`, local closures, default arguments that prevent sharing, or module-global references. `@task` preserves `functools.wraps(fn)` so `potiklib` can safely unwrap its own task decorator without stripping your custom decorators.

## License
MIT License — see [LICENSE.txt](https://github.com/BRamil0/potiklib/blob/master/LICENSE.txt).
