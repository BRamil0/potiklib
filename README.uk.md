# potiklib
[English version (README.md)](https://github.com/BRamil0/potiklib/blob/master/README.md)

**potiklib** — бібліотека для Python 3.15+ без зовнішніх залежностей, яка забезпечує прозорий DX для паралельного виконання CPU-задач та `asyncio`-коду як у free-threaded збірках (`No GIL`), так і зі стандартним `GIL`.
## Встановлення
```bash
uv add potiklib

uv pip install potiklib
```
## Швидкий старт
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
## Приклади використання
### Паралельні `async def` задачі (`@task` + `asyncio.gather`)
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
### Синхронні `def` функції (`potik` і `apotik`)
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
## Коротка довідка API
| Виклик | Усередині `run()` | Без `run()` (фолбек) |
| :--- | :--- | :--- |
| `run(main, *, workers=None)` | Піднімає рантайм (`workers` за замовчуванням `os.process_cpu_count() or 1`), запускає `def`, `async def`, корутину або `Awaitable`, і в `finally` завершує лише ініціалізовані пули. | — |
| `@task` над `async def` | Виконується в циклі подій одного з воркерів (`ATPE`) з балансуванням `Least-Loaded` і перенесенням `contextvars`. | Звичайний `await fn(*args, **kwargs)` у поточному циклі подій. |
| `@task` над `def` | Виконується в `TPE` (без GIL) або `IPE` (з GIL) і повертає результат. | Прямий виклик `fn(*args, **kwargs)`. |
| `potik(fn, /, *args, **kwargs)` | Запускає синхронну `fn` у `TPE` або `IPE` й чекає результат (із anti-deadlock фолбеком, якщо вільних потоків немає). | Прямий виклик `fn(*args, **kwargs)`. |
| `await apotik(target, /, *args, **kwargs)` | `Awaitable` / `async def` -> `ATPE`; синхронна `def` -> `TPE` або `IPE`. | Виконує в поточному контексті (`await` або прямий виклик). |

## Маршрутизація `No GIL` та `GIL`
Під час роботи `potiklib` перевіряє `sys._is_gil_enabled()` для синхронних задач і ліниво ініціалізує лише ті пули, які реально знадобилися:
- **Асинхронні задачі (`async def`, `Awaitable`):** завжди виконуються в `ATPEProvider` (`ThreadPoolExecutor`, де кожен потік має власний `asyncio` event loop).
- **Синхронні задачі без GIL (`sys._is_gil_enabled() == False`):** виконуються в `TPEProvider` (`ThreadPoolExecutor` із копіюванням `contextvars` та anti-deadlock лічильником під `threading.Lock`).
- **Синхронні задачі з увімкненим GIL (`sys._is_gil_enabled() == True`):** виконуються в `IPEProvider` (`concurrent.futures.InterpreterPoolExecutor`), що дозволяє паралелити CPU-обчислення у субінтерпретаторах.
> **Примітка:**
> При увімкненому GIL (`sys._is_gil_enabled() == True`) синхронні функції для `InterpreterPoolExecutor` мають бути picklable та визначеними на рівні модуля (top-level): уникайте `lambda`, локальних замикань, значень аргументів за замовчуванням чи звернень до модульних глобальних змінних. `@task` зберігає `functools.wraps(fn)`, завдяки чому `potiklib` розгортає лише власну обгортку без зняття сторонніх декораторів користувача.
## Ліцензія
MIT License — дивіться [LICENSE.txt](https://github.com/BRamil0/potiklib/blob/master/LICENSE.txt).