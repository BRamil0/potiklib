### Структура файлів

```text
src/potiklib/
├── __init__.py          # Публічний API
├── py.typed             # PEP 561
├── _main.py             # Головний файл (Runtime, run, potik, apotik, task)
└── providers/
    ├── __init__.py
    ├── base.py          # Protocol (Provider, SyncProvider, AsyncProvider)
    ├── atpe.py          # Async ThreadPoolExecutor
    ├── tpe.py           # ThreadPoolExecutor
    └── ipe.py           # InterpreterPoolExecutor
```

Реалізація розрахована на Python 3.15+ (PEP 695 синтаксис типізації, пряме використання `sys._is_gil_enabled()` та `concurrent.futures.InterpreterPoolExecutor`).

### Публічне API
- `potiklib.run(main, *, workers=None)` — Запускає рантайм; приймає функцію (`def` / `async def`), вже створену корутину (`run(main())`) або `Awaitable`. Дефолт: `workers = os.process_cpu_count() or 1`.
- `potiklib.potik(fn, *args, **kwargs)` — Викликає синхронне завдання у синхронному пулі та очікує результат.
- `potiklib.apotik(target, *args, **kwargs)` — Асинхронна обгортка (`@overload` з `Awaitable` першим): приймає `Awaitable`, `async def` або синхронну `def`.
- `@potiklib.task` — Декоратор із `functools.wraps(fn)` та маркером `_potik_task`, що автоматично підключає `potik`/`apotik`, а без `run()` працює як звичайний виклик.

| Виклик | Зі `run()` | Без `run()` (без пулу потоків) |
| :--- | :--- | :--- |
| `@task` над `async def` | Виконується в циклі подій (event loop) одного з виконавців (`atpe`) | Звичайний `await fn(...)` у головному циклі подій |
| `@task` над `def` | Виконується в синхронному пулі (`tpe` або `ipe`) і повертає результат | Звичайний прямий виклик `fn(...)` |
| `await apotik(target, ...)` | `Awaitable`/`async def` -> `atpe`; `def` -> `tpe` або `ipe` | Виконує в поточному контексті (`await` або прямий виклик) |
| `potik(fn, ...)` | Запускає синхронну `def` у `tpe`/`ipe` й чекає результат | Звичайний прямий виклик `fn(...)` |

### Розділення потоків для `async def` і синхронного `def` та лінива ініціалізація
- Асинхронні: `async def`, `Awaitable` і `await apotik(async_fn)` -> `ATPEProvider` (`ThreadPoolExecutor` з циклом подій у кожному воркері).
- Синхронні (без GIL, `sys._is_gil_enabled() == False`): `def`, `potik(sync_fn)` і `await apotik(sync_fn)` -> `TPEProvider` (`ThreadPoolExecutor`).
- Синхронні (з GIL, `sys._is_gil_enabled() == True`): завжди `IPEProvider` (`InterpreterPoolExecutor`). У `ipe.py` розгортання `fn.__wrapped__` виконується лише якщо `getattr(fn, "_potik_task", False)` (щоб зберігати `pickle`/cross-interpreter сумісність без зняття сторонніх декораторів користувача).

Кожен провайдер (`atpe`, `tpe`, `ipe`) створюється ліниво під `threading.Lock` лише при першому зверненні. У `finally` методу `run()` викликається `shutdown(wait=True)` тільки для тих провайдерів, які були ініціалізовані.

### Балансувальник + `contextvars` + Anti-deadlock
1. **Балансир `Least-Loaded` замість `Round-Robin` (`atpe.py`):**  
   Кожен воркер тримає лічильник `in_flight`. Нова асинхронна задача направляється виконавцю з мінімальним `in_flight`. При завершенні задачі лічильник `in_flight` зменшується **до** розв'язання `Future`, щоб наступні задачі одразу бачили звільненого воркера.
2. **Чесне копіювання `contextvars` та скасування (`atpe.py`, `tpe.py`):**  
   - У `atpe.py`: `ctx = contextvars.copy_context()`, у воркер-лупі планується створення `Task` через `worker_loop.call_soon_threadsafe(ctx.run, _create_task_and_link)`, із зв'язуванням через `concurrent.futures.Future` та прокиданням скасування (`asyncio.wait_for`) у задачу воркера.
   - У `tpe.py`: `ctx = contextvars.copy_context()` передається у воркер через `ctx.run(fn, *args, **kwargs)`.
3. **Запобіжник від дедлоку (вкладені виклики):**  
   Через `threading.local` маркуються потоки пулів.
   * У `atpe.py`: вкладені виклики всередині воркера виконуються напряму в поточному циклі подій (`await fn(...)` / `await awaitable`).
   * У `tpe.py`: під `threading.Lock` ведеться власний лічильник активних задач `_active_tasks` (без доступу до приватних полів `ThreadPoolExecutor`). Якщо синхронна функція вже всередині `tpe` робить вкладений виклик, а вільних потоків немає (`_active_tasks >= _max_workers`), задача одразу виконується локально в поточному потоці.

### Життєвий цикл `run()` і Graceful Shutdown
1. `Runtime` зберігається у `ContextVar` (та глобальному fallback для потоків, створених вручну), після чого запускається `main` (`asyncio.run` для `async def`/корутини/`Awaitable` або прямий виклик для `def`).
2. Під час першого виклику `ATPEProvider` піднімає воркерів і чекає через `threading.Event`, що всі `worker_loop` реально увійшли в `run_forever()` (`loop.call_soon(worker.ready.set)`).
3. У блоці `finally` (навіть після винятку або `Ctrl+C`) відновлюються `ContextVar`/глобальний рантайм і виконується `runtime.shutdown()`:
   * Якщо `atpe` був ініціалізований: встановлюється `closing = True`, очікується завершення активних задач (`_total_in_flight == 0`), для кожного воркера викликається `loop.call_soon_threadsafe(loop.stop)`, а у самому потоці воркера виконуються скасування залишкових задач (за потреби), `loop.run_until_complete(loop.shutdown_asyncgens())` та `loop.close()`.
   * Після цього встановлюється `_closing = True` у `Runtime` і викликається `shutdown(wait=True)` для ініціалізованих `tpe` та `ipe`.