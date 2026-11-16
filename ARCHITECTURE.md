### Структура файлів

```text
src/potiklib/
├── __init__.py          # Публічний API
├── py.typed             # PEP 561
├── _main.py             # Головний файл
└── providers/
    ├── __init__.py
    ├── base.py          # Protocol
    ├── atpe.py          # Async ThreadPoolExecutor
    ├── tpe.py           # ThreadPoolExecutor
    └── ipe.py           # InterpreterPoolExecutor
```

### Публічне API
- `potiklib.run(функція/корутина)` — Запускає пул потоків.
- `potiklib.potik()` — Викликає будь-яке синхронне завдання в іншому потоці.
- `potiklib.apotik()` — Викликає будь-яке асинхронне завдання в іншому потоці.
- `@potiklib.task` — Робить автоматичне підключення `potik/apotik`, якщо викликали без `run` то просто працює наче без `task`.

| Виклик| Зі `run()` | Без `run()` (без пулу потоків) |
| :--- | :--- | :--- |
| `@task` над `async def` | Виконується в циклі подій (event loop) одного з виконавців (worker)| Звичайний `await fn(...)` у головному циклу подій |
| `@task` над `def` | Виконується в потоці пулу і повертає результат | Звичайний прямий виклик `fn(...)` |
| `await apotik(fn, ...)` | Працює і з `async def`, і з `def`, повертає awaitable | Виконує в поточному контексті (`await` або прямий виклик) |
| `potik(fn, ...)`| Запускає синхронну `def` у пулі й чекає результат  | Звичайний прямий виклик `fn(...)` |

### Розділення потоків для `async def` і синхронного `def`
- Асинхронні: `async def` і `await apotik(async_fn)` -> `ThreadPoolExecutor` з циклом подій (`event loop`)
- Синхронні (Без GIL): `def`, `potik(sync_fn)` і `await apotik(sync_fn)` -> `ThreadPoolExecutor`
- Синхронні `GIL` (`sys._is_gil_enabled()` дає `True`) завжди користуємося — `InterpreterPoolExecutor`.

Створювати `InterpreterPoolExecutor` та `ThreadPoolExecutor` тільки при першому виклику.

### Балансувальник + `contextvars` + Anti-deadlock
1. **Балансир `Least-Loaded` замість сліпого `Round-Robin`:**  
   Кожен воркер тримає лічильник `in_flight` (скільки задач зараз на ньому). Нова асинхронна задача летить виконавцю з мінімальним `in_flight`. У випадку якщо задача «важка» і зависла на 5 секунд, інші не будуть ставати за нею в чергу.
2. **Чесне копіювання `contextvars` у воркер:**  
   `asyncio.run_coroutine_threadsafe` не переносить `contextvars` автоматично. Треба робити так: у головному потоці `ctx = contextvars.copy_context()`, а у воркер-лупі планувати створення `Task` через `loop.call_soon_threadsafe(lambda: ctx.run(create_task_and_link_future))`.
3. **Запобіжник від дедлоку (вкладені виклики):**  
   Через `threading.local` маркуємо кожен потік пулу.
   * Якщо асинхронна функція уже всередині воркера викликає іншу `@task async def` — вона не блокується, але все одно варто мати політику: або виконувати в поточному циклу подій (мінімум накладних витрат), або кидати на найменш завантажений виконавець.
   * Якщо синхронна функція уже всередині `sync_pool` робить вкладений `potik(...)` (що блокує очікування), а вільних потоків немає — **одразу виконуємо локально в поточному потоці**, щоб не отримати мертвий дедлок.

### Життєвий цикл `run()` і Graceful Shutdown
Агент має реалізувати `run(main, *, workers=None)` за суворим алгоритмом:
1. Підняти виконавців, дочекатися через `threading.Event`, що **всі** `worker_loop` реально запустилися (`run_forever()`).
2. Записати `Runtime` у `ContextVar` (і глобальний токен, якщо `run` — єдина точка входу).
3. Запустити `main` (якщо `async def` — через `asyncio.run`, якщо `def` — напряму).
4. У блоці `finally` (навіть після винятку або `Ctrl+C`):
   * Перевести стан у `closing = True`.
   * Почекати завершення активних задач у воркерах.
   * У кожному виконавцю викликати `loop.call_soon_threadsafe(loop.stop)`, потім у самому потоці виконавця виконати `loop.run_until_complete(loop.shutdown_asyncgens())` та `loop.close()`.