import asyncio
from contextvars import ContextVar

from potiklib import apotik, potik, run, task

request_id_var: ContextVar[str] = ContextVar("request_id", default="unset")


@task
def read_and_mutate_sync(new_val: str) -> str:
    seen = request_id_var.get()
    request_id_var.set(new_val)
    return seen


@task
async def read_and_mutate_async(new_val: str) -> str:
    await asyncio.sleep(0)
    seen = request_id_var.get()
    request_id_var.set(new_val)
    return seen


def test_contextvars_propagated_and_isolated() -> None:
    async def main() -> None:
        token = request_id_var.set("req-123")
        try:
            assert await read_and_mutate_async("mutated-async") == "req-123"
            assert request_id_var.get() == "req-123"

            async def via_coro() -> str:
                await asyncio.sleep(0)
                seen = request_id_var.get()
                request_id_var.set("mutated-coro")
                return seen

            assert await apotik(via_coro()) == "req-123"
            assert request_id_var.get() == "req-123"

            assert read_and_mutate_sync("mutated-sync") == "req-123"
            assert request_id_var.get() == "req-123"

            assert potik(read_and_mutate_sync, "mutated-potik") == "req-123"
            assert request_id_var.get() == "req-123"

            assert await apotik(read_and_mutate_sync, "mutated-apotik-sync") == "req-123"
            assert request_id_var.get() == "req-123"
        finally:
            request_id_var.reset(token)

    run(main, workers=2)
