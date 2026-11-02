"""Keep storage ownership until a worker has actually stopped writing."""
import asyncio


async def await_storage_task(task, *, on_cancel=None):
    """Repeated cancellation never abandons an active storage owner."""
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        if on_cancel is not None:
            on_cancel()
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        try:
            task.result()
        except BaseException:
            pass
        raise


async def storage_worker(operation, *args, on_cancel=None, **kwargs):
    task = asyncio.create_task(asyncio.to_thread(operation, *args, **kwargs))
    return await await_storage_task(task, on_cancel=on_cancel)
