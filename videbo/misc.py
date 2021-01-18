import asyncio
import os
import logging
import re
from typing import Set, Any, Optional, Callable, Awaitable

logger = logging.getLogger('videbo-misc')


async def get_free_disk_space(path: str) -> int:
    """Get free disk space in the given path. Returns MB."""
    st = await asyncio.get_running_loop().run_in_executor(None, os.statvfs, path)
    free_bytes = st.f_bavail * st.f_frsize
    return int(free_bytes / 1024 / 1024)


def sanitize_filename(filename: str) -> str:
    filename = re.sub(r"[^\w \d\-_~,;\[\]().]", "", filename, 0, re.ASCII)  # \w should only match ASCII letters
    filename = re.sub(r"[.]{2,}", ".", filename)
    return filename


class TaskManager:
    _tasks: Set[asyncio.Task] = set()

    @classmethod
    async def cancel_all(cls, app: Any):
        logger.info(f"TaskManager: cancel all remaining {len(cls._tasks)} tasks")
        for task in cls._tasks:
            task.cancel()

    @classmethod
    def fire_and_forget_task(cls, task: asyncio.Task) -> None:
        """Checks if there was an exception in the task when the task ends."""
        def task_done(_future: Any):
            try:
                # This throws an exception if there was any in the task.
                task.result()
            except asyncio.CancelledError:
                pass
            except:
                logger.exception("Error occurred in an fire-and-forget task.")
            finally:
                cls._tasks.remove(task)

        cls._tasks.add(task)
        task.add_done_callback(task_done)


class Periodic:
    def __init__(self, _async_func: Callable[..., Awaitable], *args, **kwargs) -> None:
        self.async_func = _async_func
        self.args = args
        self.kwargs = kwargs
        self.task_name: str = f'periodic-{self.async_func.__name__}'
        self._task: Optional[asyncio.Task] = None

    async def loop(self, interval_seconds: int, limit: int = None, call_immediately: bool = False) -> None:
        if not call_immediately:
            await asyncio.sleep(interval_seconds)
        i = 0
        while limit is None or i < limit:
            await self.async_func(*self.args, **self.kwargs)
            await asyncio.sleep(interval_seconds)
            i += 1

    def __call__(self, interval_seconds: int, limit: int = None, call_immediately: bool = False) -> None:
        self._task = asyncio.get_event_loop().create_task(self.loop(interval_seconds, limit, call_immediately),
                                                          name=self.task_name)
        TaskManager.fire_and_forget_task(self._task)

    def stop(self, msg: str = None) -> bool:
        return self._task.cancel(msg)


async def gather_in_batches(batch_size: int, *aws, return_exceptions: bool = False) -> list:
    results = []
    for idx in range(0, len(aws), batch_size):
        results += await asyncio.gather(*aws[idx:idx + batch_size], return_exceptions=return_exceptions)
    return results
