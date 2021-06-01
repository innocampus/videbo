import asyncio
import os
import logging
import re
import sys
from collections import OrderedDict
from pathlib import Path
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


def rel_path(filename: str) -> Path:
    """
    Returns a relative path from a file's name.
    The path starts with a directory named with the first two characters of the filename followed by the file itself.
    """
    if len(Path(filename).parts) != 1:
        raise ValueError(f"'{filename}' is not a valid filename")
    return Path(filename[:2], filename)


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


class SizeError(Exception):
    pass


class MemorySizeLRU(OrderedDict):
    """Limit object memory size, evicting the least recently looked-up key when full."""
    def __init__(self, max_bytes: int, /, *args, **kwargs):
        self.max_bytes = max_bytes
        super().__init__(*args, **kwargs)
        if sys.getsizeof(self) > self.max_bytes:
            raise SizeError(f"Items exceed maxsize of {self.max_bytes} bytes")

    def __getitem__(self, key):
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        if sys.getsizeof(self) > 2 * self.max_bytes:
            # If size is more than twice the maximum, the new item itself must be larger than the maximum;
            # if we allowed this, the following loop would remove all items from the dictionary,
            # so we remove the item and raise an error to allow exceptions for this case.
            del self[key]
            raise SizeError(f"Item itself exceeds maxsize of {self.max_bytes} bytes")
        # If size is too large now, remove items until it is less than or equal to `maxsize`
        iterator = iter(self)
        while sys.getsizeof(self) > self.max_bytes:
            del self[next(iterator)]  # delete oldest item


async def gather_in_batches(batch_size: int, *aws, return_exceptions: bool = False) -> list:
    results = []
    for idx in range(0, len(aws), batch_size):
        results += await asyncio.gather(*aws[idx:idx + batch_size], return_exceptions=return_exceptions)
    return results
