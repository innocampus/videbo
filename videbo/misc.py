import asyncio
import os
import logging
import re
from collections import OrderedDict
from pathlib import Path
from typing import Set, Any, Optional, Callable, Awaitable, Hashable, Iterable, Tuple


logger = logging.getLogger('videbo-misc')
MEGA = 1024 * 1024


async def get_free_disk_space(path: str) -> int:
    """Get free disk space in the given path. Returns MB."""
    st = await asyncio.get_running_loop().run_in_executor(None, os.statvfs, path)
    free_bytes = st.f_bavail * st.f_frsize
    return int(free_bytes / MEGA)


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
        self._task = asyncio.get_event_loop().create_task(
            self.loop(interval_seconds, limit, call_immediately),
            # name=self.task_name  # 3.8 required
        )
        TaskManager.fire_and_forget_task(self._task)

    def stop(self, msg: str = None) -> bool:
        return self._task.cancel(msg)


class SizeError(Exception):
    pass


class BytesLimitLRU(OrderedDict):
    """
    Limit object memory size, evicting the least recently accessed key when the specified maximum is exceeded.
    Only `bytes` type values are accepted. Their size is calculated by passing them into the builtin `len()` function.
    """

    def __init__(self, max_bytes: int, other: Iterable[Tuple[Hashable, bytes]] = (), **kwargs: bytes) -> None:
        """
        Uses the `dict` constructor to initialize; in addition the `.max_bytes` attribute is set and the protected
        attribute `._total_bytes` used for keeping track of the total size of the values stored is initialized.

        For details on the behaviour when adding items, refer to the `__setitem__` method.

        Since initializing a non-empty instance (i.e. using `other` or `kwargs`) makes it call the `__setitem__`
        method for each item, we introduce a private flag to prevent initializing with a total bytes size that exceeds
        the specified maximum. Otherwise some of the initial items would just be silently removed/replaced by others.
        This could cause unexpected behaviour, so the `__setitem__` method checks if the object is still initializing,
        if the maximum size is exceeded after adding an element, and raises an error in that case.
        """
        self.max_bytes = max_bytes
        self._total_bytes = 0
        self.__initializing = True
        super().__init__(other, **kwargs)
        self.__initializing = False

    def __delitem__(self, key: Hashable) -> None:
        """Usage is `del instance[key]` as with other mapping classes."""
        self._total_bytes -= len(self[key])
        super().__delitem__(key)

    def __getitem__(self, key: Hashable) -> bytes:
        """Usage is `instance[key]` as with other mapping classes."""
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def __setitem__(self, key: Hashable, value: bytes) -> None:
        """
        Calling `instance[key] = value` adds a key-value-pair as with other mapping classes.
        Only accepts `bytes` objects as values; raises a `TypeError` otherwise.
        If the length/size of the provided `value` exceeds `.max_bytes` a `SizeError` is raised.
        The internal `._total_bytes` attribute is updated.
        If the key existed before and just the value is replaced, the item is treated as most recently accessed and
        thus moved to the end of the internal linked list.
        If after adding the item `._total_bytes` exceeds `.max_bytes`, items are deleted in order from least to most
        recently accessed until the total size (in bytes) is in line with the specified maximum.
        """
        if not isinstance(value, bytes):
            raise TypeError("Not a bytes object:", repr(value))
        size = len(value)
        if size > self.max_bytes:
            # If we allowed this, the loop at the end would remove all items from the dictionary,
            # so we raise an error to allow exceptions for this case.
            raise SizeError(f"Item itself exceeds maximum allowed size of {self.max_bytes} bytes")
        # Update `_total_bytes` depending on whether the key existed already or not;
        # if so, calling `__getitem__` to determine the current size also ensures that the key is moved to the end.
        self._total_bytes += size - len(self[key]) if key in self else size
        # Now actually set the new value (and possibly new key):
        super().__setitem__(key, value)
        if self._total_bytes <= self.max_bytes:
            return
        if self.__initializing:
            raise SizeError(f"Total bytes of {self.max_bytes} exceeded")
        # If size is too large now, remove items until it is less than or equal to the defined maximum
        while self._total_bytes > self.max_bytes:
            # Delete the current oldest item, by instantiating an iterator over all keys (in order)
            # and passing its next item (i.e. the first one in order) to `__delitem__`
            del self[next(iter(self))]

    @property
    def total_bytes(self) -> int: return self._total_bytes

    @property
    def space_left(self) -> int: return self.max_bytes - self._total_bytes


async def gather_in_batches(batch_size: int, *aws, return_exceptions: bool = False) -> list:
    results = []
    for idx in range(0, len(aws), batch_size):
        results += await asyncio.gather(*aws[idx:idx + batch_size], return_exceptions=return_exceptions)
    return results
