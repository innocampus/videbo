import logging
from asyncio import Task, create_task, sleep
from collections.abc import Awaitable, Callable
from inspect import isawaitable
from time import time
from typing import Any, Optional, Union

from videbo.exceptions import NoRunningTask
from .task_manager import TaskManager


StopCallbackT = Callable[[], Union[Awaitable[None], None]]

log = logging.getLogger(__name__)


class Periodic:
    """Provides a simple utility for launching (and stopping) the periodic execution of asynchronous tasks."""
    def __init__(self, __async_func__: Callable[..., Awaitable[None]], /, *args: Any, **kwargs: Any) -> None:
        """
        To initialize, simply provide the async function object and the positional and/or keyword arguments with which
        it should be called every time.
        """
        self.async_func = __async_func__
        self.args = args
        self.kwargs = kwargs
        self.task_name: str = f'periodic-{self.async_func.__name__}'  # for convenience
        self._task: Optional[Task[None]] = None  # initialized upon starting periodic execution
        self.pre_stop_callbacks: list[StopCallbackT] = []
        self.post_stop_callbacks: list[StopCallbackT] = []

    async def loop(self, interval_seconds: float, limit: Optional[int] = None, call_immediately: bool = False) -> None:
        """
        The main execution loop that can be repeated indefinitely or a limited number of times.
        Normally, this function should not be called from the outside directly.

        Args:
            interval_seconds:
                The time in seconds to wait in between two executions
            limit (optional):
                If provided an integer, the number of executions will not exceed this value
            call_immediately (optional):
                If `False`, the first execution will only happen after the specified time interval has elapsed;
                if `True`, it will happen immediately and only the following executions will honor the interval.
        """
        exec_time = interval_seconds if call_immediately else 0
        i = 0
        while limit is None or i < limit:
            await sleep(interval_seconds - exec_time)
            started = time()
            await self.async_func(*self.args, **self.kwargs)
            i += 1
            exec_time = time() - started

    def __call__(self, interval_seconds: float, limit: Optional[int] = None, call_immediately: bool = False) -> None:
        """Starts the execution loop (see above) with the provided options."""
        self._task = create_task(
            self.loop(
                interval_seconds,
                limit=limit,
                call_immediately=call_immediately,
            ),
            name=self.task_name,
        )
        TaskManager.fire_and_forget_task(self._task)

    async def stop(self) -> bool:
        """Stops the execution loop and calls all provided callback functions."""
        if self._task is None:
            raise NoRunningTask
        log.debug(f"Stopping {self.task_name}...")
        await self._run_callbacks(self.pre_stop_callbacks)
        out = self._task.cancel()
        self._task = None
        await self._run_callbacks(self.post_stop_callbacks)
        log.info(f"Stopped {self.task_name}")
        return out

    @staticmethod
    async def _run_callbacks(callbacks: list[StopCallbackT]) -> None:
        """Taking into account if the callback functions require the `await` syntax, they are executed in order."""
        for func in callbacks:
            out = func()
            if isawaitable(out):
                await out
