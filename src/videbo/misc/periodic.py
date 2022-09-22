from asyncio import Task, get_event_loop, sleep
from collections.abc import Awaitable, Callable
from inspect import isawaitable
from time import time
from typing import Any, Optional

from videbo.exceptions import NoRunningTask
from .task_manager import TaskManager


StopCallbackT = Callable[[], Any]


class Periodic:
    """Provides a simple utility for launching (and stopping) the periodic execution of asynchronous tasks."""
    def __init__(self, _async_func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> None:
        """
        To initialize, simply provide the async function object and the positional and/or keyword arguments with which
        it should be called every time.
        """
        self.async_func = _async_func
        self.args = args
        self.kwargs = kwargs
        self.task_name: str = f'periodic-{self.async_func.__name__}'  # for convenience
        self._task: Optional[Task[Any]] = None  # initialized upon starting periodic execution
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
        self._task = get_event_loop().create_task(
            self.loop(interval_seconds, limit, call_immediately),
            name=self.task_name
        )
        TaskManager.fire_and_forget_task(self._task)

    async def stop(self) -> bool:
        """Stops the execution loop and calls all provided callback functions."""
        if self._task is None:
            raise NoRunningTask
        await self._run_callbacks(self.pre_stop_callbacks)
        out = self._task.cancel()
        self._task = None
        await self._run_callbacks(self.post_stop_callbacks)
        return out

    @staticmethod
    async def _run_callbacks(callbacks: list[StopCallbackT]) -> None:
        """Taking into account if the callback functions require the `await` syntax, they are executed in order."""
        for func in callbacks:
            out = func()
            if isawaitable(out):
                await out