from __future__ import annotations
from asyncio import CancelledError, Task, sleep
from contextlib import suppress
from inspect import isawaitable
from logging import getLogger
from time import time
from typing import Generic, Optional, Union, TYPE_CHECKING
from typing_extensions import ParamSpec, TypeAlias

from videbo.exceptions import NoRunningTask
from .task_manager import TaskManager

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    StopCallbackT: TypeAlias = Callable[[], Union[Awaitable[None], None]]

_P = ParamSpec("_P")

log = getLogger(__name__)


class Periodic(Generic[_P]):
    """Simple utility for the periodic execution of asynchronous tasks."""
    async_func: Callable[_P, Awaitable[None]]
    args: _P.args  # type: ignore[valid-type]
    kwargs: _P.kwargs  # type: ignore[valid-type]
    task_name: str
    pre_stop_callbacks: list[StopCallbackT]
    post_stop_callbacks: list[StopCallbackT]
    _task: Optional[Task[None]]

    def __init__(
        self,
        __async_func__: Callable[_P, Awaitable[None]],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> None:
        """
        Prepares task parameters; does not launch execution.

        A default for the task name is generated based on the function name;
        it can be changed by setting a different `task_name` attribute after
        initialization, but the change will only affect future launches.

        Args:
            __async_func__: The asynchronous function to periodically execute
            *args: Positional arguments to pass to `__async_func__` every time
            **kwargs: Keyword-arguments to pass to `__async_func__` every time
        """
        self.async_func = __async_func__
        self.args = args
        self.kwargs = kwargs if kwargs is not None else {}
        self.task_name = f'periodic-{self.async_func.__name__}'
        self.pre_stop_callbacks = []
        self.post_stop_callbacks = []
        self._task = None  # initialized upon starting periodic execution

    @property
    def is_running(self) -> bool:
        return self._task is not None

    async def _loop(
        self,
        interval_seconds: float,
        limit: Optional[int] = None,
        call_immediately: bool = False,
    ) -> None:
        """
        The main execution loop repeatedly calling the function.

        Args:
            interval_seconds:
                The time in seconds to wait in between two executions
            limit (optional):
                If passed an integer, determines the maximum number of times
                the function will be called; if omitted or `None` (default),
                execution will be repeated forever (until stopped).
            call_immediately (optional):
                If `False` (default), the first execution will only happen
                after the specified time interval has elapsed; if `True`,
                it will happen immediately and only the following executions
                will honor the interval.
        """
        exec_time = interval_seconds if call_immediately else 0
        i = 0
        while limit is None or i < limit:
            await sleep(interval_seconds - exec_time)
            started = time()
            await self.async_func(*self.args, **self.kwargs)
            i += 1
            exec_time = time() - started

    def __call__(
        self,
        interval_seconds: float,
        limit: Optional[int] = None,
        call_immediately: bool = False,
    ) -> None:
        """
        Starts the execution loop with the provided options.

        Args:
            interval_seconds:
                The time in seconds to wait in between two executions
            limit (optional):
                If passed an integer, determines the maximum number of times
                the function will be called; if omitted or `None` (default),
                execution will be repeated forever (until stopped).
            call_immediately (optional):
                If `False` (default), the first execution will only happen
                after the specified time interval has elapsed; if `True`,
                it will happen immediately and only the following executions
                will honor the interval.
        """
        self._task = TaskManager.fire_and_forget(
            self._loop(
                interval_seconds,
                limit=limit,
                call_immediately=call_immediately,
            ),
            name=self.task_name,
        )

    def stop(self) -> Task[None]:
        """
        Schedules the cancellation of the periodic execution loop.

        This method returns immediately without blocking. The periodic loop will
        be stopped ASAP. If pre-stop callbacks have been set, those will be
        scheduled to execute _before_ the actual loop is cancelled.

        Returns:
            The task, scheduled to run ASAP, responsible for stopping the
            periodic execution loop. To _guarantee_ that the execution has
            stopped, this task can optionally be awaited.
        """
        return TaskManager.fire_and_forget(
            self._stop(),
            name=f"stop-{self.task_name}",
        )

    async def _stop(self) -> None:
        """
        Stops the execution loop and executes all callback functions.

        Raises:
            NoRunningTask: Periodic task has already been stopped.
        """
        if self._task is None:
            raise NoRunningTask
        log.debug(f"Stopping {self.task_name}...")
        await self._run_callbacks(self.pre_stop_callbacks)
        # We do not care about the `.cancel` return value here because the loop
        # task may have had a limited number of iterations set and therefore may
        # have already been done at this point.
        _ = self._task.cancel()
        # Ensure the task is done before running the post-stop callbacks.
        with suppress(CancelledError):
            await self._task
        self._task = None
        await self._run_callbacks(self.post_stop_callbacks)
        log.info(f"Stopped {self.task_name}")

    @staticmethod
    async def _run_callbacks(callbacks: list[StopCallbackT]) -> None:
        """Runs/awaits the `callbacks` in order."""
        for func in callbacks:
            out = func()
            if isawaitable(out):
                await out
