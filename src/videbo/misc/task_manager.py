from asyncio import CancelledError, Task, create_task
from collections.abc import Coroutine
from logging import getLogger
from typing import Any, Optional, TypeVar


log = getLogger(__name__)

_T = TypeVar("_T")


class TaskManager:
    _tasks: set[Task[Any]] = set()

    @classmethod
    def cancel_all(cls) -> None:
        log.info(f"TaskManager: cancel all remaining {len(cls._tasks)} tasks")
        for task in cls._tasks:
            task.cancel()

    @classmethod
    async def shutdown(cls, *_: Any) -> None:
        log.info("\n======== Shutting down ========")
        TaskManager.cancel_all()

    @classmethod
    def fire_and_forget(
        cls,
        coroutine: Coroutine[Any, Any, _T],
        name: Optional[str] = None,
    ) -> Task[_T]:
        """Checks if there was an exception in the task when the task ends."""
        task = create_task(coroutine, name=name)

        def task_done(_future: Any) -> None:
            try:
                # This throws an exception if there was any in the task.
                task.result()
            except CancelledError:
                log.info(f"Cancelled {task.get_name()}")
            except Exception as e:
                log.exception(
                    "%s occurred in an fire-and-forget task.",
                    e.__class__.__name__,
                )
            finally:
                cls._tasks.remove(task)

        cls._tasks.add(task)
        task.add_done_callback(task_done)
        return task
