import logging
from asyncio import CancelledError, Task
from typing import Any


log = logging.getLogger(__name__)


class TaskManager:
    _tasks: set[Task[Any]] = set()

    @classmethod
    def cancel_all(cls) -> None:
        log.info(f"TaskManager: cancel all remaining {len(cls._tasks)} tasks")
        for task in cls._tasks:
            task.cancel()

    @classmethod
    async def shutdown(cls, *_: Any) -> None:
        TaskManager.cancel_all()

    @classmethod
    def fire_and_forget_task(cls, task: Task[Any]) -> None:
        """Checks if there was an exception in the task when the task ends."""
        def task_done(_future: Any) -> None:
            try:
                # This throws an exception if there was any in the task.
                task.result()
            except CancelledError:
                pass
            except Exception as e:
                log.exception(
                    "%s occurred in an fire-and-forget task.",
                    e.__class__.__name__,
                )
            finally:
                cls._tasks.remove(task)

        cls._tasks.add(task)
        task.add_done_callback(task_done)
