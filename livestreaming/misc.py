import asyncio
import os
import logging
import re
from typing import Set, Any

logger = logging.getLogger('livestreaming-misc')


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
