from asyncio import CancelledError
from collections.abc import Callable
from typing import Any
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from tests.silent_log import SilentLogMixin
from videbo.misc import task_manager


class TaskManagerTestCase(SilentLogMixin, IsolatedAsyncioTestCase):
    def tearDown(self) -> None:
        task_manager.TaskManager._tasks.clear()

    def test_cancel_all(self) -> None:
        mock_task1, mock_task2 = MagicMock(), MagicMock()
        task_manager.TaskManager._tasks = {mock_task1, mock_task2}
        self.assertIsNone(task_manager.TaskManager.cancel_all())
        mock_task1.cancel.assert_called_once_with()
        mock_task2.cancel.assert_called_once_with()
        task_manager.TaskManager._tasks.clear()

    @patch.object(task_manager.TaskManager, "cancel_all")
    async def test_shutdown(self, mock_cancel_all: MagicMock) -> None:
        self.assertIsNone(await task_manager.TaskManager.shutdown(1, "2", 3))
        mock_cancel_all.assert_called_once_with()

    def test_fire_and_forget_task(self) -> None:
        class MockTask(MagicMock):
            done_callback: Callable[..., Any]

            def add_done_callback(self, obj: Callable[..., Any]) -> None:
                self.done_callback = obj

        mock_result = MagicMock()
        mock_task = MockTask(result=mock_result)

        self.assertSetEqual(set(), task_manager.TaskManager._tasks)
        task_manager.TaskManager.fire_and_forget_task(mock_task)
        self.assertSetEqual({mock_task}, task_manager.TaskManager._tasks)

        self.assertIsNone(mock_task.done_callback("foobar"))
        mock_result.assert_called_once_with()
        self.assertSetEqual(set(), task_manager.TaskManager._tasks)

        mock_result.reset_mock()

        mock_result.side_effect = CancelledError

        task_manager.TaskManager.fire_and_forget_task(mock_task)
        self.assertSetEqual({mock_task}, task_manager.TaskManager._tasks)
        self.assertIsNone(mock_task.done_callback("foobar"))
        mock_result.assert_called_once_with()
        self.assertSetEqual(set(), task_manager.TaskManager._tasks)

        mock_result.reset_mock()

        mock_result.side_effect = Exception("foo")

        task_manager.TaskManager.fire_and_forget_task(mock_task)
        self.assertSetEqual({mock_task}, task_manager.TaskManager._tasks)
        self.videbo_main_log.setLevel(self.log_lvl)
        with self.assertLogs():
            self.assertIsNone(mock_task.done_callback("foobar"))
        mock_result.assert_called_once_with()
        self.assertSetEqual(set(), task_manager.TaskManager._tasks)
