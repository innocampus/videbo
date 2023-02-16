from asyncio.exceptions import CancelledError
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

    @patch.object(task_manager, "create_task")
    def test_fire_and_forget(self, mock_create_task: MagicMock) -> None:
        mock_result = MagicMock()
        mock_create_task.return_value = mock_task = MagicMock(
            result=mock_result
        )
        mock_coroutine = MagicMock()

        self.assertSetEqual(set(), task_manager.TaskManager._tasks)

        task_manager.TaskManager.fire_and_forget(mock_coroutine)
        self.assertSetEqual({mock_task}, task_manager.TaskManager._tasks)
        task_done_cb = mock_task.add_done_callback.call_args[0][0]
        task_done_cb(object())
        mock_result.assert_called_once_with()
        self.assertSetEqual(set(), task_manager.TaskManager._tasks)

        mock_result.reset_mock()

        mock_result.side_effect = CancelledError

        task_manager.TaskManager.fire_and_forget(mock_coroutine)
        self.assertSetEqual({mock_task}, task_manager.TaskManager._tasks)
        task_done_cb = mock_task.add_done_callback.call_args[0][0]
        task_done_cb(object())
        mock_result.assert_called_once_with()
        self.assertSetEqual(set(), task_manager.TaskManager._tasks)

        mock_result.reset_mock()

        mock_result.side_effect = Exception("foo")

        task_manager.TaskManager.fire_and_forget(mock_coroutine)
        self.assertSetEqual({mock_task}, task_manager.TaskManager._tasks)
        task_done_cb = mock_task.add_done_callback.call_args[0][0]
        self.videbo_main_log.setLevel(self.log_lvl)
        with self.assertLogs():
            task_done_cb(object())
        mock_result.assert_called_once_with()
        self.assertSetEqual(set(), task_manager.TaskManager._tasks)
