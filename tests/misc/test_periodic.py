from asyncio.tasks import sleep as async_sleep
from logging import ERROR, INFO, getLogger
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, call, patch

from videbo.exceptions import NoRunningTask
from videbo.misc.periodic import Periodic
from videbo.misc.task_manager import TaskManager

_log = getLogger("videbo.tests")


class PeriodicTestCase(IsolatedAsyncioTestCase):
    def test___init__(self) -> None:
        async def test_function(x: int, y: int, foo: str, spam: str) -> None:
            if x == y and foo == spam:
                raise ValueError
        test_args = (1, 2)
        test_kwargs = {"foo": "bar", "spam": "eggs"}
        obj = Periodic(test_function, *test_args, **test_kwargs)
        self.assertIs(test_function, obj.async_func)
        self.assertTupleEqual(test_args, obj.args)
        self.assertDictEqual(test_kwargs, obj.kwargs)
        self.assertEqual("periodic-test_function", obj.task_name)
        self.assertListEqual([], obj.pre_stop_callbacks)
        self.assertListEqual([], obj.post_stop_callbacks)
        self.assertIsNone(obj._task)

    def test_is_running(self) -> None:
        obj = Periodic(MagicMock(__name__="foo"))
        self.assertFalse(obj.is_running)
        obj._task = object()
        self.assertTrue(obj.is_running)

    @patch("videbo.misc.periodic.sleep")
    @patch("videbo.misc.periodic.time")
    async def test__loop(
        self,
        mock_time: MagicMock,
        mock_sleep: AsyncMock,
    ) -> None:
        mock_time.side_effect = [1, 2, 10, 30]

        mock_func = AsyncMock(__name__="mock_func")
        test_args = (1, 2, 3)
        test_kwargs = {"foo": "bar", "spam": "eggs"}
        obj = Periodic(mock_func, *test_args, **test_kwargs)

        interval = 3.14
        limit = 2
        immediate = False

        await obj._loop(interval, limit=limit, call_immediately=immediate)
        mock_sleep.assert_has_awaits([
            call(interval),
            call(interval - 1),
        ])
        mock_func.assert_awaited_with(*test_args, **test_kwargs)
        mock_time.assert_called_with()

        mock_sleep.reset_mock()
        mock_func.reset_mock()
        mock_time.reset_mock()
        mock_time.side_effect = [1, 2, 10, 30, 31, 32]

        interval = 31.4
        limit = 3
        immediate = True

        await obj._loop(interval, limit=limit, call_immediately=immediate)
        mock_sleep.assert_has_awaits([
            call(0),
            call(interval - 1),
            call(interval - 20),
        ])
        mock_func.assert_awaited_with(*test_args, **test_kwargs)
        mock_time.assert_called_with()

    @patch.object(TaskManager, "fire_and_forget")
    @patch.object(Periodic, "_loop", new_callable=MagicMock)
    def test___call__(
        self,
        mock__loop: MagicMock,
        mock_fire_and_forget: MagicMock,
    ) -> None:
        mock__loop.return_value = mock_awaitable = object()

        obj = Periodic(MagicMock(__name__="foo"))

        interval = 31.4
        limit = 3
        immediate = True

        self.assertIsNone(
            obj(interval, limit=limit, call_immediately=immediate)
        )
        mock__loop.assert_called_once_with(
            interval,
            limit=limit,
            call_immediately=immediate,
        )
        mock_fire_and_forget.assert_called_once_with(
            mock_awaitable,
            name=obj.task_name,
        )

    async def test_stop(self) -> None:
        async def test_function() -> None:
            _log.info("<<test_function>>")
            await async_sleep(99999)

        def cb() -> None:
            _log.info("<<cb>>")
        async def cb_async() -> None:
            _log.info("<<cb_async>>")

        periodic = Periodic(test_function)
        periodic.pre_stop_callbacks.extend([cb, cb_async])
        periodic.post_stop_callbacks.extend([cb, cb_async])

        # Calling `stop` before starting should result in an error.
        with self.assertRaises(NoRunningTask), self.assertLogs("videbo", INFO) as log_ctx:
            await periodic.stop()
        self.assertEqual(1, len(log_ctx.records))
        self.assertEqual(ERROR, log_ctx.records[0].levelno)
        self.assertEqual("videbo.misc.task_manager", log_ctx.records[0].name)
        self.assertIn(
            "NoRunningTask occurred in an fire-and-forget task",
            log_ctx.output[0],
        )

        # We don't care about the periodicity, we just want our `test_function`
        # to start sleeping immediately. We allow the event loop to perform a
        # context switch here by sleeping a nominal amount of time.
        with self.assertLogs("videbo", INFO) as log_ctx:
            periodic(123, call_immediately=True)
            await async_sleep(0.001)
            self.assertTrue(periodic.is_running)
            await periodic.stop()
            self.assertFalse(periodic.is_running)
        expected_log_messages = [
            "<<test_function>>",
            "<<cb>>",
            "<<cb_async>>",
            f"Cancelled {periodic.task_name}",
            "<<cb>>",
            "<<cb_async>>",
            f"Stopped {periodic.task_name}",
        ]
        self.assertListEqual(
            expected_log_messages,
            [record.message for record in log_ctx.records],
        )
        self.assertEqual(INFO, log_ctx.records[3].levelno)
        self.assertEqual("videbo.misc.task_manager", log_ctx.records[3].name)
        self.assertEqual(INFO, log_ctx.records[6].levelno)
        self.assertEqual("videbo.misc.periodic", log_ctx.records[6].name)
