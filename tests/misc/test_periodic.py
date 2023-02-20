from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, call, patch

from videbo.misc import periodic


class PeriodicTestCase(IsolatedAsyncioTestCase):
    def test___init__(self) -> None:
        async def test_function(x: int, y: int, foo: str, spam: str) -> None:
            print(x, y, foo, spam)
        test_args = (1, 2)
        test_kwargs = {"foo": "bar", "spam": "eggs"}
        obj = periodic.Periodic(test_function, *test_args, **test_kwargs)
        self.assertIs(test_function, obj.async_func)
        self.assertTupleEqual(test_args, obj.args)
        self.assertDictEqual(test_kwargs, obj.kwargs)
        self.assertEqual("periodic-test_function", obj.task_name)
        self.assertListEqual([], obj.pre_stop_callbacks)
        self.assertListEqual([], obj.post_stop_callbacks)
        self.assertIsNone(obj._task)

    def test_is_running(self) -> None:
        obj = periodic.Periodic(MagicMock(__name__="foo"))
        self.assertFalse(obj.is_running)
        obj._task = object()
        self.assertTrue(obj.is_running)

    @patch.object(periodic, "sleep")
    @patch.object(periodic, "time")
    async def test__loop(
        self,
        mock_time: MagicMock,
        mock_sleep: AsyncMock,
    ) -> None:
        mock_time.side_effect = [1, 2, 10, 30]

        mock_func = AsyncMock(__name__="mock_func")
        test_args = (1, 2, 3)
        test_kwargs = {"foo": "bar", "spam": "eggs"}
        obj = periodic.Periodic(mock_func, *test_args, **test_kwargs)

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

    @patch.object(periodic.TaskManager, "fire_and_forget")
    @patch.object(periodic.Periodic, "_loop", new_callable=MagicMock)
    def test___call__(
        self,
        mock__loop: MagicMock,
        mock_fire_and_forget: MagicMock,
    ) -> None:
        mock__loop.return_value = mock_awaitable = object()

        obj = periodic.Periodic(MagicMock(__name__="foo"))

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

    @patch.object(periodic.Periodic, "_run_callbacks")
    async def test_stop(self, mock__run_callbacks: AsyncMock) -> None:
        obj = periodic.Periodic(MagicMock(__name__="foo"))
        with self.assertRaises(periodic.NoRunningTask):
            await obj.stop()
        mock__run_callbacks.assert_not_called()

        mock_cancel_out = object()
        mock_cancel = MagicMock(return_value=mock_cancel_out)
        obj._task = MagicMock(cancel=mock_cancel)
        obj.pre_stop_callbacks = pre_cb = [MagicMock(), MagicMock()]
        obj.post_stop_callbacks = post_cb = [MagicMock()]

        output = await obj.stop()
        self.assertEqual(mock_cancel_out, output)
        mock_cancel.assert_called_once_with()
        mock__run_callbacks.assert_has_awaits([
            call(pre_cb),
            call(post_cb),
        ])

    async def test__run_callbacks(self) -> None:
        cb1, cb2, cb3 = MagicMock(), AsyncMock(), MagicMock()
        self.assertIsNone(
            await periodic.Periodic._run_callbacks([cb1, cb2, cb3])
        )
        cb1.assert_called_once_with()
        cb2.assert_awaited_once_with()
        cb3.assert_called_once_with()
