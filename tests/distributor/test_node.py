import logging
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, PropertyMock, call, create_autospec, patch

from videbo.distributor import node


_FOOBAR = "foo/bar"


class DistributorNodeTestCase(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.mock_client1 = AsyncMock(base_url=_FOOBAR)
        mock_client2 = MagicMock(base_url=_FOOBAR)
        mock_client3 = MagicMock(base_url=_FOOBAR)
        self.client_patcher = patch.object(
            node,
            "Client",
            side_effect=(self.mock_client1, mock_client2, mock_client3),
        )
        self.mock_client_cls = self.client_patcher.start()

        self.dl_scheduler_init_patcher = patch.object(
            node.DownloadScheduler,
            "__init__",
            return_value=None,
        )
        self.mock_dl_scheduler_cls = self.dl_scheduler_init_patcher.start()

        self.periodic_task_name = "foo"
        self.mock_periodic = MagicMock(task_name=self.periodic_task_name)
        self.periodic_patcher = patch.object(
            node,
            "Periodic",
            return_value=self.mock_periodic,
        )
        self.mock_periodic_cls = self.periodic_patcher.start()

        self.enable_patcher = patch.object(node.DistributorNode, "enable")
        self.mock_enable = self.enable_patcher.start()

    def tearDown(self) -> None:
        self.enable_patcher.stop()
        self.periodic_patcher.stop()
        self.dl_scheduler_init_patcher.stop()
        self.client_patcher.stop()
        super().tearDown()

    def test___init__(self) -> None:
        url = "abc"
        client = MagicMock()
        obj = node.DistributorNode(url, enable=True, http_client=client)
        self.assertIs(client, obj.http_client)
        self.assertIsNone(obj._status)
        self.assertFalse(obj._good)
        self.assertFalse(obj._enabled)
        self.assertSetEqual(set(), obj._files_hosted)
        self.assertSetEqual(set(), obj._files_loading)
        self.assertIsInstance(
            obj._files_awaiting_download,
            node.DownloadScheduler,
        )
        self.assertTrue(obj._log_connection_error)
        self.assertIs(self.mock_periodic, obj._periodic_watcher)
        self.assertEqual(
            self.periodic_task_name + f'-{url}',
            self.mock_periodic.task_name,
        )
        self.mock_client_cls.assert_not_called()
        self.mock_enable.assert_called_once_with()

        self.mock_enable.reset_mock()
        self.mock_periodic.task_name = self.periodic_task_name

        obj = node.DistributorNode(url, enable=False)
        self.assertIs(self.mock_client1, obj.http_client)
        self.assertIsNone(obj._status)
        self.assertFalse(obj._good)
        self.assertFalse(obj._enabled)
        self.assertSetEqual(set(), obj._files_hosted)
        self.assertSetEqual(set(), obj._files_loading)
        self.assertIsInstance(
            obj._files_awaiting_download,
            node.DownloadScheduler,
        )
        self.assertTrue(obj._log_connection_error)
        self.assertIs(self.mock_periodic, obj._periodic_watcher)
        self.assertEqual(
            self.periodic_task_name + f'-{url}',
            self.mock_periodic.task_name,
        )
        self.mock_client_cls.assert_called_once_with(url)
        self.mock_enable.assert_not_called()

    def test___repr__(self) -> None:
        obj = node.DistributorNode(_FOOBAR)
        self.assertEqual(f"<Distributor {_FOOBAR}>", repr(obj))

    def test___eq__(self) -> None:
        obj1 = node.DistributorNode(_FOOBAR)
        obj1.http_client.base_url = "foo/bar"
        obj2 = node.DistributorNode(_FOOBAR)
        obj2.http_client.base_url = "foo/baz"
        obj3 = node.DistributorNode(_FOOBAR)
        obj3.http_client.base_url = "foo/baz"
        self.assertNotEqual(obj1, MagicMock())
        self.assertNotEqual(obj1, obj2)
        self.assertEqual(obj2, obj3)

    @patch.object(node.DistributorNode, "tx_load", new_callable=PropertyMock)
    def test___lt__(self, mock_tx_load: PropertyMock) -> None:
        obj = node.DistributorNode(_FOOBAR)
        mock_tx_load.return_value = 0.5
        mock_node = MagicMock(tx_load=0.6)
        self.assertLess(obj, mock_node)
        self.assertGreater(mock_node, obj)

    def test___contains__(self) -> None:
        obj = node.DistributorNode(_FOOBAR)
        mock_file = MagicMock()
        self.assertNotIn(mock_file, obj)
        obj._files_hosted = {mock_file}
        self.assertIn(mock_file, obj)

    def test_base_url(self) -> None:
        obj = node.DistributorNode(_FOOBAR)
        self.assertEqual(self.mock_client1.base_url, obj.base_url)

        obj.base_url = new_url = "baz"
        self.assertEqual(new_url, self.mock_client1.base_url)
        self.assertEqual(new_url, obj.base_url)

    def test_status(self) -> None:
        obj = node.DistributorNode(_FOOBAR)
        with self.assertRaises(node.DistStatusUnknown):
            _ = obj.status

        obj._status = mock_status = object()
        self.assertIs(mock_status, obj.status)

    def test_is_enabled(self) -> None:
        obj = node.DistributorNode(_FOOBAR)
        obj._enabled = True
        self.assertTrue(obj.is_enabled)

    def test_is_good(self) -> None:
        obj = node.DistributorNode(_FOOBAR)
        obj._good = True
        self.assertTrue(obj.is_good)

    @patch.object(node.DistributorNode, "status", new_callable=PropertyMock)
    def test_tx_load(self, mock_status: PropertyMock) -> None:
        obj = node.DistributorNode(_FOOBAR)
        tx_curr, tx_max = 20, 42
        mock_status.return_value = MagicMock(
            tx_current_rate=tx_curr,
            tx_max_rate=tx_max,
        )
        self.assertEqual(tx_curr / tx_max, obj.tx_load)

    @patch.object(node.DistributorNode, "status", new_callable=PropertyMock)
    def test_free_space(self, mock_status: PropertyMock) -> None:
        obj = node.DistributorNode(_FOOBAR)
        mock_status.return_value = MagicMock(free_space=42.99)
        self.assertEqual(42, obj.free_space)

    @patch.object(node.DistributorNode, "status", new_callable=PropertyMock)
    def test_total_space(self, mock_status: PropertyMock) -> None:
        obj = node.DistributorNode(_FOOBAR)
        free_space, files_total_size = 123, 456
        mock_status.return_value = MagicMock(
            free_space=free_space,
            files_total_size=files_total_size,
        )
        self.assertEqual(free_space + files_total_size, obj.total_space)

    @patch.object(node.DistributorNode, "total_space", new_callable=PropertyMock)
    @patch.object(node.DistributorNode, "status", new_callable=PropertyMock)
    def test_free_space_ratio(
        self,
        mock_status: PropertyMock,
        mock_total_space: PropertyMock,
    ) -> None:
        obj = node.DistributorNode(_FOOBAR)
        free, total = 222, 555
        mock_status.return_value = MagicMock(free_space=free)
        mock_total_space.return_value = total
        self.assertEqual(round(free / total, 3), obj.free_space_ratio)

    @patch.object(node.DistributorNode, "is_good", new_callable=PropertyMock)
    @patch.object(node.DistributorNode, "tx_load", new_callable=PropertyMock)
    @patch.object(node.DistributorNode, "is_enabled", new_callable=PropertyMock)
    def test_can_serve(
        self,
        mock_is_enabled: PropertyMock,
        mock_tx_load: PropertyMock,
        mock_is_good: PropertyMock,
    ) -> None:
        obj = node.DistributorNode(_FOOBAR)
        mock_is_enabled.return_value = True
        mock_tx_load.return_value = 0.5
        mock_is_good.return_value = True
        self.assertTrue(obj.can_serve)

        mock_is_enabled.return_value = False
        self.assertFalse(obj.can_serve)

        mock_is_enabled.return_value = True
        mock_tx_load.return_value = 0.95
        self.assertFalse(obj.can_serve)

        mock_tx_load.return_value = 0.1
        mock_is_good.return_value = False
        self.assertFalse(obj.can_serve)

    @patch.object(node, "settings")
    def test_can_start_downloading(self, mock_settings: MagicMock) -> None:
        mock_settings.distribution.max_parallel_copying_tasks = 5
        obj = node.DistributorNode(_FOOBAR)
        obj._files_loading = {1, 2, 3, 4, 5}
        self.assertFalse(obj.can_start_downloading)

        obj._files_loading.discard(1)
        self.assertTrue(obj.can_start_downloading)

    def test_is_loading(self) -> None:
        obj = node.DistributorNode(_FOOBAR)
        mock_file = MagicMock()
        self.assertFalse(obj.is_loading(mock_file))
        obj._files_loading = {mock_file}
        self.assertTrue(obj.is_loading(mock_file))

    def test_scheduled_to_load(self) -> None:
        obj = node.DistributorNode(_FOOBAR)
        obj._files_awaiting_download = set()
        mock_file = MagicMock()
        self.assertFalse(obj.scheduled_to_load(mock_file))
        obj._files_awaiting_download = {mock_file}
        self.assertTrue(obj.scheduled_to_load(mock_file))

    @patch.object(node.DistributorNode, "free_space", new_callable=PropertyMock)
    @patch.object(node.DistributorNode, "can_serve", new_callable=PropertyMock)
    def test_can_host_additional(
        self,
        mock_can_serve: PropertyMock,
        mock_free_space: PropertyMock,
    ) -> None:
        mock_can_serve.return_value = True
        mock_free_space.return_value = 15
        space_needed = 14
        obj = node.DistributorNode(_FOOBAR)
        self.assertTrue(obj.can_host_additional(space_needed))

        mock_can_serve.return_value = False
        self.assertFalse(obj.can_host_additional(space_needed))

        mock_can_serve.return_value = True
        space_needed = 16
        self.assertFalse(obj.can_host_additional(space_needed))

        space_needed = 1
        mock_can_serve.side_effect = node.DistStatusUnknown
        with self.assertLogs(node.log, level=logging.ERROR):
            self.assertFalse(obj.can_host_additional(space_needed))

    @patch.object(node.DistributorNode, "can_serve", new_callable=PropertyMock)
    def test_can_provide_copy(self, mock_can_serve: PropertyMock) -> None:
        mock_can_serve.return_value = True
        mock_file = MagicMock()
        obj = node.DistributorNode(_FOOBAR)
        self.assertTrue(obj.can_provide_copy(mock_file))

        mock_can_serve.return_value = False
        self.assertFalse(obj.can_provide_copy(mock_file))

        mock_can_serve.return_value = True
        obj._files_loading = {mock_file}
        self.assertFalse(obj.can_provide_copy(mock_file))

    @patch.object(node.DistributorNode, "set_node_state")
    @patch.object(node.DistributorNode, "free_space", new_callable=PropertyMock)
    @patch.object(node.DistributorNode, "is_good", new_callable=PropertyMock)
    async def test_fetch_dist_status(
        self,
        mock_is_good: PropertyMock,
        mock_free_space: PropertyMock,
        mock_set_node_state: AsyncMock,
    ) -> None:
        mock_is_good.return_value = True
        obj = node.DistributorNode(_FOOBAR)
        obj._log_connection_error = True
        obj._status = None

        # HTTP error (good state before):

        self.mock_client1.get_status.side_effect = node.HTTPClientError
        with self.assertLogs(node.log, logging.ERROR):
            await obj.fetch_dist_status()
        self.mock_client1.get_status.assert_awaited_once_with(
            log_connection_error=True
        )
        self.assertIsNone(obj._status)
        self.assertFalse(obj._log_connection_error)
        mock_is_good.assert_called_once_with()
        mock_free_space.assert_not_called()
        mock_set_node_state.assert_awaited_once_with(False)

        self.mock_client1.get_status.reset_mock()
        mock_is_good.reset_mock()
        mock_set_node_state.reset_mock()
        obj._log_connection_error = True

        # HTTP error (bad state before):

        mock_is_good.return_value = False
        await obj.fetch_dist_status()
        self.mock_client1.get_status.assert_awaited_once_with(
            log_connection_error=True
        )
        self.assertIsNone(obj._status)
        self.assertFalse(obj._log_connection_error)
        mock_is_good.assert_called_once_with()
        mock_free_space.assert_not_called()
        mock_set_node_state.assert_not_called()

        self.mock_client1.get_status.reset_mock()
        mock_is_good.reset_mock()
        obj._log_connection_error = True

        # Success (bad state before):

        self.mock_client1.get_status.side_effect = None
        self.mock_client1.get_status.return_value = _, data = 200, object()
        obj._status = None
        with self.assertLogs(node.log, logging.INFO):
            await obj.fetch_dist_status()
        self.mock_client1.get_status.assert_awaited_once_with(
            log_connection_error=True
        )
        self.assertIs(data, obj._status)
        self.assertTrue(obj._log_connection_error)
        mock_is_good.assert_called_once_with()
        mock_free_space.assert_called_once_with()
        mock_set_node_state.assert_awaited_once_with(True)

        self.mock_client1.get_status.reset_mock()
        mock_is_good.reset_mock()
        mock_free_space.reset_mock()
        mock_set_node_state.reset_mock()
        obj._log_connection_error = True
        obj._status = None

        # Success (good state before):

        mock_is_good.return_value = True
        await obj.fetch_dist_status()
        self.mock_client1.get_status.assert_awaited_once_with(
            log_connection_error=True
        )
        self.assertIs(data, obj._status)
        self.assertTrue(obj._log_connection_error)
        mock_is_good.assert_called_once_with()
        mock_free_space.assert_not_called()
        mock_set_node_state.assert_not_called()

        self.mock_client1.get_status.reset_mock()
        mock_is_good.reset_mock()
        obj._log_connection_error = True
        obj._status = None

        # Wrong HTTP status (good state before):

        self.mock_client1.get_status.return_value = 123, object()
        with self.assertLogs(node.log, logging.ERROR):
            await obj.fetch_dist_status()
        self.mock_client1.get_status.assert_awaited_once_with(
            log_connection_error=True
        )
        self.assertIsNone(obj._status)
        self.assertTrue(obj._log_connection_error)
        mock_is_good.assert_called_once_with()
        mock_free_space.assert_not_called()
        mock_set_node_state.assert_awaited_once_with(False)

        self.mock_client1.get_status.reset_mock()
        mock_is_good.reset_mock()
        mock_set_node_state.reset_mock()

        # Wrong HTTP status (bad state before):

        mock_is_good.return_value = False
        await obj.fetch_dist_status()
        self.mock_client1.get_status.assert_awaited_once_with(
            log_connection_error=True
        )
        self.assertIsNone(obj._status)
        self.assertTrue(obj._log_connection_error)
        mock_is_good.assert_called_once_with()
        mock_free_space.assert_not_called()
        mock_set_node_state.assert_not_called()

    @patch.object(node.DistributorNode, "_delete")
    @patch("videbo.storage.util.FileStorage.get_instance")
    async def test__fetch_files_list(
        self,
        mock_get_storage_instance: MagicMock,
        mock__delete: AsyncMock,
    ) -> None:
        mock_get_storage_instance.return_value = mock_storage = MagicMock()

        obj = node.DistributorNode(_FOOBAR)

        # HTTP error:

        self.mock_client1.get_files_list.side_effect = node.HTTPClientError
        with self.assertLogs(node.log, logging.ERROR):
            await obj._fetch_files_list()
        self.assertSetEqual(set(), obj._files_hosted)
        mock_get_storage_instance.assert_called_once_with()
        mock_storage.get_file.assert_not_called()
        mock__delete.assert_not_called()

        mock_get_storage_instance.reset_mock()

        # Bad HTTP status code:

        self.mock_client1.get_files_list.side_effect = None
        self.mock_client1.get_files_list.return_value = 123, object()

        with self.assertLogs(node.log, logging.ERROR):
            await obj._fetch_files_list()
        self.assertSetEqual(set(), obj._files_hosted)
        mock_get_storage_instance.assert_called_once_with()
        mock_storage.get_file.assert_not_called()
        mock__delete.assert_not_called()

        mock_get_storage_instance.reset_mock()

        # Some unknown files:

        file_known1 = MagicMock(hash="foo", ext=".bar")
        file_known2 = MagicMock(hash="spam", ext=".eggs")
        file_unknown = MagicMock(hash="a", ext=".b")
        stored_file1, stored_file2 = MagicMock(nodes=[]), MagicMock(nodes=[])
        mock_storage.get_file.side_effect = (
            stored_file1,
            FileNotFoundError,
            stored_file2
        )
        mock_data = MagicMock(
            files=[file_known1, file_unknown, file_known2]
        )
        self.mock_client1.get_files_list.return_value = 200, mock_data
        with self.assertLogs(node.log, logging.WARNING):
            await obj._fetch_files_list()
        self.assertSetEqual({stored_file1, stored_file2}, obj._files_hosted)
        self.assertListEqual([obj], stored_file1.nodes)
        self.assertListEqual([obj], stored_file2.nodes)
        mock_get_storage_instance.assert_called_once_with()
        self.assertListEqual(
            [
                call(file_known1.hash, file_known1.ext),
                call(file_unknown.hash, file_unknown.ext),
                call(file_known2.hash, file_known2.ext),
            ],
            mock_storage.get_file.call_args_list,
        )
        mock__delete.assert_awaited_once_with(file_unknown)

        mock_get_storage_instance.reset_mock()
        mock__delete.reset_mock()
        obj._files_hosted = set()

        # No unknown files:

        stored_file1 = MagicMock(nodes=[])
        mock_data = MagicMock(files=[file_known1])
        self.mock_client1.get_files_list.return_value = 200, mock_data
        mock_storage.get_file.side_effect = None
        mock_storage.get_file.return_value = stored_file1
        await obj._fetch_files_list()
        self.assertSetEqual({stored_file1}, obj._files_hosted)
        self.assertListEqual([obj], stored_file1.nodes)
        mock_get_storage_instance.assert_called_once_with()
        mock_storage.get_file.assert_called_once_with(file_known1.hash, file_known1.ext)
        mock__delete.assert_not_called()

    @patch.object(node.TaskManager, "fire_and_forget")
    @patch.object(node.DistributorNode, "_copy", new_callable=MagicMock)
    @patch.object(node.DistributorNode, "can_start_downloading", new_callable=PropertyMock)
    @patch.object(node.DistributorNode, "scheduled_to_load")
    @patch.object(node.DistributorNode, "is_loading")
    @patch.object(node.DistributorNode, "__contains__")
    async def test_put_video(
        self,
        mock___contains__: MagicMock,
        mock_is_loading: MagicMock,
        mock_scheduled_to_load: MagicMock,
        mock_can_start_downloading: MagicMock,
        mock__copy: MagicMock,
        mock_fire_and_forget: MagicMock,
    ) -> None:
        mock__copy.return_value = copy_coroutine = object()
        obj = node.DistributorNode(_FOOBAR)

        src_url = "bla/bla"
        mock_file, from_node = MagicMock(), MagicMock(base_url=src_url)

        # Already hosts the file:

        mock___contains__.return_value = True
        mock_is_loading.return_value = False
        mock_scheduled_to_load.return_value = False

        obj.put_video(mock_file, from_node=from_node)

        mock___contains__.assert_called_once_with(mock_file)
        mock_is_loading.assert_not_called()
        mock_scheduled_to_load.assert_not_called()
        mock_can_start_downloading.assert_not_called()
        mock__copy.assert_not_called()
        mock_fire_and_forget.assert_not_called()

        mock___contains__.reset_mock()

        # Already loads the file:

        mock___contains__.return_value = False
        mock_is_loading.return_value = True
        mock_scheduled_to_load.return_value = False

        obj.put_video(mock_file, from_node=from_node)

        mock___contains__.assert_called_once_with(mock_file)
        mock_is_loading.assert_called_once_with(mock_file)
        mock_scheduled_to_load.assert_not_called()
        mock_can_start_downloading.assert_not_called()
        mock__copy.assert_not_called()
        mock_fire_and_forget.assert_not_called()

        mock___contains__.reset_mock()
        mock_is_loading.reset_mock()

        # Already wants to load the file:

        mock___contains__.return_value = False
        mock_is_loading.return_value = False
        mock_scheduled_to_load.return_value = True

        obj.put_video(mock_file, from_node=from_node)

        mock___contains__.assert_called_once_with(mock_file)
        mock_is_loading.assert_called_once_with(mock_file)
        mock_scheduled_to_load.assert_called_once_with(mock_file)
        mock_can_start_downloading.assert_not_called()
        mock__copy.assert_not_called()
        mock_fire_and_forget.assert_not_called()

        mock___contains__.reset_mock()
        mock_is_loading.reset_mock()
        mock_scheduled_to_load.reset_mock()

        # Can start copying it right away:

        mock_scheduled_to_load.return_value = False
        mock_can_start_downloading.return_value = True

        obj.put_video(mock_file, from_node=from_node)

        mock___contains__.assert_called_once_with(mock_file)
        mock_is_loading.assert_called_once_with(mock_file)
        mock_scheduled_to_load.assert_called_once_with(mock_file)
        mock_can_start_downloading.assert_called_once_with()
        mock__copy.assert_called_once_with(mock_file, from_url=src_url)
        mock_fire_and_forget.assert_called_once_with(copy_coroutine)

        mock___contains__.reset_mock()
        mock_is_loading.reset_mock()
        mock_scheduled_to_load.reset_mock()
        mock_can_start_downloading.reset_mock()
        mock__copy.reset_mock()
        mock_fire_and_forget.reset_mock()

        # Is busy and needs to schedule the file for later copying:

        mock_can_start_downloading.return_value = False
        mock_schedule = MagicMock()
        obj._files_awaiting_download = MagicMock(schedule=mock_schedule)

        obj.put_video(mock_file)  # omit `from_node` to use storage as source

        mock___contains__.assert_called_once_with(mock_file)
        mock_is_loading.assert_called_once_with(mock_file)
        mock_scheduled_to_load.assert_called_once_with(mock_file)
        mock_can_start_downloading.assert_called_once_with()
        mock__copy.assert_not_called()
        mock_fire_and_forget.assert_not_called()
        mock_schedule.assert_called_once_with(
            mock_file,
            node.settings.public_base_url,
        )

    @patch.object(node.TaskManager, "fire_and_forget")
    @patch.object(node.DistributorNode, "can_start_downloading", new_callable=PropertyMock)
    async def test__copy(
        self,
        mock_can_start_downloading: PropertyMock,
        mock_fire_and_forget: MagicMock,
    ) -> None:
        mock_can_start_downloading.return_value = True

        mock_file = MagicMock()
        # We want to track the setting of the `copying` attribute,
        # so we add a property mock to our pseudo-file object:
        type(mock_file).copying = mock_copying = PropertyMock()
        # We also want to track appending and removing to the file's
        # `nodes` list, so we add a mock list to it:
        mock_file.nodes = create_autospec(list, instance=True)

        obj = node.DistributorNode(_FOOBAR)
        # We want to track adding and discarding to the `_files_loading` set:
        obj._files_loading = mock_loading = create_autospec(set, instance=True)
        # Do not test the recursive callback, so have the scheduler raise
        # `NothingScheduled`, when calling `next` on it:
        mock_next_scheduled = MagicMock(
            side_effect=node.DownloadScheduler.NothingScheduled
        )
        obj._files_awaiting_download = MagicMock(next=mock_next_scheduled)

        self.assertSetEqual(set(), obj._files_hosted)

        url = "bla/bla"
        self.mock_client1.copy.side_effect = node.HTTPClientError

        # HTTP error and nothing else scheduled:

        with self.assertLogs(node.log, logging.ERROR):
            await obj._copy(mock_file, from_url=url)

        self.mock_client1.copy.assert_awaited_once_with(
            mock_file,
            from_url=url,
        )
        mock_file.nodes.append.assert_called_once_with(obj)
        mock_file.nodes.remove.assert_called_once_with(obj)
        self.assertListEqual(
            [call(True), call(False)],
            mock_copying.call_args_list,
        )
        mock_loading.add.assert_called_once_with(mock_file)
        mock_loading.discard.assert_called_once_with(mock_file)
        mock_can_start_downloading.assert_called_once_with()
        mock_next_scheduled.assert_called_once_with()
        self.assertSetEqual(set(), obj._files_hosted)
        mock_fire_and_forget.assert_not_called()

        mock_file.reset_mock()
        mock_copying.reset_mock()
        mock_loading.reset_mock()
        mock_can_start_downloading.reset_mock()
        mock_next_scheduled.reset_mock()
        self.mock_client1.copy.reset_mock()

        self.mock_client1.copy.side_effect = None
        self.mock_client1.copy.return_value = 200
        mock_can_start_downloading.return_value = False

        # Success and no capacity to copy another:

        with self.assertLogs(node.log, logging.INFO):
            await obj._copy(mock_file, from_url=url)

        self.mock_client1.copy.assert_awaited_once_with(
            mock_file,
            from_url=url,
        )
        mock_file.nodes.append.assert_called_once_with(obj)
        mock_file.nodes.remove.assert_not_called()
        self.assertListEqual(
            [call(True), call(False)],
            mock_copying.call_args_list,
        )
        mock_loading.add.assert_called_once_with(mock_file)
        mock_loading.discard.assert_called_once_with(mock_file)
        mock_can_start_downloading.assert_called_once_with()
        mock_next_scheduled.assert_not_called()
        self.assertSetEqual({mock_file}, obj._files_hosted)
        mock_fire_and_forget.assert_not_called()

        mock_file.reset_mock()
        mock_copying.reset_mock()
        mock_loading.reset_mock()
        mock_can_start_downloading.reset_mock()
        self.mock_client1.copy.reset_mock()
        obj._files_hosted.clear()

        self.mock_client1.copy.return_value = 123
        mock_can_start_downloading.return_value = True

        # We want to ensure that the method now recursively schedules itself
        # as a task at the end with the next file-url-pair, but only once,
        # so that we can await it again and avoid a dangling coroutine:
        next_file, next_url = MagicMock(), MagicMock()
        mock_next_scheduled.side_effect = [
            (next_file, next_url),
            node.DownloadScheduler.NothingScheduled,
        ]

        # Bad HTTP status code and launch the next copy task:

        with self.assertLogs(node.log, logging.ERROR):
            await obj._copy(mock_file, from_url=url)

        self.mock_client1.copy.assert_awaited_once_with(
            mock_file,
            from_url=url,
        )
        mock_file.nodes.append.assert_called_once_with(obj)
        mock_file.nodes.remove.assert_called_once_with(obj)
        self.assertListEqual(
            [call(True), call(False)],
            mock_copying.call_args_list,
        )
        mock_loading.add.assert_called_once_with(mock_file)
        mock_loading.discard.assert_called_once_with(mock_file)
        mock_can_start_downloading.assert_called_once_with()
        mock_next_scheduled.assert_called_once_with()
        self.assertSetEqual(set(), obj._files_hosted)

        task_args = mock_fire_and_forget.call_args.args
        self.assertEqual(1, len(task_args))
        coroutine = task_args[0]
        self.assertEqual("DistributorNode._copy", coroutine.__qualname__)
        with self.assertLogs(node.log, logging.ERROR):  # still bad HTTP code
            await coroutine  # to avoid warning about non-awaited coroutine

    @patch.object(node.DistributorNode, "status", new_callable=PropertyMock)
    async def test__delete(self, mock_status: PropertyMock) -> None:
        mock_files = (MagicMock(), MagicMock())
        mock_safe = MagicMock()

        obj = node.DistributorNode(_FOOBAR)

        self.mock_client1.delete.side_effect = node.HTTPClientError

        # HTTP error:
        with self.assertRaises(node.DistributionError):
            with self.assertLogs(node.log, logging.ERROR):
                await obj._delete(*mock_files, safe=mock_safe)

        self.mock_client1.delete.assert_awaited_once_with(
            *mock_files,
            safe=mock_safe,
        )
        mock_status.assert_not_called()

        self.mock_client1.reset_mock()
        self.mock_client1.delete.side_effect = None
        self.mock_client1.delete.return_value = 123, object()

        # Bad response code:
        with self.assertRaises(node.DistributionError):
            with self.assertLogs(node.log, logging.ERROR):
                await obj._delete(*mock_files, safe=mock_safe)

        self.mock_client1.delete.assert_awaited_once_with(
            *mock_files,
            safe=mock_safe,
        )
        mock_status.assert_not_called()

        self.mock_client1.reset_mock()

        space = 42
        mock_response_data = MagicMock(
            free_space=space,
            files_skipped=(1, 2, 3),
        )
        self.mock_client1.delete.return_value = 200, mock_response_data

        # Success:
        with self.assertLogs(node.log, logging.INFO):
            output = await obj._delete(*mock_files, safe=mock_safe)

        self.assertIs(mock_response_data, output)
        self.mock_client1.delete.assert_awaited_once_with(
            *mock_files,
            safe=mock_safe,
        )
        self.assertEqual(space, mock_status.return_value.free_space)
