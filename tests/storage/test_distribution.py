from logging import INFO, WARNING
from time import time
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, call, patch

from videbo.storage import distribution


class DistributionControllerTestCase(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.add_new_dist_node_patcher = patch.object(
            distribution.DistributionController,
            "add_new_dist_node",
        )
        self.mock_add_new_dist_node = self.add_new_dist_node_patcher.start()

        self.client_patcher = patch.object(distribution, "Client")
        self.mock_client_cls = self.client_patcher.start()

        self.periodic_patcher = patch.object(distribution, "Periodic")
        self.mock_periodic_cls = self.periodic_patcher.start()

    def tearDown(self) -> None:
        self.periodic_patcher.stop()
        self.client_patcher.stop()
        self.add_new_dist_node_patcher.stop()
        super().tearDown()

    def test___init__(self) -> None:
        urls = ["foo", "bar"]
        obj = distribution.DistributionController(urls)
        self.assertDictEqual({}, obj._dist_nodes)
        self.assertListEqual(
            [call("foo"), call("bar")],
            self.mock_add_new_dist_node.call_args_list,
        )
        self.assertEqual(5., obj._sort_cache_sec)
        self.assertEqual(0., obj._last_sort_time)
        self.assertIs(self.mock_client_cls.return_value, obj.http_client)
        self.mock_periodic_cls.assert_called_once_with(obj.free_up_dist_nodes)
        self.mock_periodic_cls.return_value.assert_called_once_with(
            distribution.settings.dist_cleanup_freq
        )

    @patch.object(distribution.DistributionController, "iter_nodes")
    async def test_free_up_dist_nodes(self, mock_iter_nodes: MagicMock) -> None:
        free_up1, free_up2 = AsyncMock(), AsyncMock()
        node1 = MagicMock(free_up_space=free_up1)
        node2 = MagicMock(free_up_space=free_up2)
        mock_iter_nodes.return_value = [node1, node2]
        obj = distribution.DistributionController()
        with self.assertLogs(distribution.log, INFO):
            await obj.free_up_dist_nodes()
        free_up1.assert_awaited_once_with()
        free_up2.assert_awaited_once_with()

    def test_iter_nodes(self) -> None:
        obj = distribution.DistributionController()
        obj._dist_nodes = mock_nodes = {"a": object(), "b": object()}
        self.assertListEqual(list(mock_nodes.values()), list(obj.iter_nodes()))

    @patch.object(distribution.DistributionController, "iter_nodes")
    def test_sort_nodes(self, mock_iter_nodes: MagicMock) -> None:
        url1, url2 = "foo", "bar"
        node1 = MagicMock(base_url=url1, __lt__=lambda _self, _other: True)
        node2 = MagicMock(base_url=url2, __lt__=lambda _self, _other: False)
        mock_iter_nodes.return_value = [node1, node2]
        obj = distribution.DistributionController()

        # Should be no-op:
        obj._last_sort_time = last_sort_time = time()
        obj._sort_cache_sec = 100.
        obj.sort_nodes()
        self.assertEqual(last_sort_time, obj._last_sort_time)
        self.assertDictEqual({}, obj._dist_nodes)
        mock_iter_nodes.assert_not_called()

        # Force
        obj.sort_nodes(force=True)
        self.assertLess(last_sort_time, obj._last_sort_time)
        self.assertDictEqual({url1: node1, url2: node2}, obj._dist_nodes)
        # Verify order:
        self.assertListEqual([url1, url2], list(obj._dist_nodes.keys()))
        mock_iter_nodes.assert_called_once_with()

        mock_iter_nodes.reset_mock()

        # Reverse order
        node1 = MagicMock(base_url=url1, __lt__=lambda _self, _other: False)
        node2 = MagicMock(base_url=url2, __lt__=lambda _self, _other: True)
        mock_iter_nodes.return_value = [node1, node2]
        # Last sorted long time ago:
        obj._last_sort_time = last_sort_time = time() - 1000.
        obj._sort_cache_sec = 1.
        obj.sort_nodes()
        self.assertLess(last_sort_time, obj._last_sort_time)
        self.assertDictEqual({url2: node2, url1: node1}, obj._dist_nodes)
        # Verify order:
        self.assertListEqual([url2, url1], list(obj._dist_nodes.keys()))

    @patch.object(distribution.DistributionController, "iter_nodes")
    def test_filter_nodes(self, mock_iter_nodes: MagicMock) -> None:
        node1, node2, node3 = MagicMock(), MagicMock(), MagicMock()
        mock_iter_nodes.return_value = [node1, node2, node3]
        obj = distribution.DistributionController()

        it = obj.filter_nodes(
            lambda n: n is node1 or n is node2,
            exclude_nodes={node1},
        )
        self.assertIs(node2, next(it))
        with self.assertRaises(StopIteration):
            next(it)

        it = obj.filter_nodes(
            lambda n: n is node1 or n is node2,
            check_nodes=[node1, node3],
        )
        self.assertIs(node1, next(it))
        with self.assertRaises(StopIteration):
            next(it)

    @patch.object(distribution.DistributionController, "filter_nodes")
    def test_get_node(self, mock_filter_nodes: MagicMock) -> None:
        node1, node2 = MagicMock(), MagicMock()
        obj = distribution.DistributionController()
        obj._dist_nodes = {"foo": node1, "bar": node2}
        output = obj.get_node("bar")
        self.assertIs(node2, output)
        mock_filter_nodes.assert_not_called()

        def fake_match_func(_node: distribution.DistributorNode) -> bool:
            return True

        mock_filter_nodes.return_value = iter([node1, node2])
        output = obj.get_node(fake_match_func)
        self.assertIs(node1, output)
        mock_filter_nodes.assert_called_once_with(fake_match_func)

        mock_filter_nodes.reset_mock()
        mock_filter_nodes.return_value = iter([])
        output = obj.get_node(fake_match_func)
        self.assertIsNone(output)
        mock_filter_nodes.assert_called_once_with(fake_match_func)

    @patch.object(distribution, "TaskManager")
    def test_remove_from_nodes(self, mock_task_manager: MagicMock) -> None:
        mock_remove1, mock_remove2 = MagicMock(), MagicMock()
        mock_node1 = MagicMock(remove=mock_remove1)
        mock_node2 = MagicMock(remove=mock_remove2)
        obj = distribution.DistributionController()
        obj._dist_nodes = {"a": mock_node1, "b": mock_node2}

        mock_file = MagicMock()
        obj.remove_from_nodes(mock_file)
        mock_remove1.assert_called_once_with(mock_file, safe=False)
        mock_remove2.assert_called_once_with(mock_file, safe=False)
        self.assertListEqual(
            [call(mock_remove1.return_value), call(mock_remove2.return_value)],
            mock_task_manager.fire_and_forget.call_args_list,
        )

    @patch.object(distribution.DistributionController, "filter_nodes")
    @patch.object(distribution, "partial")
    @patch.object(distribution.DistributionController, "sort_nodes")
    def test_get_node_to_serve(
        self,
        mock_sort_nodes: MagicMock,
        mock_partial: MagicMock,
        mock_filter_nodes: MagicMock,
    ) -> None:
        mock_file = MagicMock()

        # None found:
        mock_filter_nodes.return_value = []
        obj = distribution.DistributionController()
        node, complete = obj.get_node_to_serve(mock_file)
        self.assertIsNone(node)
        self.assertFalse(complete)
        mock_sort_nodes.assert_called_once_with()
        mock_partial.assert_called_once_with(
            distribution.DistributorNode.can_serve,
            file=mock_file,
        )
        mock_filter_nodes.assert_called_once_with(mock_partial.return_value)

        mock_sort_nodes.reset_mock()
        mock_partial.reset_mock()
        mock_filter_nodes.reset_mock()

        # First one is still loading; should return the second one:
        node_loading = MagicMock(is_loading=lambda _: True)
        node_complete = MagicMock(is_loading=lambda _: False)
        mock_filter_nodes.return_value = [node_loading, node_complete]
        node, complete = obj.get_node_to_serve(mock_file)
        self.assertIs(node_complete, node)
        self.assertTrue(complete)
        mock_sort_nodes.assert_called_once_with()
        mock_partial.assert_called_once_with(
            distribution.DistributorNode.can_serve,
            file=mock_file,
        )
        mock_filter_nodes.assert_called_once_with(mock_partial.return_value)

        mock_sort_nodes.reset_mock()
        mock_partial.reset_mock()
        mock_filter_nodes.reset_mock()

        # Only nodes that are loading; should return the first one:
        node_loading2 = MagicMock(is_loading=lambda _: True)
        mock_filter_nodes.return_value = [node_loading, node_loading2]
        node, complete = obj.get_node_to_serve(mock_file)
        self.assertIs(node_loading, node)
        self.assertFalse(complete)
        mock_sort_nodes.assert_called_once_with()
        mock_partial.assert_called_once_with(
            distribution.DistributorNode.can_serve,
            file=mock_file,
        )
        mock_filter_nodes.assert_called_once_with(mock_partial.return_value)

    @patch.object(distribution.DistributionController, "get_node")
    @patch.object(distribution, "partial")
    @patch.object(distribution.DistributionController, "sort_nodes")
    def test_handle_distribution(
        self,
        mock_sort_nodes: MagicMock,
        mock_partial: MagicMock,
        mock_get_node: MagicMock,
    ) -> None:
        can_receive, can_provide = object(), object()
        mock_partial.side_effect = can_receive, can_provide
        mock_put_video = MagicMock()
        dst, src = MagicMock(put_video=mock_put_video), object()
        mock_get_node.side_effect = dst, src

        obj = distribution.DistributionController()

        # Threshold not reached:
        num_views = distribution.settings.distribution.copy_views_threshold - 1
        mock_file = MagicMock(num_views=num_views)
        tx_load = 0
        output = obj.handle_distribution(mock_file, tx_load)
        self.assertIsNone(output)

        mock_sort_nodes.assert_not_called()
        mock_partial.assert_not_called()
        mock_get_node.assert_not_called()

        # Distribution should be successfully started:
        mock_file = MagicMock(num_views=num_views + 1000)
        output = obj.handle_distribution(mock_file, tx_load)
        self.assertIs(dst, output)
        mock_sort_nodes.assert_called_once_with()
        self.assertListEqual(
            [
                call(
                    distribution.DistributorNode.can_receive_copy,
                    file=mock_file,
                ),
                call(
                    distribution.DistributorNode.can_provide_copy,
                    file=mock_file,
                ),
            ],
            mock_partial.call_args_list,
        )
        self.assertListEqual(
            [call(can_receive), call(can_provide)],
            mock_get_node.call_args_list,
        )

        mock_sort_nodes.reset_mock()
        mock_get_node.reset_mock()
        mock_partial.reset_mock()

        # No source available; storage too busy:
        mock_get_node.side_effect = dst, None
        mock_partial.side_effect = can_receive, can_provide
        tx_load = distribution.settings.distribution.max_load_file_copy + 0.01
        with self.assertLogs(distribution.log, WARNING):
            output = obj.handle_distribution(mock_file, tx_load)
        self.assertIsNone(output)
        mock_sort_nodes.assert_called_once_with()
        self.assertListEqual(
            [
                call(
                    distribution.DistributorNode.can_receive_copy,
                    file=mock_file,
                ),
                call(
                    distribution.DistributorNode.can_provide_copy,
                    file=mock_file,
                ),
            ],
            mock_partial.call_args_list,
        )
        self.assertListEqual(
            [call(can_receive), call(can_provide)],
            mock_get_node.call_args_list,
        )

        mock_sort_nodes.reset_mock()
        mock_get_node.reset_mock()
        mock_partial.reset_mock()

        # No destination found:
        mock_partial.side_effect = None
        mock_partial.return_value = can_receive
        mock_get_node.return_value = mock_get_node.side_effect = None
        output = obj.handle_distribution(mock_file, tx_load)
        self.assertIsNone(output)
        mock_sort_nodes.assert_called_once_with()
        mock_partial.assert_called_once_with(
            distribution.DistributorNode.can_receive_copy,
            file=mock_file,
        )
        mock_get_node.assert_called_once_with(can_receive)

    @patch.object(distribution, "DistributorNode")
    @patch.object(distribution.DistributionController, "get_node")
    def test_add_new_dist_node(
        self,
        mock_get_node: MagicMock,
        mock_dist_node_cls: MagicMock,
    ) -> None:
        # Ensure no-op, if a node with the provided URL is already there:

        self.add_new_dist_node_patcher.stop()
        mock_get_node.return_value = object()
        mock_dist_node_cls.return_value = mock_node = MagicMock()

        obj = distribution.DistributionController()
        obj._dist_nodes = {}
        url = "foo/bar"
        with self.assertLogs(distribution.log, WARNING):
            obj.add_new_dist_node(url)
        self.assertDictEqual({}, obj._dist_nodes)
        mock_get_node.assert_called_once_with(url)
        mock_dist_node_cls.assert_not_called()

        mock_get_node.reset_mock()
        mock_get_node.return_value = None

        # Ensure a node is added, if none with the provided URL is found:

        with self.assertLogs(distribution.log, INFO):
            obj.add_new_dist_node(url)
        self.assertDictEqual({url: mock_node}, obj._dist_nodes)
        mock_get_node.assert_called_once_with(url)
        mock_dist_node_cls.assert_called_once_with(url)

    @patch.object(distribution.UnknownDistURL, "__init__", return_value=None)
    async def test_remove_dist_node(
        self,
        mock_unknown_err_init: MagicMock,
    ) -> None:
        # Ensure error is raised, if no node with the provided URL is known:

        mock_disable = MagicMock(
            side_effect=distribution.DistNodeAlreadyDisabled("foo")
        )
        mock_node = MagicMock(disable=mock_disable, set_node_state=AsyncMock())

        obj = distribution.DistributionController()
        url = "foo/bar"
        with self.assertLogs(distribution.log, WARNING), self.assertRaises(distribution.UnknownDistURL):
            await obj.remove_dist_node(url)
        self.assertDictEqual({}, obj._dist_nodes)
        mock_unknown_err_init.assert_called_once_with(url)

        mock_unknown_err_init.reset_mock()

        # Ensure node is removed, if a node with the provided URL is found:

        obj._dist_nodes = {url: mock_node}
        await obj.remove_dist_node(url)
        self.assertDictEqual({}, obj._dist_nodes)
        mock_unknown_err_init.assert_not_called()
        mock_node.disable.assert_called_once_with()
        mock_node.set_node_state.assert_awaited_once_with(False)

    @patch.object(distribution.DistributionController, "get_node")
    async def test_disable_dist_node(self, mock_get_node: MagicMock) -> None:
        # Ensure error is raised, if no node with the provided URL is known:
        mock_get_node.return_value = None
        mock_node = MagicMock()

        controller = distribution.DistributionController()
        url = "foo/bar"
        with self.assertLogs(distribution.log, WARNING) as log_ctx, self.assertRaises(distribution.UnknownDistURL) as err_ctx:
            controller.disable_dist_node(url)
        self.assertEqual(
            f"Unknown distributor node `{url}`",
            err_ctx.exception.text,
        )
        self.assertEqual(WARNING, log_ctx.records[0].levelno)
        self.assertEqual(
            f"Cannot disable unknown distributor at `{url}`",
            log_ctx.records[0].message,
        )
        mock_node.disable.assert_not_called()
        mock_get_node.assert_called_once_with(url)

        mock_get_node.reset_mock()

        # Ensure node is disabled, if a node with the provided URL is found:
        mock_get_node.return_value = mock_node
        controller.disable_dist_node(url)
        mock_node.disable.assert_called_once_with()
        mock_get_node.assert_called_once_with(url)

    @patch.object(distribution.DistributionController, "get_node")
    async def test_enable_dist_node(self, mock_get_node: MagicMock) -> None:
        # Ensure error is raised, if no node with the provided URL is known:
        mock_get_node.return_value = None
        mock_node = MagicMock()

        controller = distribution.DistributionController()
        url = "foo/bar"
        with self.assertLogs(distribution.log, WARNING) as log_ctx, self.assertRaises(distribution.UnknownDistURL) as err_ctx:
            controller.enable_dist_node(url)
        self.assertEqual(
            f"Unknown distributor node `{url}`",
            err_ctx.exception.text,
        )
        self.assertEqual(WARNING, log_ctx.records[0].levelno)
        self.assertEqual(
            f"Cannot enable unknown distributor at `{url}`",
            log_ctx.records[0].message,
        )
        mock_node.enable.assert_not_called()
        mock_get_node.assert_called_once_with(url)

        mock_get_node.reset_mock()

        # Ensure node is disabled, if a node with the provided URL is found:
        mock_get_node.return_value = mock_node
        controller.enable_dist_node(url)
        mock_node.enable.assert_called_once_with()
        mock_get_node.assert_called_once_with(url)

    @patch.object(distribution.DistributionController, "iter_nodes")
    def test_get_nodes_status(self, mock_iter_nodes: MagicMock) -> None:
        mock_url1, mock_status1 = "foo", object()
        mock_node1 = MagicMock(
            is_good=True,
            is_enabled=True,
            base_url=mock_url1,
            status=mock_status1,
        )
        mock_url2, mock_status2 = "bar", object()
        mock_node2 = MagicMock(
            is_good=False,
            is_enabled=True,
            base_url=mock_url2,
            status=mock_status2,
        )
        mock_url3, mock_status3 = "baz", object()
        mock_node3 = MagicMock(
            is_good=True,
            is_enabled=False,
            base_url=mock_url3,
            status=mock_status3,
        )
        mock_iter_nodes.return_value = [mock_node1, mock_node2, mock_node3]
        obj = distribution.DistributionController()

        only_good = only_enabled = False
        expected_output = {
            mock_url1: mock_status1,
            mock_url2: mock_status2,
            mock_url3: mock_status3,
        }
        output = obj.get_nodes_status(
            only_good=only_good,
            only_enabled=only_enabled,
        )
        self.assertDictEqual(expected_output, output)
        mock_iter_nodes.assert_called_once_with()

        mock_iter_nodes.reset_mock()

        only_good = True
        only_enabled = False
        expected_output = {
            mock_url1: mock_status1,
            mock_url3: mock_status3,
        }
        output = obj.get_nodes_status(
            only_good=only_good,
            only_enabled=only_enabled,
        )
        self.assertDictEqual(expected_output, output)
        mock_iter_nodes.assert_called_once_with()

        mock_iter_nodes.reset_mock()

        only_good = False
        only_enabled = True
        expected_output = {
            mock_url1: mock_status1,
            mock_url2: mock_status2,
        }
        output = obj.get_nodes_status(
            only_good=only_good,
            only_enabled=only_enabled,
        )
        self.assertDictEqual(expected_output, output)
        mock_iter_nodes.assert_called_once_with()

        mock_iter_nodes.reset_mock()

        only_good = True
        only_enabled = True
        expected_output = {
            mock_url1: mock_status1,
        }
        output = obj.get_nodes_status(
            only_good=only_good,
            only_enabled=only_enabled,
        )
        self.assertDictEqual(expected_output, output)
        mock_iter_nodes.assert_called_once_with()
