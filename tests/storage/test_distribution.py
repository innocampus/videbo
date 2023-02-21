import logging
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
        self.assertListEqual([], obj._dist_nodes)
        self.assertListEqual(
            [call("foo"), call("bar")],
            self.mock_add_new_dist_node.call_args_list,
        )
        self.assertIs(self.mock_client_cls.return_value, obj.http_client)
        self.mock_periodic_cls.assert_called_once_with(obj.free_up_dist_nodes)
        self.mock_periodic_cls.return_value.assert_called_once_with(
            distribution.settings.dist_cleanup_freq
        )

    async def test_free_up_dist_nodes(self) -> None:
        free_up1, free_up2 = AsyncMock(), AsyncMock()
        node1 = MagicMock(free_up_space=free_up1)
        node2 = MagicMock(free_up_space=free_up2)
        obj = distribution.DistributionController()
        obj._dist_nodes = [node1, node2]
        with self.assertLogs(distribution.log, logging.INFO):
            await obj.free_up_dist_nodes()
        free_up1.assert_awaited_once_with()
        free_up2.assert_awaited_once_with()

    def test_iter_nodes(self) -> None:
        obj = distribution.DistributionController()
        obj._dist_nodes = mock_nodes = [object(), object()]
        self.assertListEqual(mock_nodes, list(obj.iter_nodes()))

    def test_find_node(self) -> None:
        node_foo = MagicMock(base_url="foo")
        node_bar = MagicMock(base_url="bar")
        node_baz = MagicMock(base_url="baz")
        obj = distribution.DistributionController()
        obj._dist_nodes = [node_foo, node_bar, node_baz]

        output = obj.find_node("foo")
        self.assertIs(node_foo, output)
        output = obj.find_node("foo", check_nodes=[node_bar, node_baz])
        self.assertIsNone(output)
        output = obj.find_node("foo", exclude_nodes=[node_foo])
        self.assertIsNone(output)

        node1, node2 = MagicMock(foo=1), MagicMock(foo=2)
        node3, node4 = MagicMock(foo=3), MagicMock(foo=4)
        obj._dist_nodes = [node1, node2, node3, node4]

        output = obj.find_node(lambda n: n.foo > 1, exclude_nodes=[node2])
        self.assertIs(node3, output)

    @patch.object(distribution.DistributionController, "find_node")
    @patch.object(distribution, "partial")
    def test_copy_file(
        self,
        mock_partial: MagicMock,
        mock_find_node: MagicMock,
    ) -> None:
        mock_partial.side_effect = can_host, has_copy = object(), object()
        mock_find_node.side_effect = dst, src = MagicMock(), object()

        obj = distribution.DistributionController()
        obj._dist_nodes = [2, 3, 1]  # just to check sorting

        mock_file = MagicMock(nodes=["c", "b", "a"])
        output = obj.copy_file(mock_file)
        self.assertIs(dst, output)
        self.assertListEqual(
            [
                call(
                    distribution.DistributorNode.can_host_additional,
                    min_space_mb=mock_file.size / distribution.MEGA,
                ),
                call(
                    distribution.DistributorNode.can_provide_copy,
                    file=mock_file,
                ),
            ],
            mock_partial.call_args_list,
        )
        self.assertListEqual(
            [
                call(can_host, exclude_nodes=mock_file.nodes),
                call(has_copy, check_nodes=sorted(mock_file.nodes))
            ],
            mock_find_node.call_args_list,
        )

        mock_find_node.reset_mock()
        mock_partial.reset_mock()

        mock_partial.side_effect = None
        mock_partial.return_value = can_host
        mock_find_node.return_value = mock_find_node.side_effect = None

        output = obj.copy_file(mock_file)
        self.assertIsNone(output)
        mock_partial.assert_called_once_with(
            distribution.DistributorNode.can_host_additional,
            min_space_mb=mock_file.size / distribution.MEGA,
        )
        mock_find_node.assert_called_once_with(
            can_host,
            exclude_nodes=mock_file.nodes,
        )

    @patch.object(distribution, "DistributorNode")
    @patch.object(distribution.DistributionController, "find_node")
    def test_add_new_dist_node(
        self,
        mock_find_node: MagicMock,
        mock_dist_node_cls: MagicMock,
    ) -> None:
        # Ensure no-op, if a node with the provided URL is already there:

        self.add_new_dist_node_patcher.stop()
        mock_find_node.return_value = object()
        mock_dist_node_cls.return_value = mock_node = MagicMock()

        obj = distribution.DistributionController()
        obj._dist_nodes = []
        url = "foo/bar"
        with self.assertLogs(distribution.log, logging.WARNING):
            obj.add_new_dist_node(url)
        self.assertListEqual([], obj._dist_nodes)
        mock_find_node.assert_called_once_with(url)
        mock_dist_node_cls.assert_not_called()

        mock_find_node.reset_mock()
        mock_find_node.return_value = None

        # Ensure a node is added, if none with the provided URL is found:

        with self.assertLogs(distribution.log, logging.INFO):
            obj.add_new_dist_node(url)
        self.assertListEqual([mock_node], obj._dist_nodes)
        mock_find_node.assert_called_once_with(url)
        mock_dist_node_cls.assert_called_once_with(url)

    @patch.object(distribution.UnknownDistURL, "__init__", return_value=None)
    @patch.object(distribution.DistributionController, "find_node")
    async def test_remove_dist_node(
            self,
            mock_find_node: MagicMock,
            mock_unknown_err_init: MagicMock,
    ) -> None:
        # Ensure error is raised, if no node with the provided URL is known:

        mock_find_node.return_value = None
        mock_node = MagicMock(unlink_node=AsyncMock())

        obj = distribution.DistributionController()
        obj._dist_nodes = [mock_node]
        url = "foo/bar"
        with self.assertLogs(distribution.log, logging.WARNING):
            with self.assertRaises(distribution.UnknownDistURL):
                await obj.remove_dist_node(url)
        self.assertListEqual([mock_node], obj._dist_nodes)
        mock_find_node.assert_called_once_with(url)
        mock_unknown_err_init.assert_called_once_with(url)

        mock_find_node.reset_mock()
        mock_unknown_err_init.reset_mock()

        # Ensure node is removed, if a node with the provided URL is found:

        mock_find_node.return_value = mock_node
        await obj.remove_dist_node(url)
        self.assertListEqual([], obj._dist_nodes)
        mock_find_node.assert_called_once_with(url)
        mock_unknown_err_init.assert_not_called()

    @patch.object(distribution.UnknownDistURL, "__init__", return_value=None)
    @patch.object(distribution.DistributionController, "find_node")
    async def test__enable_or_disable_node(
            self,
            mock_find_node: MagicMock,
            mock_unknown_err_init: MagicMock,
    ) -> None:
        # Ensure error is raised, if no node with the provided URL is known:

        mock_find_node.return_value = None
        mock_node = MagicMock()

        obj = distribution.DistributionController()
        url = "foo/bar"
        enable = False
        with self.assertLogs(distribution.log, logging.WARNING):
            with self.assertRaises(distribution.UnknownDistURL):
                await obj._enable_or_disable_node(url, enable=enable)
        mock_node.disable.assert_not_called()
        mock_find_node.assert_called_once_with(url)
        mock_unknown_err_init.assert_called_once_with(url)

        mock_find_node.reset_mock()
        mock_unknown_err_init.reset_mock()

        # Ensure node is disabled, if a node with the provided URL is found:

        mock_find_node.return_value = mock_node
        await obj._enable_or_disable_node(url, enable=enable)
        mock_node.disable.assert_called_once_with()
        mock_node.enable.assert_not_called()
        mock_find_node.assert_called_once_with(url)
        mock_unknown_err_init.assert_not_called()

        mock_node.disable.reset_mock()
        mock_find_node.reset_mock()

        # Ensure node is enabled, if a node with the provided URL is found:

        enable = True
        await obj._enable_or_disable_node(url, enable=enable)
        mock_node.enable.assert_called_once_with()
        mock_node.disable.assert_not_called()
        mock_find_node.assert_called_once_with(url)
        mock_unknown_err_init.assert_not_called()

    @patch.object(
        distribution.DistributionController,
        "_enable_or_disable_node",
    )
    async def test_disable_dist_node(
        self,
        mock__enable_or_disable_node: AsyncMock,
    ) -> None:
        obj = distribution.DistributionController()
        url = "foo/bar"
        await obj.disable_dist_node(url)
        mock__enable_or_disable_node.assert_awaited_once_with(url, enable=False)

    @patch.object(
        distribution.DistributionController,
        "_enable_or_disable_node",
    )
    async def test_enable_dist_node(
        self,
        mock__enable_or_disable_node: AsyncMock,
    ) -> None:
        obj = distribution.DistributionController()
        url = "foo/bar"
        await obj.enable_dist_node(url)
        mock__enable_or_disable_node.assert_awaited_once_with(url, enable=True)

    def test_get_nodes_status(self) -> None:
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
        obj = distribution.DistributionController()
        obj._dist_nodes = [mock_node1, mock_node2, mock_node3]

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
