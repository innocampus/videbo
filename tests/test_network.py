from asyncio import CancelledError, create_task, sleep
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, call, patch

from tests.silent_log import SilentLogMixin
from videbo.exceptions import HTTPClientError
from videbo.misc import MEGA
from videbo import network


class InterfaceStatsTestCase(TestCase):
    def test_update_throughput(self) -> None:
        bytes_before, bytes_after = 1000, 3000
        interval_seconds = 2
        stats = network.InterfaceStats(bytes=bytes_after)
        self.assertEqual(0., stats.throughput)
        stats.update_throughput(bytes_before=bytes_before, interval_seconds=interval_seconds)
        self.assertEqual(1000., stats.throughput)


class NetworkInterfacesTestCase(SilentLogMixin, IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_client_request = AsyncMock()
        self.client_patcher = patch.object(network, "Client", return_value=MagicMock(request=self.mock_client_request))
        self.mock_client_cls = self.client_patcher.start()
        self.settings_patcher = patch.object(network, "settings")
        self.mock_settings = self.settings_patcher.start()
        self.ni = network.NetworkInterfaces()

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.settings_patcher.stop()

    def test___init__(self) -> None:
        self.assertEqual(0., self.ni._last_time_network_proc)
        self.assertIsNone(self.ni._fetch_task)
        self.assertIsNone(self.ni._server_status)

    def test_get_instance(self) -> None:
        instance = network.NetworkInterfaces.get_instance()
        self.assertIs(network.NetworkInterfaces._instance, instance)
        self.assertIs(instance, network.NetworkInterfaces.get_instance())

    def test_is_fetching(self) -> None:
        self.assertFalse(self.ni.is_fetching)
        self.ni._fetch_task = MagicMock()
        self.assertTrue(self.ni.is_fetching)
        self.ni._fetch_task = None

    @patch.object(network.InterfaceStats, "update_throughput")
    @patch.object(network, "time")
    @patch.object(network, "open")
    def test__fetch_proc_info(self, mock_open: MagicMock, mock_time: MagicMock,
                              mock_update_throughput: MagicMock) -> None:
        mock_time.return_value = 100.
        # Create mock net dev output:
        iface_name_1, iface_name_2 = "enp1s0", "enp2s0"
        rx_stats_dict_1 = {
            "bytes": 1,
            "packets": 2,
            "errs": 3,
            "drop": 4,
            "fifo": 5,
            "frame": 6,
            "compr": 7,
            "multicast": 8,
        }
        tx_stats_dict_1 = {k: v * 10 for k, v in rx_stats_dict_1.items()}
        rx_stats_dict_2 = tx_stats_dict_1.copy()
        tx_stats_dict_2 = rx_stats_dict_1.copy()
        iface_info_1 = f"{iface_name_1}: " \
                       f"{' '.join(str(v) for v in rx_stats_dict_1.values())} " \
                       f"{' '.join(str(v) for v in tx_stats_dict_1.values())} "
        iface_info_2 = f"{iface_name_2}: " \
                       f"{' '.join(str(v) for v in rx_stats_dict_2.values())} " \
                       f"{' '.join(str(v) for v in tx_stats_dict_2.values())} "
        lo_info_1 = f"lo: " \
                    f"{' '.join(str(v) for v in rx_stats_dict_1.values())} " \
                    f"{' '.join(str(v) for v in tx_stats_dict_1.values())} "
        mock_net_dev_lines = [
            "foo",
            "bar",
            lo_info_1,
            iface_info_1,
            iface_info_2,
        ]
        mock_open.return_value = MagicMock(__enter__=MagicMock(return_value=mock_net_dev_lines))

        self.assertDictEqual({}, self.ni._interfaces)

        # Ensure dictionary of interfaces gets updated with correct data objects:
        self.ni._fetch_proc_info()
        self.assertListEqual([iface_name_1, iface_name_2], list(self.ni._interfaces.keys()))
        iface_1 = self.ni._interfaces[iface_name_1]
        iface_2 = self.ni._interfaces[iface_name_2]
        self.assertEqual(iface_1.name, iface_name_1)
        self.assertEqual(iface_2.name, iface_name_2)
        self.assertDictEqual(iface_1.rx.dict(exclude_unset=True), rx_stats_dict_1)
        self.assertDictEqual(iface_1.tx.dict(exclude_unset=True), tx_stats_dict_1)
        self.assertDictEqual(iface_2.rx.dict(exclude_unset=True), rx_stats_dict_2)
        self.assertDictEqual(iface_2.tx.dict(exclude_unset=True), tx_stats_dict_2)
        mock_update_throughput.assert_not_called()
        self.assertEqual(100., self.ni._last_time_network_proc)

        self.ni._last_time_network_proc = 50.

        # Ensure that now the throughput gets updated:
        self.ni._fetch_proc_info()
        self.assertIs(iface_1, self.ni._interfaces[iface_name_1])
        self.assertIs(iface_2, self.ni._interfaces[iface_name_2])
        mock_update_throughput.assert_has_calls([
            call(iface_1.rx.bytes, 50.),
            call(iface_1.tx.bytes, 50.),
            call(iface_2.rx.bytes, 50.),
            call(iface_2.tx.bytes, 50.),
        ])

    def test__update_apache_status(self) -> None:
        test_lines = [
            "</dl><pre>_...WW_.K._................................................."
            "............................WWW..................................",
            "....RRR...........__</pre>",
        ]
        self.ni._server_status = network.StubStatus(server_type=network.ServerType.apache)
        self.assertFalse(self.ni._update_apache_status(["foo", "bar", "baz"]))
        self.assertTrue(self.ni._update_apache_status(test_lines))
        self.assertEqual(4, self.ni._server_status.writing)
        self.assertEqual(3, self.ni._server_status.reading)
        self.assertEqual(5, self.ni._server_status.waiting)

    def test__update_nginx_status(self) -> None:
        test_line = """Reading:3
        Writing: 5
        Waiting:  5"""
        self.ni._server_status = network.StubStatus(server_type=network.ServerType.nginx)
        self.assertFalse(self.ni._update_nginx_status("something wrong here"))
        self.assertTrue(self.ni._update_nginx_status(test_line))
        self.assertEqual(4, self.ni._server_status.writing)
        self.assertEqual(3, self.ni._server_status.reading)
        self.assertEqual(5, self.ni._server_status.waiting)

    @patch.object(network.NetworkInterfaces, "_update_nginx_status")
    @patch.object(network.NetworkInterfaces, "_update_apache_status")
    async def test__fetch_server_status(self, mock__update_apache_status: MagicMock,
                                        mock__update_nginx_status: MagicMock) -> None:
        mock_url = "foo"
        self.ni._server_status = None

        # Test response error:
        self.mock_client_request.side_effect = HTTPClientError
        await self.ni._fetch_server_status(mock_url)
        self.assertIsNone(self.ni._server_status)
        self.mock_client_request.assert_awaited_once_with("GET", mock_url)
        self.mock_client_request.reset_mock()

        # Test connection error:
        self.mock_client_request.side_effect = ConnectionRefusedError
        await self.ni._fetch_server_status(mock_url)
        self.assertIsNone(self.ni._server_status)
        self.mock_client_request.assert_awaited_once_with("GET", mock_url)
        self.mock_client_request.reset_mock()

        # Test non-200 response:
        self.mock_client_request.side_effect = None
        self.mock_client_request.return_value = 404, b"foo"
        await self.ni._fetch_server_status(mock_url)
        self.assertIsNone(self.ni._server_status)
        self.mock_client_request.assert_awaited_once_with("GET", mock_url)
        self.mock_client_request.reset_mock()

        # Test less than 4 lines of response data:
        self.mock_client_request.return_value = 200, b"foo"
        with self.assertRaises(network.UnknownServerStatusFormatError):
            await self.ni._fetch_server_status(mock_url)
        self.mock_client_request.assert_awaited_once_with("GET", mock_url)
        self.mock_client_request.reset_mock()

        mock__update_apache_status.assert_not_called()
        mock__update_nginx_status.assert_not_called()

        mock__update_nginx_status.return_value = True

        # Test nginx stats:
        mock_nginx_stats = "fake nginx stats"
        self.mock_client_request.return_value = 200, b"foo\nbar\nbaz\n" + mock_nginx_stats.encode()
        await self.ni._fetch_server_status(mock_url)
        self.mock_client_request.assert_awaited_once_with("GET", mock_url)
        self.mock_client_request.reset_mock()
        mock__update_apache_status.assert_not_called()
        mock__update_nginx_status.assert_called_once_with(mock_nginx_stats)
        mock__update_nginx_status.reset_mock()
        mock__update_apache_status.return_value = False

        # Test apache stats, but unexpected format:
        self.mock_client_request.return_value = _, test_data = 200, b"<html>\nfoo\nbar"
        with self.assertRaises(network.UnknownServerStatusFormatError):
            await self.ni._fetch_server_status(mock_url)
        mock__update_nginx_status.assert_not_called()
        mock__update_apache_status.assert_called_once_with(test_data.decode().split("\n"))

    @patch.object(network.NetworkInterfaces, "_fetch_server_status")
    @patch.object(network.NetworkInterfaces, "_fetch_proc_info")
    async def test__fetch_loop(self, mock__fetch_proc_info: MagicMock, mock__fetch_server_status: AsyncMock) -> None:
        mock__fetch_proc_info.side_effect = Exception
        mock__fetch_server_status.side_effect = Exception
        self.mock_settings.network_info_fetch_interval = 0.1
        self.mock_settings.webserver.status_page = None
        task = create_task(self.ni._fetch_loop())
        await sleep(0.05)
        task.cancel()
        with self.assertRaises(CancelledError):
            await task
        mock__fetch_proc_info.assert_called_with()
        mock__fetch_server_status.assert_not_called()
        mock__fetch_proc_info.reset_mock()

        self.mock_settings.webserver.status_page = mock_status_url = "foo"
        task = create_task(self.ni._fetch_loop())
        await sleep(0.05)
        task.cancel()
        with self.assertRaises(CancelledError):
            await task
        mock__fetch_proc_info.assert_called_with()
        mock__fetch_server_status.assert_called_with(mock_status_url)

    @patch.object(network, "create_task")
    @patch.object(network.NetworkInterfaces, "_fetch_loop", new_callable=MagicMock)
    def test_start_fetching(self, mock__fetch_loop: MagicMock, mock_create_task: MagicMock) -> None:
        mock__fetch_loop.return_value = mock_coroutine = object()
        self.ni._fetch_task = MagicMock()
        self.ni.start_fetching()
        mock__fetch_loop.assert_not_called()
        mock_create_task.assert_not_called()

        self.ni._fetch_task = None
        self.ni.start_fetching()
        mock__fetch_loop.assert_called_once_with()
        mock_create_task.assert_called_once_with(mock_coroutine)

    def test_stop_fetching(self) -> None:
        self.ni._fetch_task = None
        self.ni.stop_fetching()
        self.ni._fetch_task = mock_task = MagicMock()
        self.ni.stop_fetching()
        mock_task.cancel.assert_called_once_with()

    def test_get_first_interface(self) -> None:
        self.ni._interfaces = {}
        self.assertIsNone(self.ni.get_first_interface())
        self.ni._interfaces = {"foo": 1, "bar": 2}
        self.assertEqual(1, self.ni.get_first_interface())

    @patch.object(network.NetworkInterfaces, "get_first_interface")
    def test_get_tx_current_rate(self, mock_get_first_interface: MagicMock) -> None:
        mock_get_first_interface.return_value = None
        self.assertIsNone(self.ni.get_tx_current_rate())
        mock_get_first_interface.assert_called_once_with()
        mock_get_first_interface.reset_mock()

        # interface "foo" with throughput of 1M bytes per second:
        self.ni._interfaces = {
            "foo": MagicMock(tx=MagicMock(throughput=1_000_000)),
            "bar": MagicMock()
        }
        self.assertEqual(8., self.ni.get_tx_current_rate(interface_name="foo"))

    @patch.object(network.NetworkInterfaces, "get_first_interface")
    def test_update_node_status(self, mock_get_first_interface: MagicMock) -> None:
        mock_get_first_interface.return_value = None
        self.ni._server_status = None
        mock_status_obj = MagicMock()
        self.ni.update_node_status(mock_status_obj)
        self.assertEqual(0., mock_status_obj.tx_current_rate)
        self.assertEqual(0., mock_status_obj.rx_current_rate)
        self.assertEqual(0., mock_status_obj.tx_total)
        self.assertEqual(0., mock_status_obj.rx_total)
        mock_get_first_interface.assert_called_once_with()
        mock_get_first_interface.reset_mock()

        connections = 101
        self.ni._server_status = MagicMock(writing=connections)
        mock_get_first_interface.return_value = MagicMock(
            tx=MagicMock(throughput=1_000_000, bytes=2 * MEGA),
            rx=MagicMock(throughput=2_000_000, bytes=4 * MEGA),
        )
        self.ni.update_node_status(mock_status_obj)
        self.assertEqual(8., mock_status_obj.tx_current_rate)
        self.assertEqual(16., mock_status_obj.rx_current_rate)
        self.assertEqual(2., mock_status_obj.tx_total)
        self.assertEqual(4., mock_status_obj.rx_total)


class FunctionsTestCase(IsolatedAsyncioTestCase):
    @patch.object(network.NetworkInterfaces, "get_instance")
    async def test_network_context(self, mock_get_ni_instance: MagicMock) -> None:
        mock_start_fetching, mock_stop_fetching = MagicMock(), MagicMock()
        mock_get_ni_instance.return_value = MagicMock(
            start_fetching=mock_start_fetching,
            stop_fetching=mock_stop_fetching,
        )
        iterator = network.network_context(MagicMock())
        self.assertIsNone(await iterator.__anext__())
        mock_start_fetching.assert_called_once_with()
        mock_stop_fetching.assert_not_called()

        mock_start_fetching.reset_mock()

        with self.assertRaises(StopAsyncIteration):
            await iterator.__anext__()

        mock_start_fetching.assert_not_called()
        mock_stop_fetching.assert_called_once_with()
