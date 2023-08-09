from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from videbo.storage import start


class StartTestCase(IsolatedAsyncioTestCase):
    @patch.object(start, "settings")
    @patch("videbo.storage.monitoring.Monitoring.get_instance")
    async def test_monitoring_context(self, mock_get_mon_instance: MagicMock, mock_settings: MagicMock) -> None:
        mock_settings.monitoring.prom_text_file = True

        mock_run, mock_stop = AsyncMock(), AsyncMock()
        mock_get_mon_instance.return_value = MagicMock(
            run=mock_run,
            stop=mock_stop,
        )

        iterator = start.monitoring_context(MagicMock())
        self.assertIsNone(await iterator.__anext__())
        mock_run.assert_called_once_with()
        mock_stop.assert_not_called()

        mock_run.reset_mock()

        with self.assertRaises(StopAsyncIteration):
            await iterator.__anext__()

        mock_run.assert_not_called()
        mock_stop.assert_called_once_with()

        mock_stop.reset_mock()
        mock_settings.monitoring.prom_text_file = None

        iterator = start.monitoring_context(MagicMock())
        self.assertIsNone(await iterator.__anext__())
        with self.assertRaises(StopAsyncIteration):
            await iterator.__anext__()
        mock_run.assert_not_called()
        mock_stop.assert_not_called()

    @patch.object(start, "settings")
    @patch.object(start, "start_web_server")
    def test_start(self, mock_start_web_server: MagicMock, mock_settings: MagicMock) -> None:
        mock_settings.files_path.mkdir = mock_mkdir = MagicMock()
        self.assertIsNone(start.start())
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_start_web_server.assert_called_once_with(
            start.routes,
            str(mock_settings.listen_address),
            mock_settings.listen_port,
            cleanup_contexts=(
                start.NetworkInterfaces.app_context,
                start.LMS.app_context,
                start.FileStorage.app_context,
                start.monitoring_context,
            ),
            verbose=mock_settings.dev_mode,
        )
