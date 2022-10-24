from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from videbo.distributor import start


class StartTestCase(IsolatedAsyncioTestCase):
    @patch.object(start, "settings")
    @patch.object(start, "start_web_server")
    def test_start(self, mock_start_web_server: MagicMock, mock_settings: MagicMock) -> None:
        mock_settings.files_path.mkdir = mock_mkdir = MagicMock()
        self.assertIsNone(start.start(foo="bar"))
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_start_web_server.assert_called_once_with(
            start.routes,
            start.NetworkInterfaces.app_context,
            start.DistributorFileController.app_context,
            address=mock_settings.listen_address,
            port=mock_settings.listen_port,
            verbose=mock_settings.dev_mode,
        )
