from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from videbo.distributor import files


class DistributorFileControllerTestCase(IsolatedAsyncioTestCase):
    @patch.object(files.DistributorFileController, "get_instance")
    async def test_distributor_context(self, mock_get_dfc_instance: MagicMock) -> None:
        iterator = files.DistributorFileController.app_context(MagicMock())
        self.assertIsNone(await iterator.__anext__())
        mock_get_dfc_instance.assert_called_once_with()

        mock_get_dfc_instance.reset_mock()

        with self.assertRaises(StopAsyncIteration):
            await iterator.__anext__()

        mock_get_dfc_instance.assert_not_called()
