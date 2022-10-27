from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import urlencode

from videbo.storage.api import client


class StorageClientTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        super().setUp()

        self.settings_patcher = patch.object(client, "settings")
        self.mock_settings: MagicMock = self.settings_patcher.start()
        self.mock_settings.make_url = self.mock_make_url = MagicMock()

        self.get_jwt_patcher = patch.object(client.Client, "get_jwt_admin")
        self.mock_get_jwt_admin: AsyncMock = self.get_jwt_patcher.start()

        self.request_patcher = patch.object(client.Client, "request")
        self.mock_request: AsyncMock = self.request_patcher.start()

        self.client = client.StorageClient()

    async def asyncTearDown(self) -> None:
        await self.client._session.close()
        self.request_patcher.stop()
        self.get_jwt_patcher.stop()
        self.settings_patcher.stop()

        await super().asyncTearDown()

    async def test_get_status(self) -> None:
        output = await self.client.get_status()
        self.assertEqual(self.mock_request.return_value, output)
        self.mock_make_url.assert_called_once_with("/api/storage/status")
        self.mock_get_jwt_admin.assert_called_once_with()
        self.mock_request.assert_awaited_once_with(
            "GET",
            self.mock_make_url.return_value,
            self.mock_get_jwt_admin.return_value,
            return_model=client.StorageStatus,
        )

    async def test_get_filtered_files(self) -> None:
        expected_output = object()
        self.mock_request.return_value = 200, MagicMock(files=expected_output)

        test_kwargs = {"foo": "bar", "spam": "eggs"}
        output = await self.client.get_filtered_files(**test_kwargs)

        self.assertEqual(expected_output, output)
        self.mock_make_url.assert_called_once_with(
            f"/api/storage/files?{urlencode(test_kwargs)}"
        )
        self.mock_get_jwt_admin.assert_called_once_with()
        self.mock_request.assert_awaited_once_with(
            "GET",
            self.mock_make_url.return_value,
            self.mock_get_jwt_admin.return_value,
            return_model=client.StorageFilesList,
        )

        self.mock_make_url.reset_mock()
        self.mock_get_jwt_admin.reset_mock()
        self.mock_request.reset_mock()

        self.mock_request.return_value = 201, object()
        output = await self.client.get_filtered_files(**test_kwargs)
        self.assertIsNone(output)
        self.mock_make_url.assert_called_once_with(
            f"/api/storage/files?{urlencode(test_kwargs)}"
        )
        self.mock_get_jwt_admin.assert_called_once_with()
        self.mock_request.assert_awaited_once_with(
            "GET",
            self.mock_make_url.return_value,
            self.mock_get_jwt_admin.return_value,
            return_model=client.StorageFilesList,
        )

    @patch.object(client, "DeleteFilesList")
    async def test_delete_files(self, mock_del_files_cls: MagicMock) -> None:
        mock_del_files_cls.return_value = mock_data = object()

        mock_file1, mock_file2 = MagicMock(hash="foo"), MagicMock(hash="bar")
        output = await self.client.delete_files(mock_file1, mock_file2)

        self.assertEqual(self.mock_request.return_value, output)
        mock_del_files_cls.assert_called_once_with(hashes=["foo", "bar"])
        self.mock_make_url.assert_called_once_with("/api/storage/delete")
        self.mock_get_jwt_admin.assert_called_once_with()
        self.mock_request.assert_awaited_once_with(
            "POST",
            self.mock_make_url.return_value,
            self.mock_get_jwt_admin.return_value,
            data=mock_data,
        )

    async def test_get_distributor_nodes(self) -> None:
        output = await self.client.get_distributor_nodes()
        self.assertEqual(self.mock_request.return_value, output)
        self.mock_make_url.assert_called_once_with(
            "/api/storage/distributor/status"
        )
        self.mock_get_jwt_admin.assert_called_once_with()
        self.mock_request.assert_awaited_once_with(
            "GET",
            self.mock_make_url.return_value,
            self.mock_get_jwt_admin.return_value,
            return_model=client.DistributorStatusDict,
        )

    @patch.object(client, "DistributorNodeInfo")
    async def test_set_distributor_state(
        self,
        mock_dist_node_info_cls: MagicMock,
    ) -> None:
        mock_dist_node_info_cls.return_value = mock_data = object()
        self.mock_request.return_value = 1234, object()

        test_url = "foo/bar"

        output = await self.client.set_distributor_state(test_url, True)
        self.assertEqual(1234, output)
        mock_dist_node_info_cls.assert_called_once_with(base_url=test_url)
        self.mock_make_url.assert_called_once_with(
            "/api/storage/distributor/enable"
        )
        self.mock_get_jwt_admin.assert_called_once_with()
        self.mock_request.assert_awaited_once_with(
            "POST",
            self.mock_make_url.return_value,
            self.mock_get_jwt_admin.return_value,
            data=mock_data,
        )

        mock_dist_node_info_cls.reset_mock()
        self.mock_make_url.reset_mock()
        self.mock_get_jwt_admin.reset_mock()
        self.mock_request.reset_mock()

        output = await self.client.set_distributor_state(test_url, False)
        self.assertEqual(1234, output)
        mock_dist_node_info_cls.assert_called_once_with(base_url=test_url)
        self.mock_make_url.assert_called_once_with(
            "/api/storage/distributor/disable"
        )
        self.mock_get_jwt_admin.assert_called_once_with()
        self.mock_request.assert_awaited_once_with(
            "POST",
            self.mock_make_url.return_value,
            self.mock_get_jwt_admin.return_value,
            data=mock_data,
        )
