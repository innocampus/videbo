from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from videbo.distributor.api import client


class DistributorClientTestCase(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.parent_cls_init_patcher = patch.object(client.Client, "__init__")
        self.mock_parent___init__ = self.parent_cls_init_patcher.start()

        self.mock_jwt = object()
        self.get_jwt_patcher = patch.object(
            client.Client,
            "get_jwt_node",
            return_value=self.mock_jwt,
        )
        self.mock_get_jwt_node = self.get_jwt_patcher.start()

        self.mock_request_output = object()
        self.request_patcher = patch.object(
            client.Client,
            "request",
            return_value=self.mock_request_output,
        )
        self.mock_request_method = self.request_patcher.start()

    def tearDown(self) -> None:
        self.request_patcher.stop()
        self.get_jwt_patcher.stop()
        self.parent_cls_init_patcher.stop()
        super().tearDown()

    def test___init__(self) -> None:
        url = "foo/bar"
        kwargs = {"spam": "eggs", "beans": "toast"}
        obj = client.DistributorClient(url, **kwargs)
        self.assertEqual(url, obj.base_url)
        self.mock_parent___init__.assert_called_once_with(**kwargs)

    async def test_get_status(self) -> None:
        url = "foo/bar"
        obj = client.DistributorClient(url)
        log_err = False
        output = await obj.get_status(log_connection_error=log_err)
        self.assertIs(self.mock_request_output, output)
        self.mock_get_jwt_node.assert_called_once_with()
        self.mock_request_method.assert_awaited_once_with(
            "GET",
            url + "/api/distributor/status",
            self.mock_jwt,
            return_model=client.DistributorStatus,
            log_connection_error=log_err,
        )

    async def test_get_files_list(self) -> None:
        url = "foo/bar"
        obj = client.DistributorClient(url)
        output = await obj.get_files_list()
        self.assertIs(self.mock_request_output, output)
        self.mock_get_jwt_node.assert_called_once_with()
        self.mock_request_method.assert_awaited_once_with(
            "GET",
            url + "/api/distributor/files",
            self.mock_jwt,
            return_model=client.DistributorFileList,
        )

    @patch.object(client, "DistributorCopyFile")
    async def test_copy(self, mock_dist_copy_file_cls: MagicMock) -> None:
        mock_dist_copy_file_cls.return_value = mock_data = object()
        self.mock_request_method.return_value = code, _ = 12345, ""
        url = "foo/bar"
        obj = client.DistributorClient(url)
        size = 42.69
        mock_file, from_url = MagicMock(size=size), "bla/bla"
        output = await obj.copy(mock_file, from_url=from_url)
        self.assertIs(code, output)
        self.mock_get_jwt_node.assert_called_once_with()
        self.mock_request_method.assert_awaited_once_with(
            "POST",
            f"{url}/api/distributor/copy/{mock_file}",
            self.mock_jwt,
            data=mock_data,
            timeout=30. * 60,
        )
        mock_dist_copy_file_cls.assert_called_once_with(
            from_base_url=from_url,
            file_size=size,
        )

    @patch.object(client, "DistributorDeleteFiles")
    async def test_delete(self, mock_dist_delete_file_cls: MagicMock) -> None:
        mock_dist_delete_file_cls.return_value = mock_data = object()
        url = "foo/bar"
        obj = client.DistributorClient(url)
        mock_files = (MagicMock(), MagicMock())
        safe = False
        output = await obj.delete(*mock_files, safe=safe)
        self.assertIs(self.mock_request_output, output)
        self.mock_get_jwt_node.assert_called_once_with()
        self.mock_request_method.assert_awaited_once_with(
            "POST",
            url + "/api/distributor/delete",
            self.mock_jwt,
            data=mock_data,
            return_model=client.DistributorDeleteFilesResponse,
            timeout=60.,
        )
        mock_dist_delete_file_cls.assert_called_once_with(
            files=list(mock_files),
            safe=safe,
        )
