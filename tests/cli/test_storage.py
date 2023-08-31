from io import StringIO
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from videbo.misc.constants import MEGA
from videbo.cli import storage


class ModuleTestCase(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_stdout = StringIO()

        def write_and_break(string: str = "") -> None:
            self.mock_stdout.write(string + "\n")

        self.print_patcher = patch.object(
            storage,
            "print",
            new=write_and_break,
        )
        self.mock_print_func = self.print_patcher.start()
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self.print_patcher.stop()

    def test_print_response(self) -> None:
        self.assertIsNone(storage.print_response(200))
        self.assertIn(
            "Request was successful!",
            self.mock_stdout.getvalue(),
        )
        self.mock_stdout.seek(0)

        code = 123
        self.assertIsNone(storage.print_response(code))
        self.assertIn(
            f"HTTP response code {code}",
            self.mock_stdout.getvalue(),
        )
        self.assertIn(
            "Please check storage log for details.",
            self.mock_stdout.getvalue(),
        )

    @patch.object(storage, "print_response")
    async def test_show_storage_status(self, mock_print_resp: MagicMock) -> None:
        mock_output = "foobar"
        mock_data = MagicMock(json=MagicMock(return_value=mock_output))
        mock_get_status = AsyncMock(return_value=(200, mock_data))
        mock_client = MagicMock(get_status=mock_get_status)

        await storage.show_storage_status(mock_client)
        self.assertIn(
            mock_output,
            self.mock_stdout.getvalue(),
        )
        mock_get_status.assert_awaited_once_with()
        mock_data.json.assert_called_once_with(indent=4)
        mock_print_resp.assert_not_called()

        mock_get_status.reset_mock()
        mock_data.json.reset_mock()

        code = 123
        mock_get_status.return_value = (code, object())

        await storage.show_storage_status(mock_client)
        mock_get_status.assert_awaited_once_with()
        mock_data.json.assert_not_called()
        mock_print_resp.assert_called_once_with(code)

    @patch.object(storage, "list_files")
    @patch.object(storage, "print_response")
    @patch.object(storage, "input")
    async def test_find_orphaned_files(
        self,
        mock_input: MagicMock,
        mock_print_response: MagicMock,
        mock_list_files: MagicMock,
    ) -> None:
        mock_get_filtered_files = AsyncMock()
        mock_delete_files = AsyncMock()
        mock_client = MagicMock(
            get_filtered_files=mock_get_filtered_files,
            delete_files=mock_delete_files,
        )

        #####################
        # Abort immediately #

        mock_input.return_value = "n"
        self.assertIsNone(
            await storage.find_orphaned_files(mock_client, delete=False)
        )
        self.assertIn(
            "Aborted.",
            self.mock_stdout.getvalue(),
        )
        mock_get_filtered_files.assert_not_called()
        mock_print_response.assert_not_called()
        mock_list_files.assert_not_called()
        mock_delete_files.assert_not_called()

        #########################
        # Request and get error #

        mock_input.return_value = "yes"
        code = 456
        mock_get_filtered_files.return_value = (code, object())
        self.assertIsNone(
            await storage.find_orphaned_files(mock_client, delete=False)
        )
        self.assertIn(
            "Querying storage for orphaned files",
            self.mock_stdout.getvalue(),
        )
        mock_get_filtered_files.assert_awaited_once_with(orphaned=True)
        mock_print_response.assert_called_once_with(code)
        mock_list_files.assert_not_called()
        mock_delete_files.assert_not_called()

        mock_get_filtered_files.reset_mock()
        mock_print_response.reset_mock()
        self.mock_stdout.seek(0)

        ###########################
        # No orphaned files found #

        mock_get_filtered_files.return_value = (200, MagicMock(files=[]))
        self.assertIsNone(
            await storage.find_orphaned_files(mock_client, delete=False)
        )
        self.assertIn(
            "No orphaned files found",
            self.mock_stdout.getvalue(),
        )
        mock_get_filtered_files.assert_awaited_once_with(orphaned=True)
        mock_print_response.assert_not_called()
        mock_list_files.assert_not_called()
        mock_delete_files.assert_not_called()

        mock_get_filtered_files.reset_mock()
        self.mock_stdout.seek(0)

        ######################
        # Two orphaned files #

        file1 = MagicMock(size=MEGA)
        file2 = MagicMock(size=2 * MEGA)
        mock_get_filtered_files.return_value = (200, MagicMock(files=[file1, file2]))
        self.assertIsNone(
            await storage.find_orphaned_files(mock_client, delete=False)
        )
        self.assertIn(
            "Found 2 orphaned files with a total size of 3.0 MB",
            self.mock_stdout.getvalue(),
        )
        self.assertIn(
            "If you want to delete all orphaned files",
            self.mock_stdout.getvalue(),
        )
        mock_get_filtered_files.assert_awaited_once_with(orphaned=True)
        mock_print_response.assert_not_called()
        mock_list_files.assert_called_once_with(file1, file2)
        mock_delete_files.assert_not_called()

        mock_get_filtered_files.reset_mock()
        mock_list_files.reset_mock()
        self.mock_stdout.seek(0)

        ###########################
        # Find but abort deletion #

        mock_input.side_effect = "yes", "no"
        self.assertIsNone(
            await storage.find_orphaned_files(mock_client, delete=True)
        )
        self.assertIn(
            "Found 2 orphaned files with a total size of 3.0 MB",
            self.mock_stdout.getvalue(),
        )
        self.assertNotIn(
            "If you want to delete all orphaned files",
            self.mock_stdout.getvalue(),
        )
        self.assertIn(
            "Aborted.",
            self.mock_stdout.getvalue(),
        )
        mock_get_filtered_files.assert_awaited_once_with(orphaned=True)
        mock_print_response.assert_not_called()
        mock_list_files.assert_not_called()
        mock_delete_files.assert_not_called()

        mock_get_filtered_files.reset_mock()
        self.mock_stdout.seek(0)

        ##################################
        # Request deletion and get error #

        mock_input.side_effect = None
        mock_input.return_value = "Y"
        code = 456
        mock_delete_files.return_value = (code, object())
        self.assertIsNone(
            await storage.find_orphaned_files(mock_client, delete=True)
        )
        self.assertIn(
            "Found 2 orphaned files with a total size of 3.0 MB",
            self.mock_stdout.getvalue(),
        )
        mock_get_filtered_files.assert_awaited_once_with(orphaned=True)
        mock_print_response.assert_called_once_with(code)
        mock_list_files.assert_not_called()
        mock_delete_files.assert_awaited_once_with(file1, file2)

        mock_get_filtered_files.reset_mock()
        mock_print_response.reset_mock()
        mock_delete_files.reset_mock()
        self.mock_stdout.seek(0)

        #######################
        # Successful deletion #

        mock_input.return_value = None
        mock_delete_files.return_value = (200, {"status": "ok"})
        self.assertIsNone(
            await storage.find_orphaned_files(
                mock_client,
                delete=True,
                yes_all=True,
            )
        )
        self.assertIn(
            "All orphaned files have been deleted from storage",
            self.mock_stdout.getvalue(),
        )
        mock_get_filtered_files.assert_awaited_once_with(orphaned=True)
        mock_print_response.assert_not_called()
        mock_list_files.assert_not_called()
        mock_delete_files.assert_awaited_once_with(file1, file2)

        mock_get_filtered_files.reset_mock()
        mock_delete_files.reset_mock()
        self.mock_stdout.seek(0)

        ####################
        # Partial deletion #

        not_deleted_hashes = ["abc", "foo", "bar"]
        mock_delete_files.return_value = (
            200,
            {"status": "incomplete", "not_deleted": not_deleted_hashes}
        )
        self.assertIsNone(
            await storage.find_orphaned_files(
                mock_client,
                delete=True,
                yes_all=True,
            )
        )
        self.assertNotIn(
            "All orphaned files have been deleted from storage",
            self.mock_stdout.getvalue(),
        )
        self.assertIn(
            "The following files were not deleted",
            self.mock_stdout.getvalue(),
        )
        self.assertIn(
            "\n".join(not_deleted_hashes),
            self.mock_stdout.getvalue(),
        )
        self.assertIn(
            "Please check the storage logs",
            self.mock_stdout.getvalue(),
        )
        mock_get_filtered_files.assert_awaited_once_with(orphaned=True)
        mock_print_response.assert_not_called()
        mock_list_files.assert_not_called()
        mock_delete_files.assert_awaited_once_with(file1, file2)

    def test_list_files(self) -> None:
        file_str_1 = "abcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefgh.mp4"
        file_str_2 = "foobarbafoobarbafoobarbafoobarbafoobarbafoobarbafoobarbafoobarba.mkv"
        file_1 = MagicMock(
            __str__=MagicMock(return_value=file_str_1),
            size=2.5 * MEGA,
        )
        file_2 = MagicMock(
            __str__=MagicMock(return_value=file_str_2),
            size=6.1111111111111 * MEGA,
        )
        expected_output = (
            "",
            "+ -------------------------------------------------------------------- + --------- +",
            "| File name (hash & extension)                                         |      Size |",
            "+ -------------------------------------------------------------------- + --------- +",
            f"| {file_str_1} |    2.5 MB |",
            f"| {file_str_2} |    6.1 MB |",
            "+ -------------------------------------------------------------------- + --------- +",
            "",
        )
        self.assertIsNone(storage.list_files(file_1, file_2))
        self.assertEqual(
            "\n".join(expected_output),
            self.mock_stdout.getvalue(),
        )

    @patch.object(storage, "print_response")
    async def test_show_distributor_nodes(
        self,
        mock_print_response: MagicMock,
    ) -> None:
        mock_output = "foobar"
        mock_data = MagicMock(json=MagicMock(return_value=mock_output))
        mock_get_dist_nodes = AsyncMock(return_value=(200, mock_data))
        mock_client = MagicMock(get_distributor_nodes=mock_get_dist_nodes)

        await storage.show_distributor_nodes(mock_client)
        self.assertEqual(
            mock_output + "\n",
            self.mock_stdout.getvalue(),
        )
        mock_get_dist_nodes.assert_awaited_once_with()
        mock_data.json.assert_called_once_with(indent=4)
        mock_print_response.assert_not_called()

        mock_get_dist_nodes.reset_mock()
        mock_data.json.reset_mock()

        code = 123
        mock_get_dist_nodes.return_value = (code, object())

        await storage.show_distributor_nodes(mock_client)
        mock_get_dist_nodes.assert_awaited_once_with()
        mock_data.json.assert_not_called()
        mock_print_response.assert_called_once_with(code)

    @patch.object(storage, "print_response")
    async def test_disable_distributor_node(
        self,
        mock_print_response: MagicMock,
    ) -> None:
        mock_response = object()
        mock_set_dist_state = AsyncMock(return_value=mock_response)
        mock_client = MagicMock(set_distributor_state=mock_set_dist_state)
        url = "abcdef"
        self.assertIsNone(
            await storage.disable_distributor_node(mock_client, url)
        )
        mock_set_dist_state.assert_awaited_once_with(url, enabled=False)
        mock_print_response.assert_called_once_with(mock_response)

    @patch.object(storage, "print_response")
    async def test_enable_distributor_node(
        self,
        mock_print_response: MagicMock,
    ) -> None:
        mock_response = object()
        mock_set_dist_state = AsyncMock(return_value=mock_response)
        mock_client = MagicMock(set_distributor_state=mock_set_dist_state)
        url = "abcdef"
        self.assertIsNone(
            await storage.enable_distributor_node(mock_client, url)
        )
        mock_set_dist_state.assert_awaited_once_with(url, enabled=True)
        mock_print_response.assert_called_once_with(mock_response)
