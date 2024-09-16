import shutil
from pathlib import Path
from tempfile import mkdtemp
from time import time
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from tests.silent_log import SilentLogMixin
from videbo.distributor.api.models import DistributorStatus
from videbo.distributor.copying_file import CopyingVideoFile
from videbo.distributor.exceptions import (
    NoSuchFile,
    NotEnoughSpace,
    NotSafeToDelete,
    TooManyWaitingClients,
    UnexpectedFileSize,
)
from videbo.distributor.file_controller import DistributorFileController, log
from videbo.hashed_file import HashedFile
from videbo.misc.constants import MEGA
from videbo.network import NetworkInterfaces

MODULE_NAME = "videbo.distributor.file_controller"


class DistributorFileControllerTestCase(SilentLogMixin, IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()

        # Mock global settings:
        self.settings_patcher = patch(f"{MODULE_NAME}.settings")
        self.mock_settings = self.settings_patcher.start()
        self.mock_settings.files_path = Path("/foo/bar")

        # Mock `Client` constructor (in the parent class):
        self.client_patcher = patch("videbo.file_controller.Client")
        self.mock_client_cls = self.client_patcher.start()

        # Mock `_load_file_list` method (in the parent class):
        self._load_file_list_patcher = patch(
            "videbo.file_controller.FileController._load_file_list"
        )
        self.mock_load_file_list = self._load_file_list_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        # Start with a fresh instance every time:
        DistributorFileController._instance = None
        self._load_file_list_patcher.stop()
        self.client_patcher.stop()
        self.settings_patcher.stop()

    def test___init__(self) -> None:
        file_controller = DistributorFileController()

        # Check parent class calls:
        self.mock_client_cls.assert_called_once_with()
        self.mock_load_file_list.assert_called_once_with()

        # Check default attribute values:
        self.assertDictEqual({}, file_controller._copying_files)
        self.assertEqual(0, file_controller._clients_waiting)

        # Check that it is a singleton:
        self.assertIs(file_controller, DistributorFileController())

    def test_files_dir(self) -> None:
        file_controller = DistributorFileController()
        self.assertEqual(
            self.mock_settings.files_path,
            file_controller.files_dir,
        )

    def test_load_file_predicate(self) -> None:
        file_controller = DistributorFileController()
        file_path = Path("/foo/bar/spam.mp4")
        self.assertTrue(file_controller.load_file_predicate(file_path))
        file_path = Path("/foo/bar/spam.txt")
        self.assertFalse(file_controller.load_file_predicate(file_path))
        file_path = MagicMock(suffix=".tmp")
        self.assertFalse(file_controller.load_file_predicate(file_path))
        file_path.unlink.assert_called_once_with()

    def test_get_path(self) -> None:
        file_controller = DistributorFileController()
        file_name = "abc.def"
        output = file_controller.get_path(file_name)
        self.assertEqual(
            Path(self.mock_settings.files_path, "ab", "abc.def"),
            output,
        )
        output = file_controller.get_path(file_name, temp=True)
        self.assertEqual(
            Path(self.mock_settings.files_path, "ab", "abc.def.tmp"),
            output,
        )
        a_hash = "a" * 64
        file = HashedFile(a_hash, ".ext", 123)
        output = file_controller.get_path(file)
        self.assertEqual(
            Path(self.mock_settings.files_path, "aa", a_hash + ".ext"),
            output,
        )


    async def test_get_file(self) -> None:
        file_controller = DistributorFileController()
        # File is present:
        file_controller._files["foo"] = mock_file_foo = MagicMock()
        output = await file_controller.get_file("foo")
        self.assertIs(mock_file_foo, output)

        # File is unknown:
        with self.assertRaises(NoSuchFile):
            await file_controller.get_file("bar")

        # File is being downloaded, but too many clients are waiting:
        file_controller._copying_files["bar"] = mock_file_bar = AsyncMock()
        file_controller._clients_waiting = 60
        with self.assertRaises(TooManyWaitingClients):
            await file_controller.get_file("bar")

        # File is being downloaded, but we can await it:
        file_controller._clients_waiting = 59
        async def mock_wait_until_finished(_timeout: int) -> None:
            """Simulates how a copying file is added to the controller."""
            file_controller._files["bar"] = mock_file_bar
        mock_file_bar.wait_until_finished = mock_wait_until_finished
        output = await file_controller.get_file("bar")
        self.assertIs(mock_file_bar, output)

    @patch(f"{MODULE_NAME}.get_free_disk_space")
    async def test_get_free_space(
        self,
        mock_get_free_disk_space: AsyncMock,
    ) -> None:
        file_controller = DistributorFileController()
        mock_get_free_disk_space.return_value = 1000.01
        self.mock_settings.distribution.leave_free_space_mb = 100.
        expected_output = 900.01
        output = await file_controller.get_free_space()
        self.assertEqual(expected_output, output)
        mock_get_free_disk_space.assert_awaited_once()

    @patch(f"{MODULE_NAME}.RequestFileJWTData.node_default")
    @patch.object(DistributorFileController, "get_free_space")
    @patch.object(DistributorFileController, "get_path")
    async def test__download_and_persist(
        self,
        mock_get_path: MagicMock,
        mock_get_free_space: AsyncMock,
        mock_jwt_node_default: MagicMock,
    ) -> None:
        # Mock asynchronous response data stream from client:
        mock_data = [b"abcd", b"abcd", b"abcd", b"abcd"]
        mock_client_read = self.mock_client_cls.return_value.request_file_read
        mock_client_read.return_value.__aiter__.return_value = mock_data
        file_controller = DistributorFileController()
        mock_file = MagicMock(
            hash="foo",
            ext=".bar",
            size=16,
            source_url="https://example.com/videbo1",
            loaded_bytes=0,
        )
        mock_file.as_finished.return_value = mock_file
        file_controller._copying_files["foo"] = mock_file
        mock_jwt_node_default.return_value = mock_jwt = object()
        test_dir = Path(mkdtemp(prefix="videbo_test"))
        try:
            temp_path = Path(test_dir, "spam", "foo.bar.tmp")
            final_path = Path(test_dir, "eggs", "foo.bar")
            mock_get_path.side_effect = (temp_path, final_path)

            # Not enough space for our mock file:
            mock_get_free_space.return_value = 10. / MEGA
            with self.assertRaises(NotEnoughSpace):
                await file_controller._download_and_persist(mock_file)
            self.assertListEqual([], list(test_dir.iterdir()))
            self.assertDictEqual({}, file_controller._files)
            self.assertEqual(0, file_controller._files_total_size)
            mock_get_path.assert_not_called()
            mock_jwt_node_default.assert_not_called()

            # Successful download and move:
            mock_get_free_space.return_value = 10.
            call_time = int(time())
            await file_controller._download_and_persist(mock_file)
            self.assertTrue(temp_path.parent.is_dir())
            self.assertListEqual([], list(temp_path.parent.iterdir()))
            self.assertTrue(final_path.is_file())
            with final_path.open("rb") as f:
                self.assertEqual(b"abcdabcdabcdabcd", f.read())
            self.assertIn("foo", file_controller)
            self.assertEqual(16, file_controller._files_total_size)
            self.assertTupleEqual(
                ("foo", ".bar"),
                mock_jwt_node_default.call_args[0],
            )
            self.assertGreaterEqual(
                mock_jwt_node_default.call_args[1]["expiration_time"],
                call_time + 300,
            )
            self.assertLess(
                mock_jwt_node_default.call_args[1]["expiration_time"],
                call_time + 302,
            )
            mock_client_read.assert_called_once_with(
                "https://example.com/videbo1/file",
                mock_jwt,
                chunk_size=MEGA,
                timeout=2. * 60 * 60,
            )
            mock_file.as_finished.assert_called_once_with()

            file_controller._files.clear()
            file_controller._files_total_size = 0
            temp_path.unlink(missing_ok=True)
            final_path.unlink(missing_ok=True)
            mock_file.as_finished.reset_mock()

            # File size does not match download size:
            mock_file.size = 15
            mock_get_path.side_effect = (temp_path, final_path)
            with self.assertRaises(UnexpectedFileSize):
                await file_controller._download_and_persist(mock_file)
            self.assertTrue(temp_path.is_file())
            self.assertFalse(final_path.is_file())
            mock_file.as_finished.assert_not_called()
        finally:
            shutil.rmtree(test_dir)

    @patch.object(DistributorFileController, "_download_and_persist")
    async def test_download_and_persist(
        self,
        mock__download_and_persist: AsyncMock,
    ) -> None:
        file_controller = DistributorFileController()
        mock_file = MagicMock(
            hash="foo",
            source_url="https://example.com/videbo1",
        )
        file_controller._copying_files["foo"] = mock_file
        await file_controller.download_and_persist(mock_file)
        mock__download_and_persist.assert_called_once_with(mock_file)
        self.assertDictEqual({}, file_controller._copying_files)

        mock__download_and_persist.reset_mock()
        mock__download_and_persist.side_effect = Exception
        with self.assertLogs(DistributorFileController.log, level="ERROR"):
            await file_controller.download_and_persist(mock_file)
        mock__download_and_persist.assert_called_once_with(mock_file)
        self.assertDictEqual({}, file_controller._copying_files)

    @patch(f"{MODULE_NAME}.TaskManager.fire_and_forget")
    @patch.object(
        DistributorFileController,
        "download_and_persist",
        new_callable=MagicMock,
    )
    def test_schedule_copying(
        self,
        mock_download_and_persist: MagicMock,
        mock_fire_and_forget: MagicMock,
    ) -> None:
        file_controller = DistributorFileController()
        file_controller._files["foo"] = MagicMock()
        output = file_controller.schedule_copying(
            "foo",
            ".bar",
            from_url="https://example.com/videbo1",
            expected_file_size=123,
        )
        self.assertIsNone(output)
        mock_download_and_persist.assert_not_called()
        mock_fire_and_forget.assert_not_called()

        file_controller._files.clear()
        file_controller._copying_files["foo"] = mock_copying_file = MagicMock()
        output = file_controller.schedule_copying(
            "foo",
            ".bar",
            from_url="https://example.com/videbo1",
            expected_file_size=123,
        )
        self.assertIs(mock_copying_file, output)
        mock_download_and_persist.assert_not_called()
        mock_fire_and_forget.assert_not_called()

        a_hash = "a" * 64
        output = file_controller.schedule_copying(
            a_hash,
            ".ext",
            from_url="https://example.com/videbo1",
            expected_file_size=123,
        )
        self.assertIsInstance(output, CopyingVideoFile)
        self.assertEqual(a_hash, output.hash)
        self.assertEqual(".ext", output.ext)
        self.assertEqual(123, output.size)
        self.assertEqual("https://example.com/videbo1", output.source_url)
        self.assertEqual(0, output.loaded_bytes)
        mock_download_and_persist.assert_called_once_with(output)
        mock_fire_and_forget.assert_called_once_with(
            mock_download_and_persist.return_value,
            name="copy_file_from_node",
        )

    @patch.object(DistributorFileController, "get_path")
    async def test_delete_file(self, mock_get_path: MagicMock) -> None:
        file_controller = DistributorFileController()
        with self.assertRaises(NoSuchFile):
            await file_controller.delete_file("foo")
        mock_get_path.assert_not_called()

        self.mock_settings.dist_last_request_safety_seconds = 100
        mock_file = MagicMock(last_requested=time())
        file_controller._files["foo"] = mock_file
        with self.assertRaises(NotSafeToDelete):
            await file_controller.delete_file("foo")
        self.assertIn("foo", file_controller)
        mock_get_path.assert_not_called()

        self.mock_settings.dist_last_request_safety_seconds = 0
        mock_get_path.return_value = mock_path = MagicMock()
        await file_controller.delete_file("foo")
        self.assertDictEqual({}, file_controller._files)
        mock_get_path.assert_called_once_with(mock_file)
        mock_path.unlink.assert_called_once_with()

    @patch.object(NetworkInterfaces, "get_instance")
    @patch.object(DistributorFileController, "get_free_space")
    async def test_get_status(
        self,
        mock_get_free_disk_space: AsyncMock,
        mock_ni_cls: MagicMock,
    ) -> None:
        self.mock_settings.tx_max_rate_mbit = tx_max = 12345
        self.mock_settings.public_base_url = base_url = "example.com"
        mock_get_free_disk_space.return_value = space = 420
        mock_copying_file_1 = MagicMock(
            hash="bar",
            ext=".baz",
            loaded_bytes=1,
            size=2,
            time_started=123,
        )
        mock_copying_file_2 = MagicMock(
            hash="spam",
            ext=".eggs",
            loaded_bytes=3,
            size=4,
            time_started=456,
        )
        file_controller = DistributorFileController()
        file_controller._files["foo"] = MagicMock()
        file_controller._files_total_size = 69 * MEGA
        file_controller._clients_waiting = 42
        file_controller._copying_files = {
            "bar": mock_copying_file_1,
            "spam": mock_copying_file_2,
        }
        output = await file_controller.get_status()
        self.assertIsInstance(output, DistributorStatus)
        self.assertEqual(output.tx_max_rate, tx_max)
        self.assertEqual(output.files_total_size, 69)
        self.assertEqual(output.files_count, 1)
        self.assertEqual(output.free_space, space)
        self.assertEqual(output.bound_to_storage_node_base_url, base_url)
        self.assertEqual(output.waiting_clients, 42)
        self.assertEqual(2, len(output.copy_files_status))
        for idx, file in enumerate((mock_copying_file_1, mock_copying_file_2)):
            copy_file_status = output.copy_files_status[idx]
            self.assertEqual(file.hash, copy_file_status.hash)
            self.assertEqual(file.ext, copy_file_status.file_ext)
            self.assertEqual(file.loaded_bytes, copy_file_status.loaded)
            self.assertEqual(file.size, copy_file_status.file_size)
        mock_update_node_status = mock_ni_cls.return_value.update_node_status
        mock_update_node_status.assert_called_once_with(output, logger=log)
