import shutil
import time
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, Mock, PropertyMock, call, patch
from pathlib import Path
from tempfile import mkdtemp
from typing import TypeVar

from tests.silent_log import SilentLogMixin
from videbo.exceptions import LMSInterfaceError
from videbo.lms_api import LMS
from videbo.misc.constants import JPG_EXT, MEGA
from videbo.misc.functions import move_file
from videbo.network import NetworkInterfaces
from videbo.storage.api.models import StorageStatus
from videbo.storage.file_controller import StorageFileController, log


M = TypeVar('M', bound=Mock)

MODULE_NAME = 'videbo.storage.file_controller'
FOO, BAR, BAZ = 'foo', 'bar', 'baz'


class StorageFileControllerTestCase(SilentLogMixin, IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()

        # Mock global settings:
        self.settings_patcher = patch(f"{MODULE_NAME}.settings")
        self.mock_settings = self.settings_patcher.start()
        self.mock_settings.files_path = Path("/tmp/foo")
        self.mock_settings.thumb_cache_max_mb = 0

        # Mock `_prepare_directories` method:
        self._prepare_directories_patcher = patch.object(
            StorageFileController,
            "_prepare_directories",
        )
        self.mock_prep_dirs = self._prepare_directories_patcher.start()

        # Mock `Client` constructor (in the parent class):
        self.client_patcher = patch("videbo.file_controller.Client")
        self.mock_client_cls = self.client_patcher.start()

        # Mock `_load_file_list` method (in the parent class):
        self._load_file_list_patcher = patch(
            "videbo.file_controller.FileController._load_file_list"
        )
        self.mock_load_file_list = self._load_file_list_patcher.start()

        # Mock `ThumbnailCache` constructor:
        self.thumb_cache_patcher = patch(f"{MODULE_NAME}.ThumbnailCache")
        self.mock_thumb_cache_cls = self.thumb_cache_patcher.start()

        # Mock `DistributionController` constructor:
        self.mock_dist_controller = MagicMock()
        self.dist_controller_patcher = patch(
            f"{MODULE_NAME}.DistributionController",
            return_value=self.mock_dist_controller,
        )
        self.mock_dist_controller_cls = self.dist_controller_patcher.start()

        # Mock `Periodic` constructor:
        self.periodic_patcher = patch(f"{MODULE_NAME}.Periodic")
        self.mock_periodic_cls = self.periodic_patcher.start()

        # Initialize file_controller instance for convenience:
        self.file_controller = StorageFileController()

    def tearDown(self) -> None:
        super().tearDown()
        # Start with a fresh instance every time:
        StorageFileController._instance = None
        self.periodic_patcher.stop()
        self.dist_controller_patcher.stop()
        self.thumb_cache_patcher.stop()
        self._load_file_list_patcher.stop()
        self.client_patcher.stop()
        self._prepare_directories_patcher.stop()
        self.settings_patcher.stop()

    def test___init__(self) -> None:
        # self.file_controller is initialized in setUp method

        # Check dir preparation:
        self.mock_prep_dirs.assert_called_once_with()

        # Check parent class calls:
        self.mock_client_cls.assert_called_once_with()
        self.mock_load_file_list.assert_called_once_with()

        # Check that utility classes were correctly initialized:
        self.assertIs(
            self.file_controller.thumb_memory_cache,
            self.mock_thumb_cache_cls.return_value,
        )
        self.assertIs(
            self.file_controller.distribution_controller,
            self.mock_dist_controller_cls.return_value,
        )
        self.mock_dist_controller_cls.assert_called_once_with(
            node_urls=self.mock_settings.distribution.static_node_base_urls,
            http_client=self.mock_client_cls(),
        )

        # Check default attribute values:
        self.assertEqual(self.file_controller.num_current_uploads, 0)

        # Check that periodic tasks were launched as expected:
        self.assertListEqual(
            self.mock_periodic_cls.call_args_list,
            [
                call(self.file_controller.remove_old_temp_files),
                call(self.file_controller.discard_old_video_views),
            ]
        )
        mock_periodic_instance = self.mock_periodic_cls.return_value
        self.assertListEqual(
            mock_periodic_instance.call_args_list,
            [
                call(self.mock_settings.temp_file_cleanup_freq),
                call(self.mock_settings.views_update_freq),
            ]
        )

        # Check that it is a singleton:
        self.assertIs(self.file_controller, StorageFileController())

    @patch.object(Path, "chmod")
    @patch.object(Path, "mkdir")
    def test__prepare_directories_and_dir_properties(
        self,
        mock_mkdir: MagicMock,
        mock_chmod: MagicMock,
    ) -> None:
        self._prepare_directories_patcher.stop()
        self.file_controller._prepare_directories()
        self.assertEqual(
            self.file_controller.files_dir,
            Path(self.mock_settings.files_path, "storage"),
        )
        self.assertEqual(
            self.file_controller.temp_dir,
            Path(self.mock_settings.files_path, "temp"),
        )
        self.assertEqual(
            self.file_controller.temp_out_dir,
            Path(self.mock_settings.files_path, "temp", "out"),
        )
        self.assertListEqual(
            [call(exist_ok=True)] * 3,
            mock_mkdir.call_args_list,
        )
        mock_chmod.assert_called_once_with(0o777)

    def test_load_file_predicate(self) -> None:
        file_path = Path("/foo/bar/spam.jpg")
        self.assertFalse(self.file_controller.load_file_predicate(file_path))
        file_path = Path("/foo/bar/spam.txt.mp4")
        self.assertFalse(self.file_controller.load_file_predicate(file_path))
        file_path = Path("/foo/bar/spam.txt")
        self.assertFalse(self.file_controller.load_file_predicate(file_path))
        file_path = Path("/foo/bar/spam.mp4")
        self.assertTrue(self.file_controller.load_file_predicate(file_path))
        file_path = Path("/foo/bar/spam.webm")
        self.assertTrue(self.file_controller.load_file_predicate(file_path))

    @patch.object(
        StorageFileController,
        "files_dir",
        new_callable=PropertyMock,
    )
    @patch.object(
        StorageFileController,
        "temp_dir",
        new_callable=PropertyMock,
    )
    def test_get_path_and_get_thumbnail_path(
        self,
        mock_temp_dir: PropertyMock,
        mock_files_dir: PropertyMock,
    ) -> None:
        mock_temp_dir.return_value = temp_dir = Path("spam")
        mock_files_dir.return_value = files_dir = Path("eggs")

        name = "abcdef"
        output = self.file_controller.get_path(name, temp=True)
        expected_output = Path(temp_dir, name)
        self.assertEqual(expected_output, output)

        output = self.file_controller.get_thumbnail_path(name, num=5, temp=True)
        expected_output = Path(temp_dir, "abcdef_5.jpg")
        self.assertEqual(expected_output, output)

        output = self.file_controller.get_path(name)
        expected_output = Path(files_dir, "ab", name)
        self.assertEqual(expected_output, output)

        output = self.file_controller.get_thumbnail_path(name, num=42)
        expected_output = Path(files_dir, "ab", "abcdef_42.jpg")
        self.assertEqual(expected_output, output)


    @patch.object(LMS, "filter_orphaned_videos")
    @patch.object(StorageFileController, "iter_files")
    async def test_filtered_files(
        self,
        mock_iter_files: MagicMock,
        mock_filter_orphaned_videos: AsyncMock,
    ) -> None:
        test_extensions = ['.mp4']
        expected_file = MagicMock(hash='foo', ext='.mp4')
        wrong_ext_file = MagicMock(hash='bar', ext='.webm')
        mock_iter_files.return_value = files = (wrong_ext_file, expected_file)
        mock_filter_orphaned_videos.return_value = ['foo']

        # Test regular filter case without orphan status
        async_iterator = self.file_controller.filtered_files(
            extensions=test_extensions
        )
        out = [f async for f in async_iterator]
        self.assertListEqual([expected_file], out)
        mock_filter_orphaned_videos.assert_not_awaited()

        # Test with orphan status `True`
        async_iterator = self.file_controller.filtered_files(orphaned=True)
        out = [f async for f in async_iterator]
        self.assertEqual([expected_file], out)
        mock_filter_orphaned_videos.assert_awaited_once_with(
            *files,
            client=self.mock_client_cls.return_value,
        )

        # Test with orphan status `False`
        mock_filter_orphaned_videos.reset_mock()
        async_iterator = self.file_controller.filtered_files(
            orphaned=False,
            extensions=test_extensions,
        )
        out = [f async for f in async_iterator]
        self.assertEqual([], out)
        mock_filter_orphaned_videos.assert_awaited_once_with(
            *files,
            client=self.mock_client_cls.return_value,
        )

        # Test exception raising
        mock_filter_orphaned_videos.reset_mock()
        async_iterator = self.file_controller.filtered_files(
            extensions=['wrong']
        )
        with self.assertRaises(ValueError):
            _ = [_ async for _ in async_iterator]
        mock_filter_orphaned_videos.assert_not_awaited()

    @patch(f"{MODULE_NAME}.run_in_default_executor")
    @patch.object(
        StorageFileController,
        "files_dir",
        new_callable=PropertyMock,
    )
    @patch.object(
        StorageFileController,
        "temp_dir",
        new_callable=PropertyMock,
    )
    async def test_store_file_permanently(
        self,
        mock_temp_dir: PropertyMock,
        mock_files_dir: PropertyMock,
        mock_run_in_default_executor: AsyncMock,
    ) -> None:
        mock_temp_dir.return_value = temp_dir = Path("spam")
        mock_files_dir.return_value = files_dir = Path("eggs")

        output = await self.file_controller.store_file_permanently("abcdef")
        expected_source = Path(temp_dir, "abcdef")
        expected_destination = Path(files_dir, "ab", "abcdef")
        self.assertEqual(expected_destination, output)
        mock_run_in_default_executor.assert_awaited_once_with(
            move_file,
            expected_source,
            expected_destination,
            0o755,
        )

    @patch(f"{MODULE_NAME}.StoredVideoFile.from_path")
    @patch.object(StorageFileController, "store_file_permanently")
    async def test_store_permanently(
        self,
        mock_store_file_permanently: AsyncMock,
        mock_stored_file_from_path: MagicMock,
    ) -> None:
        file_name = "foo.bar"
        mock_store_file_permanently.return_value = mock_path = Path(file_name)
        mock_stored_file = MagicMock(hash="foo", ext="bar", size=1)
        mock_stored_file_from_path.return_value = mock_stored_file

        thumbnail_count = 4
        await self.file_controller.store_permanently(
            file_name,
            thumbnail_count=thumbnail_count,
        )
        # Check that the mock file was added to the controller:
        self.assertIn("foo", self.file_controller)
        self.assertEqual(mock_stored_file, self.file_controller["foo"])
        # Check that the mocked `store_file_permanently` method was called for
        # the "video" file first and then for all 4 thumbnails:
        calls = [
            call(file_name),
            call(f"foo_0{JPG_EXT}"),
            call(f"foo_1{JPG_EXT}"),
            call(f"foo_2{JPG_EXT}"),
            call(f"foo_3{JPG_EXT}"),
        ]
        self.assertListEqual(calls, mock_store_file_permanently.await_args_list)
        mock_stored_file_from_path.assert_called_once_with(mock_path)

    @patch(f"{MODULE_NAME}.run_in_default_executor")
    @patch.object(StorageFileController, "get_path")
    async def test_remove_video(
        self,
        mock_get_path: MagicMock,
        mock_run_in_default_executor: AsyncMock,
    ) -> None:
        mock_file = MagicMock(hash="foo", size=100 * MEGA)
        self.file_controller._files_total_size = 200 * MEGA
        self.file_controller._files["foo"] = mock_file
        # Ensure we set the correct private attributes here first:
        self.assertIn("foo", self.file_controller)
        self.assertEqual(200, self.file_controller.files_total_size_mb)
        mock_get_path.return_value = mock_path = Path("abcdef")
        await self.file_controller.remove_video(mock_file)
        # Check that the mock file was removed from the controller:
        self.assertNotIn("foo", self.file_controller)
        self.assertEqual(100, self.file_controller.files_total_size_mb)
        # Check that the mock functions were called as expected:
        mock_get_path.assert_called_once_with(str(mock_file))
        mock_run_in_default_executor.assert_awaited_once_with(mock_path.unlink)
        self.mock_dist_controller.remove_from_nodes.assert_called_once_with(
            mock_file
        )

    @patch(f"{MODULE_NAME}.run_in_default_executor")
    @patch.object(StorageFileController, "get_path")
    async def test_remove_thumbnails(
        self,
        mock_get_path: MagicMock,
        mock_run_in_default_executor: AsyncMock,
    ) -> None:
        mock_get_path.return_value = mock_path = MagicMock()

        test_count = 4
        self.mock_thumb_cache_cls().__delitem__.side_effect = (
            None, KeyError, None, KeyError
        )

        self.assertIsNone(await self.file_controller.remove_thumbnails(
            "foo",
            count=test_count,
        ))
        self.assertListEqual(
            [call(f"foo_{num}{JPG_EXT}") for num in range(test_count)],
            mock_get_path.call_args_list,
        )
        self.assertListEqual(
            [call(mock_path)] * test_count,
            self.mock_thumb_cache_cls().__delitem__.call_args_list,
        )
        self.assertListEqual(
            [call(mock_path.unlink)] * test_count,
            mock_run_in_default_executor.await_args_list,
        )

    @patch.object(StorageFileController, "remove_video")
    @patch.object(StorageFileController, "remove_thumbnails")
    @patch.object(LMS, "filter_orphaned_videos")
    async def test_remove_files(
        self,
        mock_filter_orphaned_videos: AsyncMock,
        mock_remove_thumbnails: AsyncMock,
        mock_remove_video: AsyncMock,
    ) -> None:
        test_hashes = ["foo", "bar", "baz"]
        test_origin = "abcde"
        mock_file_foo, mock_file_bar = object(), object()
        self.file_controller._files = {
            "foo": mock_file_foo,
            "bar": mock_file_bar,
        }
        mock_filter_orphaned_videos.return_value = ["foo"]

        # Only `foo` is known and an orphan, the others should not be deleted.
        not_deleted = await self.file_controller.remove_files(
            *test_hashes,
            origin=test_origin,
        )
        self.assertSetEqual({"bar", "baz"}, not_deleted)
        mock_filter_orphaned_videos.assert_awaited_once_with(
            mock_file_foo,
            mock_file_bar,
            client=self.mock_client_cls.return_value,
            origin=test_origin,
        )
        mock_remove_thumbnails.assert_awaited_once_with("foo")
        mock_remove_video.assert_awaited_once_with(mock_file_foo)

        mock_filter_orphaned_videos.reset_mock()
        mock_remove_thumbnails.reset_mock()
        mock_remove_video.reset_mock()
        ########################################################################

        # Failure to contact LMS; no files should be deleted.
        mock_filter_orphaned_videos.side_effect = LMSInterfaceError
        not_deleted = await self.file_controller.remove_files(
            *test_hashes,
            origin=test_origin,
        )
        self.assertSetEqual({"foo", "bar", "baz"}, not_deleted)
        mock_filter_orphaned_videos.assert_awaited_once_with(
            mock_file_foo,
            mock_file_bar,
            client=self.mock_client_cls.return_value,
            origin=test_origin,
        )
        mock_remove_thumbnails.assert_not_called()
        mock_remove_video.assert_not_called()
        mock_filter_orphaned_videos.reset_mock()
        ########################################################################

        # Error while removing video.
        mock_remove_video.side_effect = OSError
        mock_filter_orphaned_videos.side_effect = None
        mock_filter_orphaned_videos.return_value = ["foo"]
        with self.assertLogs(log, level="ERROR"):
            not_deleted = await self.file_controller.remove_files(
                *test_hashes,
                origin=test_origin,
            )
        self.assertSetEqual({"foo", "bar", "baz"}, not_deleted)
        mock_filter_orphaned_videos.assert_awaited_once_with(
            mock_file_foo,
            mock_file_bar,
            client=self.mock_client_cls.return_value,
            origin=test_origin,
        )
        mock_remove_thumbnails.assert_awaited_once_with("foo")
        mock_remove_video.assert_awaited_once_with(mock_file_foo)

    @patch.object(
        StorageFileController,
        "remove_files",
        new_callable=MagicMock,  # No need for async mock
    )
    @patch(f"{MODULE_NAME}.TaskManager.fire_and_forget")
    def test_schedule_file_removal(
        self,
        mock_fire_and_forget: MagicMock,
        mock_remove_files: MagicMock,
    ) -> None:
        with self.assertLogs(log, level="WARNING"):
            self.file_controller.schedule_file_removal(
                file_hash="foo",
                file_ext=".bar",
                origin="baz",
            )
        self.file_controller._files["foo"] = MagicMock(ext=".bar")
        self.file_controller.schedule_file_removal(
            file_hash="foo",
            file_ext=".bar",
            origin="baz",
        )
        mock_remove_files.assert_called_once_with("foo", origin="baz")
        mock_fire_and_forget.assert_called_once_with(
            mock_remove_files.return_value
        )

    @patch.object(
        StorageFileController,
        "temp_dir",
        new_callable=PropertyMock,
    )
    def test__remove_old_temp_files(self, mock_temp_dir: PropertyMock) -> None:
        self.mock_settings.max_temp_storage_hours = 1 / 3600  # 1 sec

        temp_dir = Path(mkdtemp(prefix="videbo_test"))
        try:
            mock_temp_dir.return_value = temp_dir
            file_old_enough = Path(temp_dir, "foo")
            file_too_new = Path(temp_dir, "bar")
            dir1 = Path(temp_dir, "dir1")
            file_old_enough.touch()
            dir1.mkdir()
            time.sleep(2)
            file_too_new.touch()
            output = self.file_controller._remove_old_temp_files()
            self.assertEqual(1, output)
            self.assertFalse(file_old_enough.exists())
            self.assertTrue(file_too_new.exists())
            self.assertTrue(dir1.exists())
        finally:
            shutil.rmtree(temp_dir)

    @patch(f"{MODULE_NAME}.run_in_default_executor")
    async def test_remove_old_temp_files(self, mock_run: AsyncMock) -> None:
        mock_run.return_value = count = 42
        with self.assertLogs(log, level="INFO") as log_ctx:
            await self.file_controller.remove_old_temp_files()
            self.assertEqual(
                f"Cleaned up temp directory: Removed {count} old file(s).",
                log_ctx.records[0].msg,
            )
        mock_run.assert_awaited_once_with(
            self.file_controller._remove_old_temp_files
        )

    @patch(f"{MODULE_NAME}.time")
    async def test_discard_old_video_views(self, mock_time: MagicMock) -> None:
        self.mock_settings.views_retention_seconds = sec = 3.14
        mock_time.return_value = mock_now = 42.
        file1, file2 = MagicMock(), MagicMock()
        self.file_controller._files = {1: file1, 2: file2}
        await self.file_controller.discard_old_video_views()
        file1.discard_views_older_than.assert_called_once_with(mock_now - sec)
        file2.discard_views_older_than.assert_called_once_with(mock_now - sec)

    @patch(f"{MODULE_NAME}.get_free_disk_space")
    @patch.object(NetworkInterfaces, 'get_instance')
    async def test_get_status(
        self,
        mock_ni_cls: MagicMock,
        mock_get_free_disk_space: AsyncMock,
    ) -> None:
        self.mock_settings.tx_max_rate_mbit = tx_max = 12345
        mock_iter_nodes = self.mock_dist_controller_cls.return_value.iter_nodes
        mock_iter_nodes.return_value = [
            MagicMock(base_url="foo"),
            MagicMock(base_url="bar"),
        ]
        mock_update_node_status = mock_ni_cls.return_value.update_node_status
        mock_get_free_disk_space.return_value = mock_free_disk_space = 999
        self.file_controller._files = {"foo": Mock(), "bar": Mock()}
        self.file_controller._files_total_size = 420 * MEGA
        output = await self.file_controller.get_status()
        self.assertIsInstance(output, StorageStatus)
        self.assertEqual(tx_max, output.tx_max_rate)
        self.assertEqual(420, output.files_total_size)
        self.assertEqual(2, output.files_count)
        self.assertEqual(mock_free_disk_space, output.free_space)
        self.assertEqual(["foo", "bar"], output.distributor_nodes)
        self.assertEqual(
            self.file_controller.num_current_uploads,
            output.num_current_uploads,
        )
        mock_get_free_disk_space.assert_awaited_once_with(
            str(self.mock_settings.files_path)
        )
        mock_update_node_status.assert_called_once_with(output, logger=log)
