import logging
import shutil
import time
from collections.abc import Iterator
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, Mock, PropertyMock, call, patch
from pathlib import Path
from tempfile import mkdtemp
from typing import TypeVar

from tests.silent_log import SilentLogMixin
from videbo.misc.constants import JPG_EXT, MEGA
from videbo.storage import util


M = TypeVar('M', bound=Mock)

FOO, BAR, BAZ = 'foo', 'bar', 'baz'


class FileStorageTestCase(SilentLogMixin, IsolatedAsyncioTestCase):
    path: Path

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.path = Path(mkdtemp(prefix="videbo_test"))

    def setUp(self) -> None:
        super().setUp()

        # All kinds of mocking:
        self.settings_patcher = patch.object(util, "settings")
        self.mock_settings = self.settings_patcher.start()
        self.mock_settings.files_path = self.path
        self.mock_settings.thumb_cache_max_mb = 0

        self._prepare_directories_patcher = patch.object(util.FileStorage, "_prepare_directories")
        self.mock_prep_dirs = self._prepare_directories_patcher.start()

        self.thumb_cache_patcher = patch.object(util, "ThumbnailCache")
        self.mock_thumb_cache_cls = self.thumb_cache_patcher.start()

        self.client_patcher = patch.object(util, "Client")
        self.mock_client_cls = self.client_patcher.start()

        self.mock_dist_controller = MagicMock()
        self.dist_controller_patcher = patch.object(
            util,
            "DistributionController",
            return_value=self.mock_dist_controller,
        )
        self.mock_dist_controller_cls = self.dist_controller_patcher.start()

        self._load_file_list_patcher = patch.object(util.FileStorage, "_load_file_list")
        self.mock_load_files = self._load_file_list_patcher.start()

        self.periodic_patcher = patch.object(util, "Periodic")
        self.mock_periodic_cls = self.periodic_patcher.start()

        # Initialize storage instance:
        self.storage = util.FileStorage()

    def tearDown(self) -> None:
        super().tearDown()
        self.periodic_patcher.stop()
        self._load_file_list_patcher.stop()
        self.dist_controller_patcher.stop()
        self.client_patcher.stop()
        self.thumb_cache_patcher.stop()
        self._prepare_directories_patcher.stop()
        self.settings_patcher.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        shutil.rmtree(cls.path)

    def test___init__(self) -> None:
        # self.storage is initialized in setUp method

        # Check dir preparation:
        self.mock_prep_dirs.assert_called_once_with()

        # Check that utility lasses were correctly initialized:
        self.assertIs(
            self.storage.thumb_memory_cache,
            self.mock_thumb_cache_cls.return_value,
        )
        self.assertIs(
            self.storage.http_client,
            self.mock_client_cls.return_value,
        )
        self.assertIs(
            self.storage.distribution_controller,
            self.mock_dist_controller_cls.return_value,
        )
        self.mock_dist_controller_cls.assert_called_once_with(
            node_urls=self.mock_settings.distribution.static_node_base_urls,
            http_client=self.mock_client_cls(),
        )

        # Check default attribute values:
        self.assertEqual(self.storage._cached_files, {})
        self.assertEqual(self.storage._cached_files_total_size, 0)

        # Check that periodic tasks were launched as expected:
        self.assertListEqual(
            self.mock_periodic_cls.call_args_list,
            [
                call(self.storage.remove_old_temp_files),
                call(self.storage.discard_old_video_views),
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

        # Now try to initialize with an invalid files path:
        self.mock_settings.files_path = Path("/doesnotexist")
        with self.assertRaises(NotADirectoryError), self.assertLogs():
            util.FileStorage()

    @patch.object(util.FileStorage, "get_instance")
    async def test_app_context(self, mock_get_fs_instance: MagicMock) -> None:
        iterator = util.FileStorage.app_context(MagicMock())
        self.assertIsNone(await iterator.__anext__())
        mock_get_fs_instance.assert_called_once_with()

        mock_get_fs_instance.reset_mock()

        with self.assertRaises(StopAsyncIteration):
            await iterator.__anext__()

        mock_get_fs_instance.assert_not_called()

    @patch.object(util.FileStorage, "__init__", return_value=None)
    def test_get_instance(self, mock___init__: MagicMock) -> None:
        self.assertIsNone(util.FileStorage._instance)
        obj = util.FileStorage.get_instance()
        self.assertIs(obj, util.FileStorage._instance)
        mock___init__.assert_called_once_with()

        mock___init__.reset_mock()

        check_obj = util.FileStorage.get_instance()
        self.assertIs(check_obj, obj)
        mock___init__.assert_not_called()

    @patch.object(util.Path, "chmod")
    @patch.object(util.Path, "mkdir")
    def test__prepare_directories(self, mock_mkdir: MagicMock, mock_chmod: MagicMock) -> None:
        self._prepare_directories_patcher.stop()
        self.storage._prepare_directories()
        self.assertEqual(self.storage.storage_dir, Path(self.path, "storage"))
        self.assertEqual(self.storage.temp_dir, Path(self.path, "temp"))
        self.assertEqual(self.storage.temp_out_dir, Path(self.path, "temp", "out"))
        self.assertListEqual(
            [call(exist_ok=True)] * 3,
            mock_mkdir.call_args_list,
        )
        mock_chmod.assert_called_once_with(0o777)

    @patch.object(util.FileStorage, '_add_video_to_cache', return_value='foo')
    def test__load_file_list(self, mock__add_video_to_cache: MagicMock) -> None:
        self._load_file_list_patcher.stop()

        def do_tests():
            # add method called, no log entry made
            self.storage._cached_files = {n: None for n in range(21)}
            with patch.object(util, 'log') as mock_logger:
                self.storage._load_file_list()
                mock__add_video_to_cache.assert_called_once_with(hash_part, ext_part, file_to_load)
                mock_logger.assert_not_called()
            mock__add_video_to_cache.reset_mock()

            # add method called
            self.storage._cached_files = {n: None for n in range(20)}
            with self.assertLogs(util.log):
                self.storage._load_file_list()
                mock__add_video_to_cache.assert_called_once_with(hash_part, ext_part, file_to_load)
            mock__add_video_to_cache.reset_mock()

            # add method called (other log case)
            self.storage._cached_files = {n: None for n in range(10)}
            with self.assertLogs(util.log):
                self.storage._load_file_list()
                mock__add_video_to_cache.assert_called_once_with(hash_part, ext_part, file_to_load)

            mock__add_video_to_cache.side_effect = Exception
            with self.assertLogs(util.log, logging.ERROR):
                self.storage._load_file_list()

        # Dummy files for this test:
        self.storage.storage_dir = Path(self.path, "storage")
        test_dir = Path(self.storage.storage_dir, 'test')
        hash_part, ext_part = 'file_hash', '.mp4'
        file_to_load = Path(test_dir, hash_part + ext_part)
        wrong_ext = Path(test_dir, 'wrong.ext')
        two_dot_file = Path(test_dir, 'some.file.mp4')
        not_a_file = Path(test_dir, 'not_a_file')
        self.storage.storage_dir.mkdir()
        test_dir.mkdir()
        file_to_load.touch()
        wrong_ext.touch()
        two_dot_file.touch()
        not_a_file.mkdir()
        try:
            do_tests()
        finally:
            not_a_file.rmdir()
            two_dot_file.unlink()
            wrong_ext.unlink()
            file_to_load.unlink()
            test_dir.rmdir()
            self.storage.storage_dir.rmdir()

    @patch.object(util, 'StoredVideoFile')
    def test__add_video_to_cache(self, mock_hvf_class: MagicMock) -> None:
        mock_file_obj = MagicMock()
        mock_hvf_class.return_value = mock_file_obj
        mock_file_size = 10
        mock_stat_method = MagicMock(return_value=MagicMock(st_size=mock_file_size))
        mock_file_path = MagicMock()
        mock_file_path.stat = mock_stat_method

        test_total_size, test_hash, test_ext = 10, 'abc', '.ext'
        self.storage._cached_files_total_size = test_total_size

        out = self.storage._add_video_to_cache(file_hash=test_hash, file_ext=test_ext, file_path=mock_file_path)

        mock_hvf_class.assert_called_once_with(test_hash, test_ext)
        mock_stat_method.assert_called_once_with()
        self.assertEqual(mock_file_obj.size, mock_file_size)
        self.assertIs(self.storage._cached_files[test_hash], mock_file_obj)
        self.assertEqual(self.storage._cached_files_total_size, test_total_size + mock_file_size)
        self.assertIs(out, mock_file_obj)

        mock_hvf_class.reset_mock()
        mock_stat_method.reset_mock()
        mock_stat_method.side_effect = FileNotFoundError
        with self.assertLogs(util.log, logging.ERROR):
            out = self.storage._add_video_to_cache(file_hash=test_hash,
                                                   file_ext=test_ext,
                                                   file_path=mock_file_path)
        mock_hvf_class.assert_called_once_with(test_hash, test_ext)
        mock_stat_method.assert_called_once_with()
        self.assertIs(out, mock_file_obj)

    @patch.object(util, "time")
    async def test_discard_old_video_views(self, mock_time: MagicMock) -> None:
        self.mock_settings.views_retention_seconds = sec = 3.14
        mock_time.return_value = mock_now = 42.
        file1, file2 = MagicMock(), MagicMock()
        self.storage._cached_files = {1: file1, 2: file2}
        await self.storage.discard_old_video_views()
        file1.discard_views_older_than.assert_called_once_with(mock_now - sec)
        file2.discard_views_older_than.assert_called_once_with(mock_now - sec)

    def test_files_total_size_mb(self) -> None:
        expected_output = 2.1  # MB
        self.storage._cached_files_total_size = expected_output * MEGA
        self.assertEqual(expected_output, self.storage.files_total_size_mb)

    def test_files_count(self) -> None:
        expected_count = 5
        self.storage._cached_files = {n: "foo" for n in range(expected_count)}
        self.assertEqual(expected_count, self.storage.files_count)

    def test_iter_files(self) -> None:
        self.storage._cached_files = {n: f"foo{n}" for n in range(5)}
        iterator = self.storage.iter_files()
        self.assertIsInstance(iterator, Iterator)
        self.assertListEqual(list(iterator), list(self.storage._cached_files.values()))

    @patch.object(util.FileStorage, "_filter_by_orphan_status")
    @patch.object(util.FileStorage, "iter_files")
    async def test_filtered_files(self, mock_iter_files: MagicMock, mock__filter_by_orphan_status: AsyncMock) -> None:
        test_extensions, test_types = ['.mp4'], ['video', 'video_temp']
        expected_file = MagicMock(ext='.mp4')
        wrong_ext_file = MagicMock(ext='.webm')
        mock_iter_files.return_value = files = (wrong_ext_file, expected_file)
        mock__filter_by_orphan_status.return_value = {expected_file}

        # Test regular filter case without orphan status
        out = [f async for f in self.storage.filtered_files(extensions=test_extensions, types=test_types)]
        self.assertListEqual(out, [expected_file])
        mock__filter_by_orphan_status.assert_not_awaited()

        # Test with orphan status True
        out = [
            f async for f in self.storage.filtered_files(orphaned=True, extensions=test_extensions, types=test_types)
        ]
        self.assertEqual(out, [expected_file])
        mock__filter_by_orphan_status.assert_awaited_once_with(files, True)

        # Test with orphan status False
        mock__filter_by_orphan_status.reset_mock()
        mock__filter_by_orphan_status.return_value = set()
        out = [
            f async for f in self.storage.filtered_files(orphaned=False, extensions=test_extensions, types=test_types)
        ]
        self.assertEqual(out, [])
        mock__filter_by_orphan_status.assert_awaited_once_with(files, False)

        # Test exception raising
        mock__filter_by_orphan_status.reset_mock()
        with self.assertRaises(ValueError):
            _ = [f async for f in self.storage.filtered_files(extensions=['wrong'])]
        mock__filter_by_orphan_status.assert_not_awaited()

        with self.assertRaises(ValueError):
            _ = [f async for f in self.storage.filtered_files(types=['wrong'])]
        mock__filter_by_orphan_status.assert_not_awaited()

        self.storage._cached_files = {}

    @patch.object(util.LMS, 'filter_orphaned_videos')
    async def test__filter_by_orphan_status(self, mock_filter_orphaned_videos: AsyncMock) -> None:
        class MockFile(MagicMock):
            def __hash__(self) -> int:
                return int(self.hash, 16)

            def __eq__(self, other) -> bool:
                return self.hash == other.hash

        file1, file2 = MockFile(hash=FOO), MockFile(hash=BAR)
        self.storage._cached_files = {FOO: file1, BAR: file2}
        mock_filter_orphaned_videos.return_value = [BAR, "causes warning"]

        orphaned = True
        expected_output = {file2}
        output = await self.storage._filter_by_orphan_status((file1, file2), orphaned=orphaned)
        self.assertEqual(expected_output, output)
        mock_filter_orphaned_videos.assert_awaited_once_with(file1, file2, client=self.storage.http_client)

        mock_filter_orphaned_videos.reset_mock()
        mock_filter_orphaned_videos.return_value = [BAR]
        orphaned = False
        expected_output = {file1}
        output = await self.storage._filter_by_orphan_status((file1, file2), orphaned=orphaned)
        self.assertEqual(expected_output, output)
        mock_filter_orphaned_videos.assert_awaited_once_with(file1, file2, client=self.storage.http_client)

    def test_get_file(self) -> None:
        mock_file = MagicMock(ext=BAR)
        self.storage._cached_files = {FOO: mock_file}
        output = self.storage.get_file(FOO, BAR)
        self.assertEqual(output, mock_file)
        with self.assertRaises(FileNotFoundError):
            self.storage.get_file(BAZ, BAR)

    @patch.object(util, "rel_path")
    def test_get_path(self, mock_rel_path: MagicMock) -> None:
        self.storage.storage_dir = Path(self.path, "storage")
        self.storage.temp_dir = Path(self.path, "temp")

        mock_rel_path.return_value = mock_path = Path(BAZ, BAR)

        temp = True
        expected_output = Path(self.storage.temp_dir, FOO)
        output = self.storage.get_path(FOO, temp=temp)
        self.assertEqual(expected_output, output)
        mock_rel_path.assert_not_called()

        temp = False
        expected_output = Path(self.storage.storage_dir, mock_path)
        output = self.storage.get_path(FOO, temp=temp)
        self.assertEqual(expected_output, output)
        mock_rel_path.assert_called_once_with(FOO)

    @patch.object(util.FileStorage, "get_path")
    @patch.object(util.FileStorage, "get_file")
    def test_get_perm_video_path(
        self,
        mock_get_file: MagicMock,
        mock_get_path: MagicMock,
    ) -> None:
        mock_get_file.return_value = mock_file_obj = object()
        mock_get_path.return_value = expected_output = Path(BAZ, BAR)

        output = self.storage.get_perm_video_path(FOO, BAR)
        self.assertEqual(expected_output, output)
        mock_get_file.assert_called_once_with(FOO, BAR)
        mock_get_path.assert_called_once_with(str(mock_file_obj), temp=False)

    @patch.object(util.FileStorage, "get_path")
    def test_get_temp_video_path(self, mock_get_path: MagicMock) -> None:
        mock_get_path.return_value = expected_output = Path(BAZ, BAR)

        output = self.storage.get_temp_video_path(FOO, BAR)
        self.assertEqual(expected_output, output)
        mock_get_path.assert_called_once_with(FOO + BAR, temp=True)

    @patch.object(util.FileStorage, "get_path")
    @patch.object(util.FileStorage, "get_file")
    def test_get_perm_thumbnail_path(
        self,
        mock_get_file: MagicMock,
        mock_get_path: MagicMock,
    ) -> None:
        mock_get_path.return_value = expected_output = Path(BAZ, BAR)

        num = 42
        output = self.storage.get_perm_thumbnail_path(FOO, BAR, num=num)
        self.assertEqual(expected_output, output)
        mock_get_file.assert_called_once_with(FOO, BAR)
        mock_get_path.assert_called_once_with(f"{FOO}_{num}{JPG_EXT}", temp=False)

    @patch.object(util.FileStorage, "get_path")
    def test_get_temp_thumbnail_path(self, mock_get_path: MagicMock) -> None:
        mock_get_path.return_value = expected_output = Path(BAZ, BAR)

        num = 42
        output = self.storage.get_temp_thumbnail_path(FOO, num=num)
        self.assertEqual(expected_output, output)
        mock_get_path.assert_called_once_with(f"{FOO}_{num}{JPG_EXT}", temp=True)

    @patch.object(util, "run_in_default_executor")
    @patch.object(util.FileStorage, "get_path")
    async def test_store_file_permanently(
        self,
        mock_get_path: MagicMock,
        mock_run_in_default_executor: AsyncMock,
    ) -> None:
        expected_source = Path(FOO, BAR)
        expected_destination = Path(BAZ, BAR)
        mock_get_path.side_effect = (expected_source, expected_destination)

        output = await self.storage.store_file_permanently(BAZ)
        self.assertEqual(expected_destination, output)
        self.assertListEqual(
            [call(BAZ, temp=True), call(BAZ, temp=False)],
            mock_get_path.call_args_list,
        )
        mock_run_in_default_executor.assert_awaited_once_with(
            util.move_file,
            expected_source,
            expected_destination,
            0o755,
        )

    @patch.object(util.FileStorage, "_add_video_to_cache")
    @patch.object(util.FileStorage, "store_file_permanently")
    async def test_store_permanently(
        self,
        mock_store_file_permanently: AsyncMock,
        mock__add_video_to_cache: MagicMock,
    ) -> None:
        mock_store_file_permanently.return_value = mock_path = Path(BAZ)

        thumb_count = 4
        self.assertIsNone(await self.storage.store_permanently(
            FOO,
            BAR,
            thumbnail_count=thumb_count,
        ))
        vid_storage_calls = [call(FOO + BAR)]
        thumb_storage_calls = [
            call(f"{FOO}_{num}{JPG_EXT}")
            for num in range(thumb_count)
        ]
        self.assertListEqual(
            vid_storage_calls + thumb_storage_calls,
            mock_store_file_permanently.await_args_list,
        )
        mock__add_video_to_cache.assert_called_once_with(
            FOO,
            BAR,
            mock_path,
        )

    @patch.object(util, "run_in_default_executor")
    @patch.object(util.FileStorage, "get_perm_video_path")
    async def test_remove_video(
        self,
        mock_get_perm_video_path: MagicMock,
        mock_run_in_default_executor: AsyncMock,
    ) -> None:
        mock_get_perm_video_path.return_value = mock_path = MagicMock()

        self.storage._cached_files_total_size = test_total_size = 42070
        self.storage._cached_files[FOO] = MagicMock()
        mock_file = MagicMock(
            hash=FOO,
            ext=BAR,
            size=test_total_size - 1,
        )
        self.assertIsNone(await self.storage.remove_video(mock_file))
        self.assertDictEqual({}, self.storage._cached_files)
        self.assertEqual(1, self.storage._cached_files_total_size)
        mock_get_perm_video_path.assert_called_once_with(FOO, BAR)
        mock_run_in_default_executor.assert_awaited_once_with(
            mock_path.unlink
        )
        self.mock_dist_controller.remove_from_nodes.assert_called_once_with(
            mock_file
        )

    @patch.object(util, "run_in_default_executor")
    @patch.object(util.FileStorage, "get_path")
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

        self.assertIsNone(await self.storage.remove_thumbnails(
            FOO,
            count=test_count,
        ))
        self.assertListEqual(
            [
                call(f"{FOO}_{num}{JPG_EXT}", temp=False)
                for num in range(test_count)
            ],
            mock_get_path.call_args_list,
        )
        self.assertListEqual(
            [call(mock_path)] * test_count,
            self.mock_thumb_cache_cls().__delitem__.call_args_list,
        )
        self.assertListEqual(
            [call(mock_path.unlink) for _ in range(test_count)],
            mock_run_in_default_executor.await_args_list,
        )

    @patch.object(util.FileStorage, "remove_video")
    @patch.object(util.FileStorage, "remove_thumbnails")
    @patch.object(util.LMS, "filter_orphaned_videos")
    async def test_remove_files(
        self,
        mock_filter_orphaned_videos: AsyncMock,
        mock_remove_thumbnails: AsyncMock,
        mock_remove_video: AsyncMock,
    ) -> None:
        test_hashes = [FOO, BAR, BAZ]
        test_origin = "abcde"
        mock_file_foo, mock_file_bar = object(), object()
        self.storage._cached_files = {FOO: mock_file_foo, BAR: mock_file_bar}
        mock_filter_orphaned_videos.return_value = [FOO]

        output = await self.storage.remove_files(*test_hashes, origin=test_origin)
        self.assertSetEqual({BAR, BAZ}, output)
        mock_filter_orphaned_videos.assert_awaited_once_with(
            mock_file_foo, mock_file_bar, client=self.storage.http_client, origin=test_origin
        )
        mock_remove_thumbnails.assert_awaited_once_with(FOO)
        mock_remove_video.assert_awaited_once_with(mock_file_foo)

        mock_filter_orphaned_videos.reset_mock()
        mock_remove_thumbnails.reset_mock()
        mock_remove_video.reset_mock()

        mock_filter_orphaned_videos.side_effect = util.LMSInterfaceError
        output = await self.storage.remove_files(*test_hashes, origin=test_origin)
        self.assertSetEqual(set(), output)
        mock_filter_orphaned_videos.assert_awaited_once_with(
            mock_file_foo, mock_file_bar, client=self.storage.http_client, origin=test_origin
        )
        mock_remove_thumbnails.assert_not_called()
        mock_remove_video.assert_not_called()

    def test__remove_old_temp_files(self) -> None:
        self.mock_settings.max_temp_storage_hours = 1 / 3600  # 1 sec

        self.storage.temp_dir = Path(self.path, "temp")
        file1 = Path(self.storage.temp_dir, 'foo')
        file2 = Path(self.storage.temp_dir, 'bar')
        dir1 = Path(self.storage.temp_dir, 'dir1')
        dir2 = Path(self.storage.temp_dir, 'dir2')
        self.storage.temp_dir.mkdir()
        file1.touch()
        dir1.mkdir()
        time.sleep(2)
        file2.touch()
        dir2.mkdir()

        try:
            output = self.storage._remove_old_temp_files()
        finally:
            dir2.rmdir()
            file2.unlink(missing_ok=True)
            dir1.rmdir()
            file1.unlink(missing_ok=True)
            self.storage.temp_dir.rmdir()
        self.assertEqual(1, output)

    @patch.object(util, "run_in_default_executor")
    async def test_remove_old_temp_files(self, mock_run: AsyncMock) -> None:
        mock_run.return_value = count = 42
        with self.assertLogs(util.log, logging.INFO) as log_ctx:
            await self.storage.remove_old_temp_files()
            self.assertEqual(
                f"Cleaned up temp directory: Removed {count} old file(s).",
                log_ctx.records[0].msg,
            )
        mock_run.assert_awaited_once_with(self.storage._remove_old_temp_files)

    @patch.object(util.FileStorage, 'files_total_size_mb', new_callable=PropertyMock)
    @patch.object(util.FileStorage, 'files_count', new_callable=PropertyMock)
    @patch.object(util, 'get_free_disk_space')
    @patch.object(util.NetworkInterfaces, 'get_instance')
    async def test_get_status(
        self,
        mock_ni_get_instance: MagicMock,
        mock_get_free_disk_space: MagicMock,
        mock_files_count: MagicMock,
        mock_files_total_size_mb: MagicMock,
    ) -> None:
        self.mock_settings.tx_max_rate_mbit = tx_max = 12345
        mock_iter_nodes = self.mock_dist_controller_cls.return_value.iter_nodes
        mock_iter_nodes.return_value = [
            MagicMock(base_url="foo"),
            MagicMock(base_url="bar"),
        ]
        mock_update_node_status = mock_ni_get_instance.return_value.update_node_status
        mock_get_free_disk_space.return_value = mock_free_disk_space = 999
        mock_files_count.return_value = mock_files_count = 111
        mock_files_total_size_mb.return_value = mock_files_total_size = 555
        output = await self.storage.get_status()
        self.assertIsInstance(output, util.StorageStatus)
        self.assertEqual(mock_files_total_size, output.files_total_size)
        self.assertEqual(mock_files_count, output.files_count)
        self.assertEqual(mock_free_disk_space, output.free_space)
        mock_get_free_disk_space.assert_awaited_once_with(str(self.mock_settings.files_path))
        self.assertEqual(tx_max, output.tx_max_rate)
        mock_update_node_status.assert_called_once_with(output, logger=util.log)
        self.assertEqual(["foo", "bar"], output.distributor_nodes)
        self.assertEqual(self.storage.num_current_uploads, output.num_current_uploads)


class FunctionsTestCase(IsolatedAsyncioTestCase):
    def test_is_allowed_file_ending(self) -> None:
        test_name = None
        self.assertTrue(util.is_allowed_file_ending(test_name))
        test_name = 'foobar'
        self.assertFalse(util.is_allowed_file_ending(test_name))
        test_name = 'something.MP4'
        self.assertTrue(util.is_allowed_file_ending(test_name))

    @patch.object(util, "_video_delete_task", new_callable=MagicMock)  # No need for async mock
    @patch.object(util.TaskManager, "fire_and_forget")
    def test_schedule_video_delete(
        self,
        mock_fire_and_forget: MagicMock,
        mock__video_delete_task: MagicMock,
    ) -> None:
        self.assertIsNone(util.schedule_video_delete(
            file_hash=FOO,
            file_ext=BAR,
            origin=BAZ,
        ))
        mock__video_delete_task.assert_called_once_with(FOO, BAR, BAZ)
        mock_fire_and_forget.assert_called_once_with(mock__video_delete_task.return_value)

    @patch.object(util.FileStorage, "get_instance")
    async def test__video_delete_task(self, mock_get_instance: MagicMock) -> None:
        mock_file = MagicMock(hash=FOO)
        mock_get_file, mock_remove_files = MagicMock(return_value=mock_file), AsyncMock()
        mock_get_instance.return_value.get_file = mock_get_file
        mock_get_instance.return_value.remove_files = mock_remove_files

        self.assertIsNone(await util._video_delete_task(
            file_hash=FOO,
            file_ext=BAR,
            origin=BAZ,
        ))
        mock_get_file.assert_called_once_with(FOO, BAR)
        mock_remove_files.assert_awaited_once_with(FOO, origin=BAZ)

        mock_get_file.reset_mock()
        mock_remove_files.reset_mock()
        mock_get_file.side_effect = FileNotFoundError
        self.assertIsNone(await util._video_delete_task(FOO, BAR, BAZ))
        mock_get_file.assert_called_once_with(FOO, BAR)
        mock_remove_files.assert_not_awaited()
