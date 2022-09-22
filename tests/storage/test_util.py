import logging
import shutil
import time
from collections.abc import Iterator
from unittest.mock import patch, MagicMock, call, Mock
from pathlib import Path
from typing import TypeVar, Type

from tests.base import BaseTestCase, async_test, AsyncMock
from videbo.storage import util


main_log = logging.getLogger('videbo')

M = TypeVar('M', bound=Mock)

TESTED_MODULE_PATH = 'videbo.storage.util'
STORAGE_SETTINGS_PATH = TESTED_MODULE_PATH + '.settings'

FOO, BAR, BAZ = 'foo', 'bar', 'baz'


class HashedVideoFileTestCase(BaseTestCase):

    def test___init__(self) -> None:
        test_hash, test_ext = 'test', '.ext'
        obj = util.HashedVideoFile(file_hash=test_hash, file_ext=test_ext)
        self.assertEqual(obj.hash, test_hash)
        self.assertEqual(obj.file_ext, test_ext)

        # Expecting error, when extension doesn't start with a dot:
        with self.assertRaises(util.HashedFileInvalidExtensionError):
            util.HashedVideoFile(file_hash=test_hash, file_ext='ext')

    def test___str__(self) -> None:
        test_hash, test_ext = 'test', '.ext'
        obj = util.HashedVideoFile(file_hash=test_hash, file_ext=test_ext)
        self.assertEqual(str(obj), test_hash + test_ext)

    def test___hash__(self) -> None:
        test_hash, test_ext = 12345, ".foo"
        obj = util.HashedVideoFile(file_hash=f"{test_hash:x}", file_ext=test_ext)
        self.assertEqual(hash(obj), test_hash)

    def test___eq__(self) -> None:
        test_hash = "foobarbaz"
        file1 = util.HashedVideoFile(file_hash=test_hash, file_ext=".foo")
        file2 = util.HashedVideoFile(file_hash=test_hash, file_ext=".bar")
        file3 = util.HashedVideoFile(file_hash="somethingelse", file_ext=".bar")
        self.assertEqual(file1, file2)
        self.assertNotEqual(file3, file2)
        file4 = util.StoredHashedVideoFile("foo", ".bar")
        same = file1 == file4


class StoredHashedVideoFileTestCase(BaseTestCase):

    @patch(TESTED_MODULE_PATH + '.FileNodes')
    @patch(TESTED_MODULE_PATH + '.HashedVideoFile.__init__')
    def test_init(self, mock_superclass_init: MagicMock, mock_file_nodes: MagicMock) -> None:
        mock_nodes_obj = 'mock'
        mock_file_nodes.return_value = mock_nodes_obj

        test_hash, test_ext = 'test', '.ext'
        obj = util.StoredHashedVideoFile(file_hash=test_hash, file_ext=test_ext)

        mock_superclass_init.assert_called_once_with(test_hash, test_ext)
        self.assertEqual(obj.file_size, -1)
        self.assertEqual(obj.views, 0)
        self.assertEqual(obj.nodes, mock_nodes_obj)

    @patch(TESTED_MODULE_PATH + '.FileNodes')
    @patch(TESTED_MODULE_PATH + '.HashedVideoFile.__init__')
    def test_lt(self, *_: MagicMock) -> None:
        test_hash, test_ext = 'test', '.ext'
        obj = util.StoredHashedVideoFile(file_hash=test_hash, file_ext=test_ext)
        obj.views = 10
        mock_other = MagicMock(views=20)
        self.assertLess(obj, mock_other)


class FileStorageTestCase(BaseTestCase):
    log_lvl: int

    @classmethod
    def setUpClass(cls) -> None:
        cls.log_lvl = main_log.level
        main_log.setLevel(logging.CRITICAL)

    @classmethod
    def tearDownClass(cls) -> None:
        main_log.setLevel(cls.log_lvl)

    def setUp(self) -> None:
        super().setUp()
        self.path = Path('/tmp/videbo_storage_test')

        # All kinds of mocking:
        self.settings_patcher = patch(STORAGE_SETTINGS_PATH)
        self.mock_settings = self.settings_patcher.start()
        self.mock_settings.files_path = self.path
        self.mock_settings.thumb_cache_max_mb = 0

        self.dist_controller_patcher = patch(TESTED_MODULE_PATH + '.DistributionController')
        self.mock_dist_controller_cls = self.dist_controller_patcher.start()

        self.create_dir_patcher = patch(TESTED_MODULE_PATH + '.create_dir_if_not_exists')
        self.mock_create_dir = self.create_dir_patcher.start()

        self.gc_cron_patcher = patch.object(util.FileStorage, '_garbage_collect_cron', new=MagicMock())
        self.mock_gc_cron = self.gc_cron_patcher.start()

        self.mock_gc_task = 'mock'
        self.asyncio_create_task_patcher = patch(TESTED_MODULE_PATH + '.asyncio.create_task',
                                                 return_value=self.mock_gc_task)
        self.mock_asyncio_create_task = self.asyncio_create_task_patcher.start()

        self.task_mgr_patcher = patch(TESTED_MODULE_PATH + '.TaskManager')
        self.mock_task_mgr = self.task_mgr_patcher.start()

        # Initialize storage instance:
        self.path.mkdir()
        self.storage = util.FileStorage(self.path)

    def tearDown(self) -> None:
        super().tearDown()
        shutil.rmtree(self.path)
        self.task_mgr_patcher.stop()
        self.asyncio_create_task_patcher.stop()
        self.gc_cron_patcher.stop()
        self.create_dir_patcher.stop()
        self.dist_controller_patcher.stop()
        self.settings_patcher.stop()

    def test_init(self) -> None:
        # self.storage is initialized in setUp method
        self.assertEqual(self.storage.path, self.path)
        self.assertEqual(self.storage.storage_dir, Path(self.storage.path, 'storage'))
        self.assertEqual(self.storage.temp_dir, Path(self.storage.path, 'temp'))
        self.assertEqual(self.storage.temp_out_dir, Path(self.storage.temp_dir, 'out'))
        self.assertEqual(self.storage._cached_files, {})
        self.assertEqual(self.storage._cached_files_total_size, 0)
        self.assertEqual(self.storage.distribution_controller, self.mock_dist_controller_cls())

        create_temp_call = call(self.storage.temp_dir, 0o755)
        create_temp_out_call = call(self.storage.temp_out_dir, 0o777, explicit_chmod=True)
        self.assertListEqual(self.mock_create_dir.call_args_list, [create_temp_call, create_temp_out_call])

        self.mock_asyncio_create_task.assert_called_once_with(self.mock_gc_cron())
        self.mock_task_mgr.fire_and_forget_task.assert_called_once_with(self.mock_gc_task)

        with self.assertRaises(NotADirectoryError), self.assertLogs():
            util.FileStorage(Path('/doesnotexist'))

    def test_get_instance(self) -> None:
        url1, url2 = 'foo', 'bar'
        self.mock_settings.static_dist_node_base_urls = [url1, url2]
        obj = util.FileStorage.get_instance()
        self.assertIsInstance(obj.distribution_controller, MagicMock)
        obj.distribution_controller.add_new_dist_node.assert_has_calls([call(url1), call(url2)])
        obj.distribution_controller.start_periodic_reset_task.assert_called_once_with()

        # Check that another call to the tested method will not try to create a new object,
        # but will return the previously created one:
        obj.distribution_controller.add_new_dist_node.reset_mock()
        obj.distribution_controller.start_periodic_reset_task.reset_mock()
        with patch.object(util.FileStorage, '__init__', return_value=None) as mock_init:
            check_obj = util.FileStorage.get_instance()
            self.assertIs(check_obj, obj)
            self.assertIsInstance(check_obj.distribution_controller, MagicMock)  # to fool type checking
            mock_init.assert_not_called()
            check_obj.distribution_controller.add_new_dist_node.assert_not_called()
            check_obj.distribution_controller.start_periodic_reset_task.assert_not_called()

    @patch.object(util.FileStorage, '_add_video_to_cache', return_value='foo')
    def test_load_file_list(self, mock__add_video_to_cache: MagicMock) -> None:

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

    @patch.object(util, 'StoredHashedVideoFile')
    def test__add_video_to_cache(self, mock_hvf_class: MagicMock) -> None:
        mock_file_obj = MagicMock()
        mock_hvf_class.return_value = mock_file_obj
        mock_file_size = 10
        mock_stat_method = MagicMock(return_value=MagicMock(st_size=mock_file_size))
        mock_file_path = MagicMock()
        mock_file_path.stat = mock_stat_method
        self.mock_dist_controller_cls.add_video = MagicMock()

        test_total_size, test_hash, test_ext = 10, 'abc', '.ext'
        self.storage._cached_files_total_size = test_total_size

        out = self.storage._add_video_to_cache(file_hash=test_hash, file_ext=test_ext, file_path=mock_file_path)

        mock_hvf_class.assert_called_once_with(test_hash, test_ext)
        mock_stat_method.assert_called_once_with()
        self.assertEqual(mock_file_obj.file_size, mock_file_size)
        self.assertIs(self.storage._cached_files[test_hash], mock_file_obj)
        self.assertEqual(self.storage._cached_files_total_size, test_total_size + mock_file_size)
        self.mock_dist_controller_cls().add_video.assert_called_once_with(mock_file_obj)
        self.assertIs(out, mock_file_obj)

        mock_hvf_class.reset_mock()
        mock_stat_method.reset_mock()
        self.mock_dist_controller_cls().add_video.reset_mock()
        mock_stat_method.side_effect = FileNotFoundError
        with self.assertLogs(util.log, logging.ERROR):
            out = self.storage._add_video_to_cache(file_hash=test_hash,
                                                   file_ext=test_ext,
                                                   file_path=mock_file_path)
        mock_hvf_class.assert_called_once_with(test_hash, test_ext)
        mock_stat_method.assert_called_once_with()
        self.mock_dist_controller_cls().add_video.assert_not_called()
        self.assertIs(out, mock_file_obj)

    def test_get_files_total_size_mb(self) -> None:
        test_total_size = 2 * 1024 * 1024 + 0.1
        expected_output = 2  # MB
        self.storage._cached_files_total_size = test_total_size
        out = self.storage.get_files_total_size_mb()
        self.assertEqual(out, expected_output)

    def test_get_files_count(self) -> None:
        expected_count = 5
        self.storage._cached_files = {n: 'foo' for n in range(expected_count)}
        out = self.storage.get_files_count()
        self.assertEqual(out, expected_count)

    def test_iter_files(self) -> None:
        self.storage._cached_files = {n: f'foo{n}' for n in range(5)}
        iterator = self.storage.iter_files()
        self.assertIsInstance(iterator, Iterator)
        self.assertListEqual(list(iterator), list(self.storage._cached_files.values()))

    @async_test
    @patch.object(util.FileStorage, '_filter_by_orphan_status', new_callable=AsyncMock)
    async def test_filtered_files(self, mock__filter_by_orphan_status: AsyncMock) -> None:
        test_extensions, test_types = ['.mp4'], ['video', 'video_temp']
        expected_file = MagicMock(file_ext='.mp4')
        wrong_ext_file = MagicMock(file_ext='.webm')
        files = (wrong_ext_file, expected_file)
        self.storage._cached_files = MagicMock(values=MagicMock(return_value=files))
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

        file1, file2 = MockFile(hash="foo"), MockFile(hash="bar")
        self.storage._cached_files = {"foo": file1, "bar": file2}
        mock_filter_orphaned_videos.return_value = [file2, MagicMock(hash="causes warning")]

        orphaned = True
        expected_output = {file2}
        output = await self.storage._filter_by_orphan_status((file1, file2), orphaned=orphaned)
        self.assertEqual(expected_output, output)
        mock_filter_orphaned_videos.assert_awaited_once_with(file1, file2)

        mock_filter_orphaned_videos.reset_mock()
        mock_filter_orphaned_videos.return_value = [file2]
        orphaned = False
        expected_output = {file1}
        output = await self.storage._filter_by_orphan_status((file1, file2), orphaned=orphaned)
        self.assertEqual(expected_output, output)
        mock_filter_orphaned_videos.assert_awaited_once_with(file1, file2)

    @async_test
    async def test_get_file(self) -> None:
        test_hash, test_ext = 'foo', 'bar'
        mock_file = MagicMock(file_ext=test_ext)
        self.storage._cached_files = {test_hash: mock_file}
        output = await self.storage.get_file(test_hash, test_ext)
        self.assertEqual(output, mock_file)
        with self.assertRaises(FileNotFoundError):
            await self.storage.get_file('baz', 'ext')

    @async_test
    @patch.object(util, 'asyncio')
    @patch.object(util, 'VideoConfig')
    @patch.object(util, 'Video')
    @patch.object(util.FileStorage, 'get_thumb_path_in_temp')
    async def test_generate_thumbs(self, mock_get_thumb_path_in_temp: MagicMock, mock_video_cls: MagicMock, mock_video_config_cls: MagicMock,
                                   mock_asyncio: MagicMock) -> None:
        mock_asyncio.gather = AsyncMock()

        test_video_length = 3
        test_thumb_height, test_thumb_count = 2, 1
        test_video_check_user = 'foo'
        mock_thumb_path, mock_init_vid_config = 'bar', 'baz'
        mock_file_hash, mock_video_file = 'abc', 'xyz'

        mock_file = MagicMock(hash=mock_file_hash)
        mock_vid_info = MagicMock(get_length=MagicMock(return_value=test_video_length), video_file=mock_video_file)
        self.mock_settings.thumb_height = test_thumb_height
        self.mock_settings.thumb_suggestion_count = test_thumb_count
        self.mock_settings.check_user = test_video_check_user
        mock_get_thumb_path_in_temp.return_value = mock_thumb_path
        mock_video_config_cls.return_value = mock_init_vid_config
        mock_save_thumbnail = MagicMock()
        mock_video_cls.return_value.save_thumbnail = mock_save_thumbnail

        output = await self.storage.generate_thumbs(mock_file, mock_vid_info)
        self.assertEqual(output, test_thumb_count)
        thumb_number = 0
        mock_get_thumb_path_in_temp.assert_called_once_with(mock_file, thumb_number)
        mock_video_cls.assert_called_once_with(video_config=mock_init_vid_config)
        mock_video_config_cls.assert_called_once_with()
        test_offset = int(test_video_length / test_thumb_count * (thumb_number + 0.5))
        test_temp_out_file = Path(self.storage.temp_out_dir, mock_file_hash + "_" + str(thumb_number) + util.JPG_EXT)
        mock_save_thumbnail.assert_called_once_with(mock_video_file, mock_thumb_path, test_offset, test_thumb_height,
                                                    temp_output_file=test_temp_out_file)

        mock_get_thumb_path_in_temp.reset_mock()
        mock_video_cls.reset_mock()
        mock_video_config_cls.reset_mock()
        mock_save_thumbnail.reset_mock()
        self.mock_settings.check_user = None
        output = await self.storage.generate_thumbs(mock_file, mock_vid_info)
        self.assertEqual(output, test_thumb_count)
        mock_get_thumb_path_in_temp.assert_called_once_with(mock_file, thumb_number)
        mock_video_cls.assert_called_once_with(video_config=mock_init_vid_config)
        mock_video_config_cls.assert_called_once_with()
        mock_save_thumbnail.assert_called_once_with(mock_video_file, mock_thumb_path, test_offset, test_thumb_height,
                                                    temp_output_file=None)

    @patch.object(util, 'hashlib')
    def test_get_hash_gen(self, mock_hashlib: MagicMock) -> None:
        mock_hashlib.sha256.return_value = 'test'
        self.assertEqual(self.storage.get_hash_gen(), 'test')
        mock_hashlib.sha256.assert_called_once_with()

    @patch.object(util, 'TempFile')
    @patch.object(util, 'tempfile')
    @patch.object(util, 'os')
    def test_create_temp_file(self, mock_os: MagicMock, mock_tempfile_module: MagicMock, mock_tempfile_cls: MagicMock) -> None:
        mock_tempfile_obj = 'test'
        mock_tempfile_cls.return_value = mock_tempfile_obj
        mock_fd, mock_path = 'foo', 'bar'
        mock_tempfile_module.mkstemp = MagicMock(return_value=(mock_fd, mock_path))
        mock_io = 'baz'
        mock_os.fdopen = MagicMock(return_value=mock_io)

        output = self.storage.create_temp_file()
        self.assertEqual(output, mock_tempfile_obj)
        mock_tempfile_module.mkstemp.assert_called_once_with(prefix='upload_', dir=self.storage.temp_dir)
        mock_os.chmod.assert_called_once_with(mock_path, 0o644)
        mock_os.fdopen.assert_called_once_with(mock_fd, mode='wb')
        mock_tempfile_cls.assert_called_once_with(mock_io, Path(mock_path), self.storage)

    @patch.object(util, 'rel_path')
    def test_get_path(self, mock_rel_path: MagicMock) -> None:
        mock_path = Path('foo/bar')
        mock_rel_path.return_value = mock_path
        mock_file = MagicMock()
        expected_output = Path(self.storage.storage_dir, mock_path)
        output = self.storage.get_path(mock_file)
        self.assertEqual(output, expected_output)
        mock_rel_path.assert_called_once_with(str(mock_file))

    @patch.object(util.TempFile, 'get_path')
    def test_get_path_in_temp(self, mock_temp_file_get_path: MagicMock) -> None:
        mock_file = MagicMock()
        output = self.storage.get_path_in_temp(mock_file)
        self.assertEqual(output, mock_temp_file_get_path.return_value)
        mock_temp_file_get_path.assert_called_once_with(self.storage.temp_dir, mock_file)

    def test_get_thumb_path(self) -> None:
        test_hash, test_number = 'foo', 0
        mock_file = MagicMock(hash=test_hash)
        expected_name = test_hash + "_" + str(test_number) + util.JPG_EXT
        expected_output = Path(self.storage.storage_dir, test_hash[0:2], expected_name)
        output = self.storage.get_thumb_path(mock_file, test_number)
        self.assertEqual(output, expected_output)

    @patch.object(util.TempFile, 'get_thumb_path')
    def test_get_thumb_path_in_temp(self, mock_temp_file_get_thumb_path: MagicMock) -> None:
        mock_file, test_number = MagicMock(), 0
        output = self.storage.get_thumb_path_in_temp(mock_file, test_number)
        self.assertEqual(output, mock_temp_file_get_thumb_path.return_value)
        mock_temp_file_get_thumb_path.assert_called_once_with(self.storage.temp_dir, mock_file, test_number)

    def test__delete_file(self) -> None:
        mock_is_file = MagicMock(return_value=True)
        mock_unlink = MagicMock()
        mock_file_path = MagicMock(is_file=mock_is_file, unlink=mock_unlink)
        output = self.storage._delete_file(mock_file_path)
        self.assertEqual(output, True)
        mock_is_file.assert_called_once_with()
        mock_unlink.assert_called_once_with()

        mock_is_file.reset_mock()
        mock_unlink.reset_mock()
        mock_is_file.return_value = False
        output = self.storage._delete_file(mock_file_path)
        self.assertEqual(output, False)
        mock_is_file.assert_called_once_with()
        mock_unlink.assert_not_called()

    def test__move_file(self) -> None:
        # Mocking first argument
        mock_path = MagicMock()
        mock_path.is_file = MagicMock(return_value=True)
        mock_path.unlink = MagicMock()
        mock_path.rename = MagicMock()

        # Mocking second argument
        mock_new_parent = MagicMock()
        mock_new_parent.is_dir = MagicMock(return_value=True)
        mock_new_parent.mkdir = MagicMock()
        mock_new_file_path = MagicMock()
        mock_new_file_path.parent = mock_new_parent
        mock_new_file_path.is_file = MagicMock(return_value=True)
        mock_new_file_path.chmod = MagicMock()

        # Case 1
        self.storage._move_file(mock_path, mock_new_file_path)
        mock_path.is_file.assert_called_once_with()
        mock_new_parent.is_dir.assert_called_once_with()
        mock_new_parent.mkdir.assert_not_called()
        mock_new_file_path.is_file.assert_called_once_with()
        mock_path.unlink.assert_called_once_with()
        mock_path.rename.assert_not_called()
        mock_new_file_path.chmod.assert_not_called()

        mock_path.is_file.reset_mock()
        mock_new_parent.is_dir.reset_mock()
        mock_new_file_path.is_file.reset_mock()
        mock_path.unlink.reset_mock()

        # Case 2
        mock_path.is_file = MagicMock(return_value=False)
        with self.assertRaises(FileNotFoundError):
            self.storage._move_file(mock_path, mock_new_file_path)
        mock_path.is_file.assert_called_once_with()

        mock_path.is_file.reset_mock()
        mock_path.is_file = MagicMock(return_value=True)

        # Case 3
        mock_new_parent.is_dir = MagicMock(return_value=False)
        self.storage._move_file(mock_path, mock_new_file_path)
        mock_path.is_file.assert_called_once_with()
        mock_new_parent.is_dir.assert_called_once_with()
        mock_new_parent.mkdir.assert_called_once_with(mode=0o755, parents=True)  # <-- difference
        mock_new_file_path.is_file.assert_called_once_with()
        mock_path.unlink.assert_called_once_with()
        mock_path.rename.assert_not_called()
        mock_new_file_path.chmod.assert_not_called()

        mock_path.is_file.reset_mock()
        mock_new_parent.is_dir.reset_mock()
        mock_new_parent.mkdir.reset_mock()
        mock_new_file_path.is_file.reset_mock()
        mock_path.unlink.reset_mock()
        mock_new_parent.is_dir = MagicMock(return_value=True)

        # Case 4
        mock_new_file_path.is_file = MagicMock(return_value=False)
        self.storage._move_file(mock_path, mock_new_file_path)
        mock_path.is_file.assert_called_once_with()
        mock_new_parent.is_dir.assert_called_once_with()
        mock_new_parent.mkdir.assert_not_called()
        mock_new_file_path.is_file.assert_called_once_with()
        mock_path.unlink.assert_not_called()
        mock_path.rename.assert_called_once_with(mock_new_file_path)
        mock_new_file_path.chmod.assert_called_once_with(0o644)

    @async_test
    @patch.object(util, 'asyncio')
    @patch.object(util.FileStorage, '_add_video_to_cache')
    @patch.object(util.FileStorage, 'get_path')
    @patch.object(util.FileStorage, 'get_path_in_temp')
    async def test_add_file_from_temp(self, mock_get_path_in_temp: MagicMock, mock_get_path: MagicMock, mock__add_video_to_cache: MagicMock, mock_aio: MagicMock) -> None:
        mock_temp_path = 'abc'
        mock_new_file_path = 'xyz'
        mock_get_path_in_temp.return_value = mock_temp_path
        mock_get_path.return_value = mock_new_file_path

        mock_run = mocked_loop_runner(mock_aio)

        test_hash, test_ext = 'foo', 'bar'
        mock_file = MagicMock(hash=test_hash, file_ext=test_ext)

        output = await self.storage.add_file_from_temp(mock_file)
        self.assertIsNone(output)
        mock_get_path_in_temp.assert_called_once_with(mock_file)
        mock_get_path.assert_called_once_with(mock_file)
        mock_run.assert_awaited_once_with(None, self.storage._move_file, mock_temp_path, mock_new_file_path)
        mock__add_video_to_cache.assert_called_once_with(test_hash, test_ext, mock_new_file_path)

    @async_test
    @patch.object(util, 'asyncio')
    @patch.object(util.FileStorage, 'get_thumb_path')
    @patch.object(util.FileStorage, 'get_thumb_path_in_temp')
    async def test_add_thumbs_from_temp(self, mock_get_thumb_path_in_temp: MagicMock, mock_get_thumb_path: MagicMock, mock_aio: MagicMock) -> None:
        mock_old_thumb_path = 'abc'
        mock_new_thumb_file = 'xyz'
        mock_get_thumb_path_in_temp.return_value = mock_old_thumb_path
        mock_get_thumb_path.return_value = mock_new_thumb_file

        # runner not AsyncMock on purpose because asyncio.gather is used for awaiting all tasks
        mock_run = mocked_loop_runner(mock_aio, MagicMock)
        mock_aio.gather = AsyncMock()

        mock_file = MagicMock(hash='foo')
        test_count = 2

        output = await self.storage.add_thumbs_from_temp(mock_file, test_count)
        self.assertIsNone(output)
        expected_calls_list = [call(mock_file, i) for i in range(test_count)]
        self.assertListEqual(mock_get_thumb_path_in_temp.call_args_list, expected_calls_list)
        self.assertListEqual(mock_get_thumb_path.call_args_list, expected_calls_list)
        expected_calls_list = [
            call(None, self.storage._move_file, mock_old_thumb_path, mock_new_thumb_file) for _ in range(test_count)
        ]
        self.assertListEqual(mock_run.call_args_list, expected_calls_list)
        mock_aio.gather.assert_awaited_once_with(*(mock_run.return_value for _ in range(test_count)))

    @async_test
    @patch.object(util, 'asyncio')
    @patch.object(util.FileStorage, 'get_path')
    async def test_remove(self, mock_get_path: MagicMock, mock_aio: MagicMock) -> None:
        mock_path = 'xyz'
        mock_get_path.return_value = mock_path

        mock_run = mocked_loop_runner(mock_aio, return_value=True)

        test_hash, test_size = 'foo', 1
        mock_file = MagicMock(hash=test_hash, file_size=test_size)
        self.storage._cached_files = {test_hash: 'bar'}
        self.storage._cached_files_total_size = 2

        output = await self.storage.remove(mock_file)
        self.assertIsNone(output)
        mock_get_path.assert_called_once_with(mock_file)
        mock_run.assert_awaited_once_with(None, self.storage._delete_file, mock_path)
        self.assertNotIn(test_hash, self.storage._cached_files)
        self.assertEqual(self.storage._cached_files_total_size, 1)
        self.mock_dist_controller_cls().remove_video.assert_called_once_with(mock_file)

        mock_get_path.reset_mock()
        mock_run.reset_mock()
        self.mock_dist_controller_cls().remove_video.reset_mock()
        mock_run.return_value = False

        with self.assertRaises(FileNotFoundError):
            await self.storage.remove(mock_file)
        mock_get_path.assert_called_once_with(mock_file)
        mock_run.assert_awaited_once_with(None, self.storage._delete_file, mock_path)
        self.mock_dist_controller_cls().remove_video.assert_not_called()

    @async_test
    @patch.object(util, 'asyncio')
    @patch.object(util.FileStorage, 'get_thumb_path')
    async def test_remove_thumbs(self, mock_get_thumb_path: MagicMock, mock_aio: MagicMock) -> None:
        mock_path = 'xyz'
        mock_get_thumb_path.return_value = mock_path

        # False must be returned at some point or method would fall into infinite loop during testing
        returns = [True, False]
        mock_run = mocked_loop_runner(mock_aio, side_effect=returns)

        mock_file = MagicMock(hash='foo')

        output = await self.storage.remove_thumbs(mock_file)
        self.assertIsNone(output)
        self.assertListEqual(mock_get_thumb_path.call_args_list,
                             [call(mock_file, i) for i in range(len(returns))])
        self.assertListEqual(mock_run.call_args_list,
                             [call(None, self.storage._delete_file, mock_path) for _ in returns])

    @patch.object(util.FileStorage, 'remove')
    @patch.object(util.FileStorage, 'remove_thumbs')
    @patch.object(util.LMS, 'filter_orphaned_videos')
    async def test_remove_files(self, mock_filter_orphaned_videos: AsyncMock, mock_remove_thumbs: AsyncMock,
                                mock_remove: AsyncMock) -> None:
        test_hashes = [FOO, BAR, BAZ]
        test_origin = "abcde"
        mock_file_foo, mock_file_bar = object(), object()
        self.storage._cached_files = {FOO: mock_file_foo, BAR: mock_file_bar}
        mock_filter_orphaned_videos.return_value = [MagicMock(hash=FOO)]

        output = await self.storage.remove_files(*test_hashes, origin=test_origin)
        self.assertSetEqual({BAR, BAZ}, output)
        mock_filter_orphaned_videos.assert_awaited_once_with(mock_file_foo, mock_file_bar, origin=test_origin)
        mock_remove_thumbs.assert_awaited_once_with(mock_file_foo)
        mock_remove.assert_awaited_once_with(mock_file_foo)

        mock_filter_orphaned_videos.reset_mock()
        mock_remove_thumbs.reset_mock()
        mock_remove.reset_mock()

        mock_filter_orphaned_videos.side_effect = util.LMSInterfaceError
        output = await self.storage.remove_files(*test_hashes, origin=test_origin)
        self.assertSetEqual(set(), output)
        mock_filter_orphaned_videos.assert_awaited_once_with(mock_file_foo, mock_file_bar, origin=test_origin)
        mock_remove_thumbs.assert_not_called()
        mock_remove.assert_not_called()

    def test_garbage_collect_temp_dir(self) -> None:
        self.storage.GC_TEMP_FILES_SECS = 1

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
            output = self.storage.garbage_collect_temp_dir()
        finally:
            dir2.rmdir()
            file2.unlink(missing_ok=True)
            dir1.rmdir()
            file1.unlink(missing_ok=True)
            self.storage.temp_dir.rmdir()
        self.assertEqual(output, 1)

    @async_test
    @patch.object(util, 'asyncio')
    async def test__garbage_collect_cron(self, mock_aio: MagicMock) -> None:
        mock_run = mocked_loop_runner(mock_aio, return_value=1)
        # As a way out of the otherwise infinite loop,
        # have the sleep method cause an arbitrary error that we can catch
        mock_aio.sleep = AsyncMock(side_effect=ValueError)

        self.gc_cron_patcher.stop()

        with self.assertRaises(ValueError):
            await self.storage._garbage_collect_cron()
        mock_run.assert_awaited_once_with(None, self.storage.garbage_collect_temp_dir)
        mock_aio.sleep.assert_awaited_once_with(self.storage.GC_ITERATION_SECS)

        mock_run.reset_mock()
        mock_aio.sleep.reset_mock()
        mock_run.return_value = 0

        with self.assertRaises(ValueError):
            await self.storage._garbage_collect_cron()
        mock_run.assert_awaited_once_with(None, self.storage.garbage_collect_temp_dir)
        mock_aio.sleep.assert_awaited_once_with(self.storage.GC_ITERATION_SECS)

        self.gc_cron_patcher.start()

    @async_test
    @patch.object(util.StorageStatus, 'construct')
    @patch.object(util.FileStorage, 'get_files_total_size_mb')
    @patch.object(util.FileStorage, 'get_files_count')
    @patch.object(util, 'get_free_disk_space')
    @patch.object(util.NetworkInterfaces, 'get_instance')
    async def test_get_status(self, mock_ni_get_instance: MagicMock, mock_get_free_disk_space: MagicMock, mock_get_files_count: MagicMock,
                              mock_get_files_total_size_mb: MagicMock, mock_status_construct: MagicMock) -> None:
        mock_get_dist_node_base_urls = self.mock_dist_controller_cls.return_value.get_dist_node_base_urls
        mock_get_dist_node_base_urls.return_value = mock_dist_node_urls = ['foo', 'bar']
        mock_update_node_status = mock_ni_get_instance.return_value.update_node_status
        mock_get_free_disk_space.return_value = mock_free_disk_space = 999
        mock_get_files_count.return_value = mock_files_count = 111
        mock_get_files_total_size_mb.return_value = mock_files_total_size = 555
        mock_status_construct.return_value = mock_status = MagicMock()
        output = await self.storage.get_status()
        self.assertEqual(mock_status, output)
        self.assertEqual(mock_files_total_size, output.files_total_size)
        self.assertEqual(mock_files_count, output.files_count)
        self.assertEqual(mock_free_disk_space, output.free_space)
        mock_get_free_disk_space.assert_awaited_once_with(str(self.mock_settings.files_path))
        self.assertEqual(self.mock_settings.tx_max_rate_mbit, output.tx_max_rate)
        mock_update_node_status.assert_called_once_with(mock_status, logger=util.log)
        self.assertEqual(mock_dist_node_urls, output.distributor_nodes)
        self.assertEqual(self.storage.num_current_uploads, output.num_current_uploads)


class TempFileTestCase(BaseTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.storage_patcher = patch.object(util, 'FileStorage')
        self.mock_storage = self.storage_patcher.start()

        self.mock_path = MagicMock()
        self.obj = util.TempFile(MagicMock(), MagicMock(), util.FileStorage(self.mock_path))

    def tearDown(self) -> None:
        super().tearDown()
        self.storage_patcher.stop()

    @async_test
    @patch.object(util, 'asyncio')
    async def test_write(self, mock_aio: MagicMock) -> None:
        mock_run = mocked_loop_runner(mock_aio)

        mock_data = bytes('foo', encoding='utf8')
        output = await self.obj.write(mock_data)
        self.assertIsNone(output)
        mock_run.assert_awaited_once_with(None, self.obj._update_hash_write_file, mock_data)
        self.assertEqual(self.obj.size, len(mock_data))

        mock_run.reset_mock()
        self.obj.is_writing = True
        with self.assertRaises(util.PendingWriteOperationError):
            await self.obj.write(mock_data)
        mock_run.assert_not_awaited()

    def test__update_hash_write_file(self) -> None:
        mock_data = bytes('foo', encoding='utf8')
        self.assertIsNone(self.obj._update_hash_write_file(mock_data))
        self.obj.hash.update.assert_called_once_with(mock_data)
        self.assertIsInstance(self.obj.file, MagicMock)  # To prevent pycharm from chastising us
        self.obj.file.write.assert_called_once_with(mock_data)

    @async_test
    @patch.object(util, 'asyncio')
    async def test_close(self, mock_aio: MagicMock) -> None:
        mock_run = mocked_loop_runner(mock_aio)
        self.assertIsNone(await self.obj.close())
        mock_run.assert_awaited_once_with(None, self.obj.file.close)

    @async_test
    @patch.object(util, 'asyncio')
    @patch.object(util, 'HashedVideoFile')
    async def test_persist(self, mock_hashed_file_cls: MagicMock, mock_aio: MagicMock) -> None:
        mock_run = mocked_loop_runner(mock_aio)

        mock_file = MagicMock()
        mock_hashed_file_cls.return_value = mock_file
        test_ext = 'foo'

        output = await self.obj.persist(test_ext)
        self.assertEqual(output, mock_file)
        mock_hashed_file_cls.assert_called_once_with(self.obj.hash.hexdigest.return_value, test_ext)
        mock_run.assert_awaited_once_with(None, self.obj._move, mock_file)

        mock_hashed_file_cls.reset_mock()
        mock_run.reset_mock()
        self.obj.is_writing = True

        with self.assertRaises(util.PendingWriteOperationError):
            await self.obj.persist(test_ext)
        mock_hashed_file_cls.assert_not_called()
        mock_run.assert_not_called()

    @patch.object(util.TempFile, 'get_path')
    def test__move(self, mock_get_path: MagicMock) -> None:
        mock_is_file = MagicMock(return_value=True)
        mock_path = MagicMock(is_file=mock_is_file)
        mock_get_path.return_value = mock_path

        mock_file = MagicMock()
        self.assertIsNone(self.obj._move(mock_file))
        mock_get_path.assert_called_once_with(self.obj.storage.temp_dir, mock_file)
        self.assertIsInstance(self.obj.path, MagicMock)  # preventing type checker from flipping out
        self.obj.path.unlink.assert_called_once_with()
        self.obj.path.rename.assert_not_called()

        mock_get_path.reset_mock()
        self.obj.path.unlink.reset_mock()
        self.obj.path.rename.reset_mock()
        mock_path.is_file.return_value = False
        self.assertIsNone(self.obj._move(mock_file))
        mock_get_path.assert_called_once_with(self.obj.storage.temp_dir, mock_file)
        self.obj.path.unlink.assert_not_called()
        self.obj.path.rename.assert_called_once_with(mock_path)

    def test_get_path(self) -> None:
        test_hash, test_ext = 'foo', 'bar'
        test_temp_dir, test_file = Path('test'), MagicMock(hash=test_hash, file_ext=test_ext)
        expected_output = Path(test_temp_dir, test_hash + test_ext)
        output = self.obj.get_path(test_temp_dir, test_file)
        self.assertEqual(output, expected_output)

    def test_get_thumb_path(self) -> None:
        test_hash = 'foo'
        test_temp_dir, test_file, test_number = Path('test'), MagicMock(hash=test_hash), 1
        expected_output = Path(test_temp_dir, test_hash + "_" + str(test_number) + util.JPG_EXT)
        output = self.obj.get_thumb_path(test_temp_dir, test_file, test_number)
        self.assertEqual(output, expected_output)

    @async_test
    @patch.object(util, 'asyncio')
    async def test_delete(self, mock_aio) -> None:
        mock_run = mocked_loop_runner(mock_aio)
        self.assertIsNone(await self.obj.delete())
        mock_run.assert_awaited_once_with(None, self.obj._delete_file)

    def test__delete_file(self) -> None:
        # preventing type checker from flipping out
        self.assertIsInstance(self.obj.file, MagicMock)
        self.assertIsInstance(self.obj.path, MagicMock)

        self.obj.path.is_file.return_value = True
        self.assertIsNone(self.obj._delete_file())
        self.obj.file.close.assert_called_once_with()
        self.obj.path.unlink.assert_called_once_with()

        self.obj.file.close.reset_mock()
        self.obj.path.unlink.reset_mock()

        self.obj.path.is_file.return_value = False
        self.assertIsNone(self.obj._delete_file())
        self.obj.file.close.assert_called_once_with()
        self.obj.path.unlink.assert_not_called()


class FunctionsTestCase(BaseTestCase):

    def test_create_dir_if_not_exists(self) -> None:
        # Should not do anything, because the root directory always exists:
        util.create_dir_if_not_exists('/')

        # Test creation
        test_path = Path('/tmp/videbo_test_path')
        util.create_dir_if_not_exists(test_path)
        self.assertTrue(test_path.is_dir())
        test_path.rmdir()

        # Test creation with explicit permissions setting:
        test_mode = 0o777
        util.create_dir_if_not_exists(test_path, mode=test_mode, explicit_chmod=True)
        self.assertTrue(test_path.is_dir())
        self.assertEqual(oct(test_path.stat().st_mode & 0o777), oct(test_mode))
        test_path.rmdir()

        # Test throwing error for unsuccessful creation:
        mock_path = MagicMock()
        mock_path.is_dir.return_value = False
        with patch(TESTED_MODULE_PATH + '.Path') as mock_path_class:
            mock_path_class.return_value = mock_path
            with self.assertRaises(util.CouldNotCreateDir):
                util.create_dir_if_not_exists(mock_path)

    def test_is_allowed_file_ending(self) -> None:
        test_name = None
        self.assertTrue(util.is_allowed_file_ending(test_name))
        test_name = 'foobar'
        self.assertFalse(util.is_allowed_file_ending(test_name))
        test_name = 'something.MP4'
        self.assertTrue(util.is_allowed_file_ending(test_name))

    @patch.object(util, 'asyncio')
    @patch.object(util, '_video_delete_task', new_callable=MagicMock)  # No need for async mock
    @patch.object(util, 'TaskManager')
    def test_schedule_video_delete(self, mock_task_mgr: MagicMock, mock__video_delete_task: MagicMock, mock_aio: MagicMock) -> None:
        mock_aio.create_task.return_value = 'foo'

        test_hash, test_ext, test_origin = 'abc', 'xyz', 'bar'
        self.assertIsNone(util.schedule_video_delete(test_hash, test_ext, test_origin))
        mock__video_delete_task.assert_called_once_with(test_hash, test_ext, test_origin)
        mock_aio.create_task.assert_called_once_with(mock__video_delete_task.return_value)
        mock_task_mgr.fire_and_forget_task.assert_called_once_with('foo')

    @async_test
    @patch.object(util.FileStorage, 'get_instance')
    async def test__video_delete_task(self, mock_get_instance: MagicMock) -> None:
        mock_file = MagicMock(hash=FOO)
        mock_get_file, mock_remove_files = AsyncMock(return_value=mock_file), AsyncMock()
        mock_get_instance.return_value.get_file = mock_get_file
        mock_get_instance.return_value.remove_files = mock_remove_files

        test_hash, test_ext, test_origin = 'abc', 'xyz', 'bar'
        self.assertIsNone(await util._video_delete_task(test_hash, test_ext, test_origin))
        mock_get_file.assert_awaited_once_with(test_hash, test_ext)
        mock_remove_files.assert_awaited_once_with(FOO, origin=test_origin)

        mock_get_file.reset_mock()
        mock_remove_files.reset_mock()
        mock_get_file.side_effect = FileNotFoundError
        self.assertIsNone(await util._video_delete_task(test_hash, test_ext, test_origin))
        mock_get_file.assert_awaited_once_with(test_hash, test_ext)
        mock_remove_files.assert_not_awaited()


def mocked_loop_runner(mock_asyncio: MagicMock, mock_cls: Type[M] = AsyncMock, **kwargs) -> M:
    mock_run = mock_cls(**kwargs)
    mock_loop = MagicMock(run_in_executor=mock_run)
    mock_asyncio.get_event_loop.return_value = mock_loop
    return mock_run
