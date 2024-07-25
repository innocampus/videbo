import shutil
from collections.abc import ValuesView
from pathlib import Path
from tempfile import mkdtemp
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, Mock, PropertyMock, patch

from tests.silent_log import SilentLogMixin
from videbo.file_controller import FileController
from videbo.hashed_file import HashedFile
from videbo.misc.constants import MEGA


class FileControllerTestCase(SilentLogMixin, IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.abstract_patcher = patch.multiple(
            FileController,
            __abstractmethods__=set(),
        )
        cls.abstract_patcher.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.abstract_patcher.stop()

    def setUp(self) -> None:
        super().setUp()

        # Mock `Client` constructor (in the parent class):
        self.client_patcher = patch("videbo.file_controller.Client")
        self.mock_client_cls = self.client_patcher.start()

        # Mock `_load_file_list` method (in the parent class):
        self._load_file_list_patcher = patch.object(
            FileController,
            "_load_file_list",
        )
        self.mock_load_file_list = self._load_file_list_patcher.start()

        # Initialize file_controller instance for convenience:
        self.file_controller = FileController()

    def tearDown(self) -> None:
        super().tearDown()
        # Start with a fresh instance every time:
        FileController._instance = None
        self._load_file_list_patcher.stop()
        self.client_patcher.stop()

    def test___init__(self) -> None:
        self.mock_client_cls.assert_called_once_with()
        self.mock_load_file_list.assert_called_once_with()

    def test___getitem__(self) -> None:
        mock_file_1 = Mock()
        mock_file_2 = Mock()
        self.file_controller._files = {"foo": mock_file_1, "bar": mock_file_2}
        self.assertIs(mock_file_1, self.file_controller["foo"])
        self.assertIs(mock_file_2, self.file_controller["bar"])

    def test___len__(self) -> None:
        self.assertEqual(0, len(self.file_controller))
        mock_file_1 = Mock()
        mock_file_2 = Mock()
        self.file_controller._files = {"foo": mock_file_1, "bar": mock_file_2}
        self.assertEqual(2, len(self.file_controller))

    def test___str__(self) -> None:
        self.assertEqual("FileController", str(self.file_controller))

    def test_iter_files(self) -> None:
        mock_file_1 = Mock()
        mock_file_2 = Mock()
        mock_file_3 = Mock()
        self.file_controller._files = {
            "foo": mock_file_1,
            "bar": mock_file_2,
            "baz": mock_file_3,
        }
        view = self.file_controller.iter_files()
        self.assertIsInstance(view, ValuesView)
        self.assertListEqual(
            [mock_file_1, mock_file_2, mock_file_3],
            list(view),
        )

    def test_files_total_size_mb(self) -> None:
        self.file_controller._files_total_size = 2.1 * MEGA
        self.assertEqual(2.1, self.file_controller.files_total_size_mb)

    @patch.object(type(FileController), "__call__")
    async def test_app_context(self, mock_controller_cls: MagicMock) -> None:
        iterator = FileController.app_context(MagicMock())
        self.assertIsNone(await iterator.__anext__())
        mock_controller_cls.assert_called_once_with()

        mock_controller_cls.reset_mock()

        with self.assertRaises(StopAsyncIteration):
            await iterator.__anext__()

        mock_controller_cls.assert_not_called()

    def test__add_file(self) -> None:
        self.assertDictEqual({}, self.file_controller._files)
        self.assertEqual(0, self.file_controller._files_total_size)
        mock_file = MagicMock(hash="foo", size=123)
        self.file_controller._add_file(mock_file)
        self.assertDictEqual({"foo": mock_file}, self.file_controller._files)
        self.assertEqual(123, self.file_controller._files_total_size)

    def test__remove_file(self) -> None:
        self.file_controller._files = {"foo": Mock(size=123)}
        self.file_controller._files_total_size = 123
        self.file_controller._remove_file("foo")
        self.assertDictEqual({}, self.file_controller._files)
        self.assertEqual(0, self.file_controller._files_total_size)

    @patch.object(FileController, "MAX_NUM_FILES_TO_PRINT", new=2)
    @patch.object(FileController, "files_dir", new_callable=PropertyMock)
    @patch.object(FileController, "get_type_arg", return_value=HashedFile)
    def test__load_file_list(
        self,
        mock_get_type_arg: MagicMock,
        mock_files_dir: PropertyMock,
    ) -> None:
        self._load_file_list_patcher.stop()
        mock_get_type_arg.return_value = HashedFile

        # Pseudo file hashes:
        a_hash = "a" * 64
        b_hash = "b" * 64
        c_hash = "c" * 64
        d_hash = "d" * 64

        # Make sure we start with an empty file controller:
        self.assertEqual(0, self.file_controller._files_total_size)
        self.assertEqual(0, len(self.file_controller._files))

        # Actually write a few bytes to a temporary directory,
        # but make sure we remove it afterwards.
        files_dir = Path(mkdtemp(prefix="videbo_test"))
        try:
            mock_files_dir.return_value = files_dir
            # Dummy files for this test:
            test_dir = Path(files_dir, "test")
            test_dir.mkdir()
            a_file_path = Path(test_dir, f"{a_hash}.mp4")
            b_file_path = Path(test_dir, f"{b_hash}.webm")
            c_file_path = Path(test_dir, f"{c_hash}.mp4")
            d_file_path = Path(test_dir, f"{d_hash}.webm")
            no_ext_path = Path(test_dir, "foo")
            two_ext_path = Path(test_dir, "foo.bar.mp4")
            not_allowed_ext_path = Path(test_dir, "foo.mkv")
            with a_file_path.open(mode="wb") as f:
                f.write(b"abc")  # 3 bytes
            with b_file_path.open(mode="wb") as f:
                f.write(b"abcdef")  # 6 bytes
            with c_file_path.open(mode="wb") as f:
                f.write(b"a")  # 1 byte
            d_file_path.touch() # 0 bytes
            no_ext_path.touch()
            two_ext_path.touch()
            not_allowed_ext_path.touch()
            Path(test_dir, "not_a_file").mkdir()
            with self.assertLogs(self.file_controller.log) as log_ctx:
                self.file_controller._load_file_list()
            # Check that the file sizes were calculated correctly:
            self.assertEqual(10, self.file_controller._files_total_size)
            # Check that all three files were added:
            self.assertEqual(4, len(self.file_controller))
            self.assertIn(a_hash, self.file_controller)
            self.assertIn(b_hash, self.file_controller)
            self.assertIn(c_hash, self.file_controller)
            # Check that logging was done as expected:
            self.assertEqual(7, len(log_ctx.records))
            log_messages = [record.msg for record in log_ctx.records]
            self.assertListEqual(
                [
                    f"Adding file {a_file_path} to {self.file_controller}",
                    f"Adding file {b_file_path} to {self.file_controller}",
                    "Skip logging other files being added...",
                    f"File with invalid/no extension: {no_ext_path}",
                    f"File with invalid/no extension: {two_ext_path}",
                    f"File extension not allowed: {not_allowed_ext_path}",
                    f"{self.file_controller} found 4 files",
                ],
                log_messages,
            )

            # None of the files should be added:
            self.file_controller._files.clear()
            self.file_controller._files_total_size = 0
            with patch.object(
                FileController,
                "load_file_predicate",
                return_value=False,
            ), self.assertLogs(self.file_controller.log) as log_ctx:
                self.file_controller._load_file_list()
            self.assertEqual(0, self.file_controller._files_total_size)
            self.assertEqual(0, len(self.file_controller))
            self.assertEqual(1, len(log_ctx.records))
            self.assertEqual(
                f"{self.file_controller} found 0 files",
                log_ctx.records[0].msg,
            )

            # Invalid files directory:
            mock_files_dir.return_value = a_file_path
            with self.assertRaises(NotADirectoryError):
                self.file_controller._load_file_list()
        finally:
            shutil.rmtree(files_dir)
