from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from videbo import temp_file


class TempFileTestCase(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.__init___patcher = patch.object(
            temp_file.TempFile,
            "__init__",
            return_value=None,
        )
        self.mock___init__ = self.__init___patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        self.__init___patcher.stop()

    @patch.object(temp_file, "mkstemp")
    def test_create(self, mock_mkstemp: MagicMock) -> None:
        mock_mkstemp.return_value = mock_fd, mock_path = object(), "foo"
        test_dir = "bar"
        obj = temp_file.TempFile.create(test_dir, open_mode="wb")
        self.assertIsInstance(obj, temp_file.TempFile)
        mock_mkstemp.assert_called_once_with(
            prefix=temp_file.TempFile.file_name_prefix,
            dir=test_dir,
        )
        self.mock___init__.assert_called_once_with(
            mock_fd,
            Path(mock_path),
            open_mode="wb",
        )

    @patch.object(temp_file.TempFile, "open")
    @patch.object(temp_file, "sha256")
    @patch.object(temp_file.Path, "chmod")
    def test___init__(
            self,
            mock_chmod: MagicMock,
            mock_sha256: MagicMock,
            mock_open: MagicMock,
    ) -> None:
        self.__init___patcher.stop()

        mock_sha256.return_value = mock_hash = object()
        fd = 123
        path = "foo"
        obj = temp_file.TempFile(fd, path)
        self.assertEqual(fd, obj._file_descriptor)
        self.assertEqual(Path(path), obj._path)
        self.assertFalse(obj._is_writing)
        self.assertIsNone(obj._file_handle)
        self.assertEqual(mock_hash, obj._hash)
        self.assertEqual(0, obj._size)
        mock_chmod.assert_called_once_with(0o644)
        mock_open.assert_not_called()

        mock_chmod.reset_mock()

        obj = temp_file.TempFile(fd, path, open_mode="wb")
        self.assertEqual(fd, obj._file_descriptor)
        self.assertEqual(Path(path), obj._path)
        self.assertFalse(obj._is_writing)
        self.assertIsNone(obj._file_handle)
        self.assertEqual(mock_hash, obj._hash)
        self.assertEqual(0, obj._size)
        mock_chmod.assert_called_once_with(0o644)
        mock_open.assert_called_once_with("wb")

        self.__init___patcher.start()

    def test_path(self) -> None:
        obj = temp_file.TempFile(0, "")
        obj._path = test_path = object()
        self.assertIs(test_path, obj.path)

    def test_is_open(self) -> None:
        obj = temp_file.TempFile(0, "")
        obj._file_handle = object()
        self.assertTrue(obj.is_open)
        obj._file_handle = None
        self.assertFalse(obj.is_open)

    def test_digest(self) -> None:
        obj = temp_file.TempFile(0, "")
        expected_output = object()
        obj._hash = MagicMock(
            hexdigest=MagicMock(return_value=expected_output)
        )
        self.assertIs(expected_output, obj.digest)

    def test_size(self) -> None:
        obj = temp_file.TempFile(0, "")
        obj._size = test_size = object()
        self.assertIs(test_size, obj.size)

    @patch.object(temp_file, "sha256")
    @patch.object(temp_file.os, "fdopen")
    def test_open(self, mock_fdopen: MagicMock, mock_sha256: MagicMock) -> None:
        mock_fdopen.return_value = mock_handle = object()
        mock_sha256.return_value = mock_hash = object()
        obj = temp_file.TempFile(0, "")
        obj._size = 12345
        obj._file_descriptor = mock_fd = object()

        output = obj.open("rb")
        self.assertIs(obj, output)
        self.assertEqual(mock_handle, obj._file_handle)
        self.assertEqual(mock_hash, obj._hash)
        self.assertEqual(0, obj._size)
        mock_fdopen.assert_called_once_with(mock_fd, "rb")
        mock_sha256.assert_called_once_with()

    @patch.object(temp_file, "run_in_default_executor")
    async def test_close(self, mock_run_in_default_executor: AsyncMock) -> None:
        obj = temp_file.TempFile(0, "")
        obj._file_handle = None
        self.assertIsNone(await obj.close())
        mock_run_in_default_executor.assert_not_called()

        obj._file_handle = mock_handle = MagicMock()
        self.assertIsNone(await obj.close())
        mock_run_in_default_executor.assert_awaited_once_with(mock_handle.close)
        self.assertIsNone(obj._file_handle)

    @patch.object(temp_file.TempFile, "is_open", new_callable=PropertyMock)
    async def test___aenter__(self, mock_is_open: PropertyMock) -> None:
        mock_is_open.return_value = False
        obj = temp_file.TempFile(0, "")
        with self.assertRaises(RuntimeError):
            await obj.__aenter__()

        mock_is_open.return_value = True
        output = await obj.__aenter__()
        self.assertIs(obj, output)

    @patch.object(temp_file.TempFile, "close")
    async def test___aexit__(self, mock_close: AsyncMock) -> None:
        obj = temp_file.TempFile(0, "")
        self.assertIsNone(await obj.__aexit__(None, None, None))
        mock_close.assert_awaited_once_with()

    def test__write(self) -> None:
        obj = temp_file.TempFile(0, "")
        obj._file_handle = None
        obj._hash = MagicMock()
        obj._size = 0

        data = b"abc"
        with self.assertRaises(RuntimeError):
            obj._write(data)
        obj._hash.update.assert_not_called()
        self.assertEqual(0, obj._size)

        obj._file_handle = MagicMock()

        self.assertIsNone(obj._write(data))
        obj._file_handle.write.assert_called_once_with(data)
        obj._hash.update.assert_called_once_with(data)
        self.assertEqual(len(data), obj._size)

    @patch.object(temp_file, "run_in_default_executor")
    async def test_write(self, mock_run_in_default_executor: AsyncMock) -> None:
        obj = temp_file.TempFile(0, "")
        obj._is_writing = True
        data = b"abc"
        with self.assertRaises(temp_file.PendingWriteOperationError):
            await obj.write(data)
        mock_run_in_default_executor.assert_not_called()

        obj._is_writing = False
        self.assertIsNone(await obj.write(data))
        mock_run_in_default_executor.assert_awaited_once_with(obj._write, data)

    @patch.object(temp_file, "run_in_default_executor")
    async def test_delete(self, mock_run_in_default_executor: AsyncMock) -> None:
        obj = temp_file.TempFile(0, "")
        obj._path = MagicMock()
        self.assertIsNone(await obj.delete())
        mock_run_in_default_executor.assert_awaited_once_with(
            obj._path.unlink,
            True,
        )

    @patch.object(temp_file, "run_in_default_executor")
    @patch.object(temp_file.TempFile, "digest", new_callable=PropertyMock)
    async def test_persist(
            self,
            mock_digest: PropertyMock,
            mock_run_in_default_executor: AsyncMock,
    ) -> None:
        mock_digest.return_value = digest = "abcdef"
        obj = temp_file.TempFile(0, "")
        obj._is_writing = True
        obj._path = initial_path = Path("foo", "bar.baz")

        # Error when `_is_writing` is `True`:
        with self.assertRaises(temp_file.PendingWriteOperationError):
            await obj.persist()
        mock_digest.assert_not_called()
        mock_run_in_default_executor.assert_not_called()
        self.assertEqual(initial_path, obj._path)

        # Target should be in the same directory with the same extension,
        # but with the hash digest as the file name stem:
        obj._is_writing = False
        expected_target_path = initial_path.with_stem(digest)
        output = await obj.persist()
        self.assertEqual(expected_target_path, output)
        self.assertEqual(expected_target_path, obj._path)
        mock_digest.assert_called_once_with()
        mock_run_in_default_executor.assert_awaited_once_with(
            temp_file.move_file,
            initial_path,
            expected_target_path,
        )
        self.assertTrue(obj._is_writing)

        mock_digest.reset_mock()
        mock_run_in_default_executor.reset_mock()
        obj._is_writing = False
        obj._path = initial_path

        # Target should be in a new directory with a new extension,
        # and with a specified file name stem:
        file_ext = ".eggs"
        file_name_stem = "spam"
        target_dir = "something"
        expected_target_path = Path(
            target_dir,
            initial_path.with_stem(file_name_stem).with_suffix(file_ext).name,
        )
        output = await obj.persist(
            file_ext=file_ext,
            file_name_stem=file_name_stem,
            target_dir=target_dir,
        )
        self.assertEqual(expected_target_path, output)
        self.assertEqual(expected_target_path, obj._path)
        mock_digest.assert_not_called()
        mock_run_in_default_executor.assert_awaited_once_with(
            temp_file.move_file,
            initial_path,
            expected_target_path,
        )
        self.assertTrue(obj._is_writing)
