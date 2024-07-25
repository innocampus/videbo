from unittest import TestCase
from unittest.mock import MagicMock

from videbo.hashed_file import HashedFile


class HashedFileTestCase(TestCase):
    def test_from_path(self) -> None:
        test_hash = "a" * 64
        mock_path = MagicMock(stem=test_hash, suffix=".ext")
        mock_path.stat.return_value.st_size = 123
        obj = HashedFile.from_path(mock_path)
        self.assertEqual(test_hash, obj.hash)
        self.assertEqual(".ext", obj.ext)
        self.assertEqual(123, obj.size)

    def test___init__(self) -> None:
        test_hash = "a" * 64
        obj = HashedFile(file_hash=test_hash, file_ext=".ext", file_size=123)
        self.assertEqual(test_hash, obj.hash)
        self.assertEqual(".ext", obj.ext)
        self.assertEqual(123, obj.size)
        with self.assertRaises(ValueError):
            HashedFile(file_hash=test_hash, file_ext=".ext.gz", file_size=123)
        with self.assertRaises(ValueError):
            HashedFile(file_hash="aaa", file_ext=".ext", file_size=123)
        with self.assertRaises(ValueError):
            HashedFile(file_hash=test_hash, file_ext=".ext", file_size=-1)

    def test___str__(self) -> None:
        test_hash = "a" * 64
        test_ext = ".ext"
        obj = HashedFile(file_hash=test_hash, file_ext=test_ext, file_size=123)
        self.assertEqual(test_hash + test_ext, str(obj))

    def test__repr__(self) -> None:
        test_hash = "a" * 64
        test_ext = ".ext"
        obj = HashedFile(file_hash=test_hash, file_ext=test_ext, file_size=123)
        self.assertEqual(
            f"HashedFile('{test_hash}', '{test_ext}', 123)",
            repr(obj),
        )

    def test___hash__(self) -> None:
        test_hash = "a" * 64
        expected_output = int(test_hash, 16)
        obj = HashedFile(file_hash=test_hash, file_ext=".foo", file_size=123)
        # Because Python's built-in `hash` function truncates the value returned
        # from the custom `__hash__` method, we call the latter directly here:
        self.assertEqual(expected_output, obj.__hash__())

    def test___eq__(self) -> None:
        a_hash = "a" * 64
        b_hash = "b" * 64
        file1 = HashedFile(file_hash=a_hash, file_ext=".foo", file_size=123)
        file2 = HashedFile(file_hash=a_hash, file_ext=".bar", file_size=123)
        file3 = HashedFile(file_hash=b_hash, file_ext=".foo", file_size=123)
        self.assertEqual(file1, file2)
        self.assertNotEqual(file3, file2)
        self.assertNotEqual(file1, object())
