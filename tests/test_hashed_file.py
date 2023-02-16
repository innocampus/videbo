from unittest import TestCase

from videbo import hashed_file


class HashedFileTestCase(TestCase):
    def test___init__(self) -> None:
        test_hash, test_ext = "test", ".ext"
        obj = hashed_file.HashedFile(file_hash=test_hash, file_ext=test_ext)
        self.assertEqual(test_hash, obj.hash)
        self.assertEqual(test_ext, obj.ext)
        with self.assertRaises(ValueError):
            hashed_file.HashedFile(file_hash=test_hash, file_ext="ext")

    def test___str__(self) -> None:
        test_hash, test_ext = "test", ".ext"
        obj = hashed_file.HashedFile(file_hash=test_hash, file_ext=test_ext)
        self.assertEqual(test_hash + test_ext, str(obj))

    def test__repr__(self) -> None:
        test_hash, test_ext = "test", ".ext"
        obj = hashed_file.HashedFile(file_hash=test_hash, file_ext=test_ext)
        self.assertEqual(f"HashedFile({test_hash}{test_ext})", repr(obj))

    def test___hash__(self) -> None:
        test_hash, test_ext = 12345, ".foo"
        obj = hashed_file.HashedFile(file_hash=f"{test_hash:x}", file_ext=test_ext)
        self.assertEqual(test_hash, hash(obj))

    def test___eq__(self) -> None:
        test_hash = "foobarbaz"
        file1 = hashed_file.HashedFile(file_hash=test_hash, file_ext=".foo")
        file2 = hashed_file.HashedFile(file_hash=test_hash, file_ext=".bar")
        file3 = hashed_file.HashedFile(file_hash="somethingelse", file_ext=".bar")
        self.assertEqual(file1, file2)
        self.assertNotEqual(file3, file2)
        self.assertNotEqual(file1, object())
