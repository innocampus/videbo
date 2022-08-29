from unittest import TestCase
from unittest.mock import MagicMock, patch

from videbo.storage.api import models


class FileTypeTestCase(TestCase):

    def test_values(self) -> None:
        output = models.FileType.values()
        expected = frozenset(member.value for member in models.FileType.__members__.values())
        self.assertSetEqual(expected, output)


class StorageFileInfoTestCase(TestCase):

    def test___repr__(self) -> None:
        file_hash, file_ext = 'spam', '.eggs'
        obj = models.StorageFileInfo(hash=file_hash, file_extension=file_ext, file_size=12345)
        self.assertEqual(file_hash + file_ext, repr(obj))

    @patch.object(models.StorageFileInfo, '__repr__')
    def test___str__(self, mock_repr: MagicMock) -> None:
        mock_repr.return_value = string = 'foobar'
        obj = models.StorageFileInfo(hash='something', file_extension='.else', file_size=12345)
        self.assertEqual(string, str(obj))
