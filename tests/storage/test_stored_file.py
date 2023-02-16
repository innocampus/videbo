from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch

from videbo.storage import stored_file


class StoredVideoFileTestCase(TestCase):
    @patch.object(stored_file.HashedFile, "__init__")
    def test___init__(self, mock_superclass_init: MagicMock) -> None:
        obj = stored_file.StoredVideoFile(file_hash="foo", file_ext="bar")
        mock_superclass_init.assert_called_once_with("foo", "bar")
        self.assertEqual(-1, obj.size)
        self.assertDictEqual({}, obj.unique_views)
        self.assertListEqual([], obj.nodes)
        self.assertFalse(obj.copying)

    @patch.object(stored_file.StoredVideoFile, "num_views", new_callable=PropertyMock)
    @patch.object(stored_file.HashedFile, "__init__")
    def test___lt__(self, _: MagicMock, mock_num_views: PropertyMock) -> None:
        obj = stored_file.StoredVideoFile(file_hash="foo", file_ext="bar")
        mock_num_views.return_value = 10
        other = MagicMock(num_views=20)
        self.assertLess(obj, other)
