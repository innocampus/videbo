from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch

from videbo.storage.stored_file import StoredVideoFile


class StoredVideoFileTestCase(TestCase):
    def test___init__(self) -> None:
        test_hash = "a" * 64
        file = StoredVideoFile(
            file_hash=test_hash,
            file_ext=".ext",
            file_size=123,
        )
        self.assertEqual(test_hash, file.hash)
        self.assertEqual(".ext", file.ext)
        self.assertEqual(123, file.size)
        self.assertDictEqual({}, file.unique_views)

    @patch.object(StoredVideoFile, "num_views", new_callable=PropertyMock)
    def test___lt__(self, mock_num_views: PropertyMock) -> None:
        test_hash = "a" * 64
        file = StoredVideoFile(test_hash, ".ext", 123)
        mock_num_views.return_value = 10
        other = MagicMock(num_views=20)
        self.assertLess(file, other)

    def test_num_views(self) -> None:
        test_hash = "a" * 64
        file = StoredVideoFile(test_hash, ".ext", 123)
        file.unique_views = {"a": 123, "b": 456}
        self.assertEqual(2, file.num_views)

    @patch("videbo.storage.stored_file.time")
    def test_register_view_by(self, mock_time: MagicMock) -> None:
        mock_time.return_value = mock_now = 1000
        test_hash = "a" * 64
        file = StoredVideoFile(test_hash, ".ext", 123)
        file.unique_views = {"a": 123, "b": 456}
        file.register_view_by("a")
        self.assertDictEqual({"b": 456, "a": mock_now}, file.unique_views)

    def test_discard_views_older_than(self) -> None:
        test_hash = "a" * 64
        file = StoredVideoFile(test_hash, ".ext", 123)
        file.unique_views = {"a": 123, "b": 456, "c": 789}
        file.discard_views_older_than(200)
        self.assertDictEqual({"b": 456, "c": 789}, file.unique_views)
        file.unique_views = {}
        file.discard_views_older_than(200)
        self.assertDictEqual({}, file.unique_views)
