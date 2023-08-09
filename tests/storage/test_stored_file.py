from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch

from videbo.storage import stored_file


class StoredVideoFileTestCase(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.hashed_file_patcher = patch.object(stored_file.HashedFile, "__init__")
        self.mock_hashed_file___init__ = self.hashed_file_patcher.start()

    def tearDown(self) -> None:
        self.hashed_file_patcher.stop()
        super().tearDown()

    def test___init__(self) -> None:
        file = stored_file.StoredVideoFile(file_hash="foo", file_ext=".bar")
        self.mock_hashed_file___init__.assert_called_once_with("foo", ".bar")
        self.assertEqual(-1, file.size)
        self.assertDictEqual({}, file.unique_views)

    @patch.object(stored_file.StoredVideoFile, "num_views", new_callable=PropertyMock)
    def test___lt__(self, mock_num_views: PropertyMock) -> None:
        file = stored_file.StoredVideoFile(file_hash="foo", file_ext=".bar")
        mock_num_views.return_value = 10
        other = MagicMock(num_views=20)
        self.assertLess(file, other)

    def test_num_views(self) -> None:
        file = stored_file.StoredVideoFile(file_hash="foo", file_ext=".bar")
        file.unique_views = {"a": 123, "b": 456}
        self.assertEqual(2, file.num_views)

    @patch.object(stored_file, "time")
    def test_register_view_by(self, mock_time: MagicMock) -> None:
        mock_time.return_value = mock_now = 1000
        file = stored_file.StoredVideoFile(file_hash="foo", file_ext=".bar")
        file.unique_views = {"a": 123, "b": 456}
        file.register_view_by("a")
        self.assertDictEqual({"b": 456, "a": mock_now}, file.unique_views)

    def test_discard_views_older_than(self) -> None:
        file = stored_file.StoredVideoFile(file_hash="foo", file_ext=".bar")
        file.unique_views = {"a": 123, "b": 456, "c": 789}
        file.discard_views_older_than(200)
        self.assertDictEqual({"b": 456, "c": 789}, file.unique_views)
        file.unique_views = {}
        file.discard_views_older_than(200)
        self.assertDictEqual({}, file.unique_views)
