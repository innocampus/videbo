import logging
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, call, patch
from urllib.parse import urlencode

from videbo import lms_api


main_log = logging.getLogger('videbo')


class LMSTestCase(IsolatedAsyncioTestCase):
    log_lvl: int

    @classmethod
    def setUpClass(cls) -> None:
        cls.log_lvl = main_log.level
        main_log.setLevel(logging.CRITICAL)

    @classmethod
    def tearDownClass(cls) -> None:
        main_log.setLevel(cls.log_lvl)

    def tearDown(self) -> None:
        lms_api.LMS._collection = {}

    @patch.object(lms_api.LMS, "__init__")
    def test_add(self, mock___init__: MagicMock) -> None:
        mock___init__.return_value = None
        url1, url2, url3 = "foo", "bar", "baz"
        self.assertIsNone(lms_api.LMS.add(url1, url2, url3))
        mock___init__.assert_has_calls([call(url1), call(url2), call(url3)])

    def test_iter_all(self) -> None:
        mock_lms1, mock_lms2 = MagicMock(), MagicMock()
        lms_api.LMS._collection = {"foo": mock_lms1, "bar": mock_lms2}
        out = list(lms_api.LMS.iter_all())
        self.assertListEqual([mock_lms1, mock_lms2], out)

    def test___init__(self) -> None:
        test_url = "foo"
        obj = lms_api.LMS(api_url=test_url)
        self.assertEqual(test_url, obj.api_url)
        self.assertDictEqual({test_url: obj}, lms_api.LMS._collection)

    @patch.object(lms_api.LMSRequestJWTData, "get_standard_token")
    @patch.object(lms_api.HTTPClient, "videbo_request")
    async def test__post_request(self, mock_videbo_request: AsyncMock, mock_get_standard_token: MagicMock) -> None:
        test_url = "abc"
        test_function = "foo"

        expected_query = urlencode({lms_api.LMS.FUNCTION_QUERY_PARAMETER: test_function})
        expected_url = f"{test_url}?{expected_query}"

        mock_data = MagicMock()
        mock_return_type = MagicMock
        mock_get_standard_token.return_value = mock_token = "xyz"
        mock_videbo_request.return_value = mock_response = object()

        obj = lms_api.LMS(api_url=test_url)
        output = await obj._post_request(test_function, mock_data, expected_return_type=mock_return_type)
        self.assertEqual(mock_response, output)
        mock_get_standard_token.assert_called_once_with()
        mock_videbo_request.assert_awaited_once_with(
            "POST",
            expected_url,
            mock_token,
            mock_data,
            mock_return_type,
            timeout=30,
            external=True,
        )

    @patch.object(lms_api.LMS, "_post_request")
    async def test_videos_missing(self, mock__post_request: AsyncMock) -> None:
        test_video1 = lms_api.VideoModel(hash="foo", file_ext=".mp4")
        test_video2 = lms_api.VideoModel(hash="bar", file_ext=".mp4")
        mock__post_request.side_effect = lms_api.HTTPResponseError

        with self.assertRaises(lms_api.LMSInterfaceError):
            await lms_api.LMS("abc").videos_missing(test_video1, test_video2)

        mock__post_request.assert_awaited_once_with(
            "videos_missing",
            lms_api.VideosMissingRequest(videos=[test_video1, test_video2]),
            lms_api.VideosMissingResponse,
        )

        mock__post_request.reset_mock()
        mock__post_request.side_effect = None
        mock__post_request.return_value = 400, None

        with self.assertRaises(lms_api.LMSInterfaceError):
            await lms_api.LMS("abc").videos_missing(test_video1, test_video2)

        mock__post_request.assert_awaited_once_with(
            "videos_missing",
            lms_api.VideosMissingRequest(videos=[test_video1, test_video2]),
            lms_api.VideosMissingResponse,
        )

        mock__post_request.reset_mock()
        mock_response = MagicMock()
        mock__post_request.return_value = 200, mock_response

        output = await lms_api.LMS("abc").videos_missing(test_video1, test_video2)
        self.assertIs(mock_response, output)

        mock__post_request.assert_awaited_once_with(
            "videos_missing",
            lms_api.VideosMissingRequest(videos=[test_video1, test_video2]),
            lms_api.VideosMissingResponse,
        )

    @patch.object(lms_api.LMS, "VIDEOS_CHECK_MAX_BATCH_SIZE", new=2)
    @patch.object(lms_api, "VideoModel")
    @patch.object(lms_api.LMS, "iter_all")
    async def test_filter_orphaned_videos(self, mock_iter_all: MagicMock, mock_video_model_cls: MagicMock) -> None:
        mock_video1 = MagicMock(hash="foo", file_ext=".mp4")
        mock_video2 = MagicMock(hash="bar", file_ext=".mp4")
        mock_video3 = MagicMock(hash="baz", file_ext=".mp4")
        test_origin = "https://spam.eggs"
        lms1 = MagicMock(api_url=test_origin + "/something", videos_missing=AsyncMock())
        lms2 = MagicMock(api_url="example.com/other", videos_missing=AsyncMock())

        mock_iter_all.return_value = [lms1, lms2]
        mock_video_model_cls.from_orm = lambda x: x
        
        output = await lms_api.LMS.filter_orphaned_videos(mock_video1, mock_video2, mock_video3, origin=test_origin)
        self.assertListEqual([], output)

        mock_iter_all.assert_has_calls([call(), call()])
        lms1.videos_missing.assert_not_called()
        lms2.videos_missing.assert_has_awaits([
            call(mock_video1, mock_video2),
            call(mock_video3),
        ])

        mock_iter_all.reset_mock()
        lms2.videos_missing.reset_mock()
        lms2.videos_missing.side_effect = lms_api.LMSInterfaceError

        with self.assertRaises(lms_api.LMSInterfaceError):
            await lms_api.LMS.filter_orphaned_videos(mock_video1, mock_video2, mock_video3, origin=test_origin)

        mock_iter_all.assert_called_once_with()
        lms1.videos_missing.assert_not_called()
        lms2.videos_missing.assert_awaited_once_with(mock_video1, mock_video2)
