from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, call, patch
from urllib.parse import urlencode

from tests.silent_log import SilentLogMixin
from videbo import lms_api


class LMSTestCase(SilentLogMixin, IsolatedAsyncioTestCase):
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

    def test__get_function_url(self) -> None:
        test_url = "abc"
        test_function = "foobar"
        expected_query = urlencode({lms_api.LMS.FUNCTION_QUERY_PARAMETER: test_function})
        expected_url = f"{test_url}?{expected_query}"
        output = lms_api.LMS(test_url)._get_function_url(test_function)
        self.assertEqual(expected_url, output)

    @patch.object(lms_api, "VideosMissingRequest")
    @patch.object(lms_api.LMSRequestJWTData, "get_standard_token")
    @patch.object(lms_api.LMS, "_get_function_url")
    async def test_videos_missing(self, mock__get_function_url: MagicMock, mock_get_standard_token: MagicMock,
                                  mock_videos_missing_req_cls: MagicMock) -> None:
        mock__get_function_url.return_value = mock_url = "url"
        mock_get_standard_token.return_value = mock_token = "abc"
        mock_videos_missing_req_cls.return_value = mock_data = "xyz"

        test_video1 = lms_api.VideoModel(hash="foo", file_ext=".mp4")
        test_video2 = lms_api.VideoModel(hash="bar", file_ext=".mp4")
        mock_client = MagicMock(request=AsyncMock(side_effect=lms_api.HTTPClientError))

        with self.assertRaises(lms_api.LMSInterfaceError):
            await lms_api.LMS("abc").videos_missing(test_video1, test_video2, client=mock_client)
        mock__get_function_url.assert_called_once_with("videos_missing")
        mock_get_standard_token.assert_called_once_with()
        mock_videos_missing_req_cls.assert_called_once_with(videos=[test_video1, test_video2])
        mock_client.request.assert_awaited_once_with(
            "POST",
            mock_url,
            mock_token,
            data=mock_data,
            return_model=lms_api.VideosMissingResponse,
            timeout=30,
            external=True,
        )

        mock_client.request.reset_mock()
        mock_client.request.side_effect = None
        mock_client.request.return_value = 400, None
        mock__get_function_url.reset_mock()
        mock_get_standard_token.reset_mock()
        mock_videos_missing_req_cls.reset_mock()

        with self.assertRaises(lms_api.LMSInterfaceError):
            await lms_api.LMS("abc").videos_missing(test_video1, test_video2, client=mock_client)
        mock__get_function_url.assert_called_once_with("videos_missing")
        mock_get_standard_token.assert_called_once_with()
        mock_videos_missing_req_cls.assert_called_once_with(videos=[test_video1, test_video2])
        mock_client.request.assert_awaited_once_with(
            "POST",
            mock_url,
            mock_token,
            data=mock_data,
            return_model=lms_api.VideosMissingResponse,
            timeout=30,
            external=True,
        )

        mock_client.request.reset_mock()
        mock_response = MagicMock()
        mock_client.request.return_value = 200, mock_response
        mock__get_function_url.reset_mock()
        mock_get_standard_token.reset_mock()
        mock_videos_missing_req_cls.reset_mock()

        output = await lms_api.LMS("abc").videos_missing(test_video1, test_video2, client=mock_client)
        self.assertIs(mock_response, output)
        mock__get_function_url.assert_called_once_with("videos_missing")
        mock_get_standard_token.assert_called_once_with()
        mock_videos_missing_req_cls.assert_called_once_with(videos=[test_video1, test_video2])
        mock_client.request.assert_awaited_once_with(
            "POST",
            mock_url,
            mock_token,
            data=mock_data,
            return_model=lms_api.VideosMissingResponse,
            timeout=30,
            external=True,
        )

    @patch.object(lms_api.LMS, "VIDEOS_CHECK_MAX_BATCH_SIZE", new=2)
    @patch.object(lms_api, "VideoModel")
    @patch.object(lms_api.LMS, "iter_all")
    async def test_filter_orphaned_videos(self, mock_iter_all: MagicMock, mock_video_model_cls: MagicMock) -> None:
        mock_video1 = MagicMock(hash="foo", file_ext=".mp4")
        mock_video2 = MagicMock(hash="bar", file_ext=".mp4")
        mock_video3 = MagicMock(hash="baz", file_ext=".mp4")
        mock_client = MagicMock()
        test_origin = "https://spam.eggs"
        lms1 = MagicMock(api_url=test_origin + "/something", videos_missing=AsyncMock())
        lms2 = MagicMock(api_url="example.com/other", videos_missing=AsyncMock())

        mock_iter_all.return_value = [lms1, lms2]
        mock_video_model_cls.from_orm = lambda x: x

        output = await lms_api.LMS.filter_orphaned_videos(
            mock_video1, mock_video2, mock_video3, client=mock_client, origin=test_origin
        )
        self.assertListEqual([], output)

        mock_iter_all.assert_has_calls([call(), call()])
        lms1.videos_missing.assert_not_called()
        lms2.videos_missing.assert_has_awaits([
            call(mock_video1, mock_video2, client=mock_client),
            call(mock_video3, client=mock_client),
        ])

        mock_iter_all.reset_mock()
        lms2.videos_missing.reset_mock()
        lms2.videos_missing.side_effect = lms_api.LMSInterfaceError

        with self.assertRaises(lms_api.LMSInterfaceError):
            await lms_api.LMS.filter_orphaned_videos(
                mock_video1, mock_video2, mock_video3, client=mock_client, origin=test_origin
            )

        mock_iter_all.assert_called_once_with()
        lms1.videos_missing.assert_not_called()
        lms2.videos_missing.assert_awaited_once_with(mock_video1, mock_video2, client=mock_client)
