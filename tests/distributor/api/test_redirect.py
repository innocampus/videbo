from unittest import TestCase
from unittest.mock import MagicMock, patch
from urllib.parse import quote, urlencode

from videbo.distributor.api import redirect


class RedirectToDistributorTestCase(TestCase):
    @patch.object(redirect, "extract_jwt_from_request")
    def test___init__(self, mock_extract_jwt_from_request: MagicMock) -> None:
        mock_extract_jwt_from_request.return_value = mock_jwt = "spam"
        mock_request = MagicMock()
        mock_request.query.getone.return_value = None
        url = "foo"
        mock_node = MagicMock(base_url=url)
        expected_data = {'jwt': mock_jwt}
        expected_url = f"{url}/file?{urlencode(expected_data)}"
        obj = redirect.RedirectToDistributor(
            mock_request,
            mock_node,
            MagicMock(),
            MagicMock(),
        )
        self.assertEqual(expected_url, obj.location)
        mock_extract_jwt_from_request.assert_called_once_with(mock_request)
        mock_request.query.getone.assert_called_once_with("downloadas", None)

        mock_extract_jwt_from_request.reset_mock()
        mock_request.reset_mock()

        mock_request.query.getone.return_value = download_as = "test 123 äöü"
        expected_data["downloadas"] = quote(download_as)
        expected_url = f"{url}/file?{urlencode(expected_data)}"
        obj = redirect.RedirectToDistributor(
            mock_request,
            mock_node,
            MagicMock(),
            MagicMock(),
        )
        self.assertEqual(expected_url, obj.location)
        mock_extract_jwt_from_request.assert_called_once_with(mock_request)
        mock_request.query.getone.assert_called_once_with("downloadas", None)
