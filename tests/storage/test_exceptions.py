from unittest import TestCase
from unittest.mock import MagicMock, patch

from aiohttp.web_exceptions import HTTPConflict, HTTPNotFound

from videbo.storage import exceptions


class UnknownDistURLTestCase(TestCase):
    def test___init__(self) -> None:
        url = "foo/bar"
        expected_text = f"Unknown distributor node `{url}`"
        exc = exceptions.UnknownDistURL(url)
        self.assertIsInstance(exc, HTTPNotFound)
        self.assertEqual(expected_text, exc.text)

        with patch.object(HTTPNotFound, "__init__") as mock_init:
            kw = {"a": 1, "b": 2}
            exc = exceptions.UnknownDistURL(url, **kw)
            mock_init.assert_called_once_with(exc, text=expected_text, **kw)


class DistNodeAlreadySetTestCase(TestCase):
    def test___init__(self) -> None:
        url = "foo/bar"
        enabled = True
        expected_text = f"Already enabled `{url}`"
        exc = exceptions.DistNodeAlreadySet(url, enabled)
        self.assertIsInstance(exc, HTTPConflict)
        self.assertEqual(expected_text, exc.text)

        enabled = False
        expected_text = f"Already disabled `{url}`"
        exc = exceptions.DistNodeAlreadySet(url, enabled)
        self.assertEqual(expected_text, exc.text)

        with patch.object(HTTPConflict, "__init__") as mock_init:
            kw = {"a": 1, "b": 2}
            exc = exceptions.DistNodeAlreadySet(url, enabled, **kw)
            mock_init.assert_called_once_with(exc, text=expected_text, **kw)


class DistNodeAlreadyEnabledTestCase(TestCase):
    @patch.object(exceptions.DistNodeAlreadySet, "__init__")
    def test___init__(self, mock_parent_init: MagicMock) -> None:
        url = "foo/bar"
        kw = {"a": 1, "b": 2}
        exceptions.DistNodeAlreadyEnabled(url, **kw)
        mock_parent_init.assert_called_once_with(url, True, **kw)


class DistNodeAlreadyDisabledTestCase(TestCase):
    @patch.object(exceptions.DistNodeAlreadySet, "__init__")
    def test___init__(self, mock_parent_init: MagicMock) -> None:
        url = "foo/bar"
        kw = {"a": 1, "b": 2}
        exceptions.DistNodeAlreadyDisabled(url, **kw)
        mock_parent_init.assert_called_once_with(url, False, **kw)
