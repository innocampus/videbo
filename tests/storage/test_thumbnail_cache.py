import logging
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from videbo.misc import MEGA
from videbo.storage import thumbnail_cache


thumbnail_settings = thumbnail_cache.settings.thumbnails


class ThumbnailCacheTestCase(IsolatedAsyncioTestCase):
    @patch.object(thumbnail_cache.BytesLimitLRU, "__init__")
    def test___init__(self, mock_super_init: MagicMock) -> None:
        max_ = 1
        other = ((Path("foo"), b"foo"), (Path("bar"), b"bar"))
        kwargs = {"x": b"x", "y": b"y"}
        with self.assertLogs(thumbnail_cache._log, logging.WARNING):
            thumbnail_cache.ThumbnailCache(max_, other=other, **kwargs)
        mock_super_init.assert_called_once_with(max_, other=other, **kwargs)
        mock_super_init.reset_mock()

        thumbnail_cache.ThumbnailCache(other=other, **kwargs)
        mock_super_init.assert_called_once_with(0, other=other, **kwargs)

    def test_max_bytes(self) -> None:
        obj = thumbnail_cache.ThumbnailCache()
        obj._max_bytes = 1
        self.assertEqual(1, obj.max_bytes)
        obj._max_bytes = 0
        expected_max = int(thumbnail_settings.cache_max_mb * MEGA)
        self.assertEqual(expected_max, obj.max_bytes)

    @patch.object(thumbnail_cache, "settings")
    @patch.object(thumbnail_cache.BytesLimitLRU, "__setitem__")
    @patch.object(thumbnail_cache, "run_in_default_executor")
    @patch.object(thumbnail_cache.BytesLimitLRU, "__getitem__")
    @patch.object(thumbnail_cache.BytesLimitLRU, "__contains__")
    async def test_get_and_update(
        self,
        mock___contains__: MagicMock,
        mock___getitem__: MagicMock,
        mock_run_in_default_executor: AsyncMock,
        mock___setitem__: MagicMock,
        mock_settings: MagicMock,
    ) -> None:
        mock___contains__.return_value = True
        mock___getitem__.return_value = expected_output = object()
        path = MagicMock()
        obj = thumbnail_cache.ThumbnailCache()

        output = await obj.get_and_update(path)
        self.assertEqual(expected_output, output)
        mock___getitem__.assert_called_once_with(path)
        mock_run_in_default_executor.assert_not_called()
        mock___setitem__.assert_not_called()

        mock___getitem__.reset_mock()

        mock___contains__.return_value = False
        mock_settings.thumbnails.cache_max_mb = 0
        mock_run_in_default_executor.return_value = expected_output = object()

        output = await obj.get_and_update(path)
        self.assertEqual(expected_output, output)
        mock___getitem__.assert_not_called()
        mock_run_in_default_executor.assert_called_once_with(path.read_bytes)
        mock___setitem__.assert_not_called()

        mock_run_in_default_executor.reset_mock()

        mock_settings.thumbnails.cache_max_mb = 1

        output = await obj.get_and_update(path)
        self.assertEqual(expected_output, output)
        mock___getitem__.assert_not_called()
        mock_run_in_default_executor.assert_called_once_with(path.read_bytes)
        mock___setitem__.assert_called_once_with(path, expected_output)
