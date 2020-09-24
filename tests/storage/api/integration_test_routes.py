import json
import os
import unittest
from unittest.mock import patch

from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from tests.base import load_basic_config_from
from videbo.storage.api.routes import routes
from videbo.auth import external_jwt_encode
from videbo import settings


TESTED_MODULE_PATH = 'videbo.storage.api.routes'
STORAGE_SETTINGS_PATH = TESTED_MODULE_PATH + '.storage_settings'


class RoutesTestCase(AioHTTPTestCase):

    CONFIG_FILE_NAME = 'config.ini'

    TEST_VIDEO_FILE_PATH = 'tests/test_video.mp4'  # relative to project top dir.
    _TEST_VIDEO_EXISTS = os.path.isfile(TEST_VIDEO_FILE_PATH)
    _SKIP_REASON_NO_TEST_VIDEO = "No test video file found"

    @classmethod
    def setUpClass(cls) -> None:
        """
        To provide access to to basic configuration during testing.
        """
        if not settings.config.sections():
            load_basic_config_from(cls.CONFIG_FILE_NAME)

    def setUp(self) -> None:
        self.settings_patcher = patch(STORAGE_SETTINGS_PATH)
        self.mock_settings = self.settings_patcher.start()
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self.settings_patcher.stop()

    async def get_application(self):
        app = web.Application()
        app.add_routes(routes)
        return app

    @unittest_run_loop
    async def test_get_max_size(self):
        method, url = 'GET', '/api/upload/maxsize'
        test_value = 12345
        expected_response_text = json.dumps({'max_size': test_value})
        self.mock_settings.max_file_size_mb = test_value
        resp = await self.client.request(method, url)
        self.assertEqual(resp.status, 200)
        self.assertEqual(await resp.text(), expected_response_text)

    @unittest.skipUnless(_TEST_VIDEO_EXISTS, _SKIP_REASON_NO_TEST_VIDEO)
    def test_upload_file(self):
        pass
