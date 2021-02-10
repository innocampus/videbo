import json
import os
import unittest
from io import StringIO, BytesIO
from pathlib import Path
from hashlib import md5
import asyncio

from aiohttp import web, FormData
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from tests.base import load_basic_config_from
from videbo.storage.api.routes import routes
from videbo.storage.api.models import FileType, UploadFileJWTData, SaveFileJWTData, DeleteFileJWTData
from videbo.storage import storage_settings
from videbo.auth import external_jwt_encode, external_jwt_decode
from videbo.misc import rel_path
from videbo import settings


MEGA = 1024 * 1024
AUTHORIZATION, BEARER_PREFIX = 'Authorization', 'Bearer '
CONTENT_TYPE = 'Content-Type'


class Base(AioHTTPTestCase):

    CONFIG_FILE_NAME = 'config.ini'
    THUMBNAIL_EXT = 'jpg'

    # These can be adjusted as necessary to point to a test video file:
    TEST_VIDEO_FILE_DIR = Path(settings.topdir, 'tests')
    TEST_VIDEO_FILE_NAME = 'test_video.mp4'
    TEST_VIDEO_FILE_EXT = 'mp4'

    test_video_file_path = Path(TEST_VIDEO_FILE_DIR, TEST_VIDEO_FILE_NAME)
    test_vid_exists = test_video_file_path.is_file()
    skip_reason_no_test_vid = "No test video file found"
    test_vid_size_mb = os.path.getsize(test_video_file_path) / MEGA if test_vid_exists else None

    @classmethod
    def setUpClass(cls) -> None:
        """
        To provide access to to basic configuration during testing.
        """
        if not settings.config.sections():
            load_basic_config_from(cls.CONFIG_FILE_NAME)
        storage_settings.load()

    async def get_application(self):
        app = web.Application()
        app.add_routes(routes)
        return app

    def setUp(self) -> None:
        """
        We found the need to override this method to avoid the re-creation and destruction of the event loop.
        The original methods caused problems during testing.
        This implementation simply hands off loop management to the asyncio backend.
        """
        self.loop = asyncio.get_event_loop()
        self.setup_app_server_client()
        self.loop.run_until_complete(self.client.start_server())
        self.loop.run_until_complete(self.setUpAsync())

    def setup_app_server_client(self) -> None:
        self.app = self.loop.run_until_complete(self.get_application())
        self.server = self.loop.run_until_complete(self.get_server(self.app))
        self.client = self.loop.run_until_complete(self.get_client(self.server))

    def tearDown(self) -> None:
        self.loop.run_until_complete(self.tearDownAsync())
        self.loop.run_until_complete(self.client.close())

    @staticmethod
    def get_request_file_headers(file_hash: str, file_ext: str) -> dict:
        jwt_dict = {
            'type': FileType.VIDEO.value,
            'hash': file_hash,
            'file_ext': file_ext,
            'rid': '1',
            'role': 'client',
            'exp': 0,
            'iss': 0
        }
        return {AUTHORIZATION: BEARER_PREFIX + external_jwt_encode(jwt_dict)}


class RoutesIntegrationTestCaseFail(Base):
    INVALID_EXT = 'm4v'

    @unittest.skipUnless(Base.test_vid_exists, Base.skip_reason_no_test_vid)
    @unittest_run_loop
    async def test_upload_file(self):
        method, url, headers, payload = 'POST', '/api/upload/file', {}, {}

        # Without JWT:
        resp = await self.client.request(method, url, data=payload, headers=headers)
        self.assertEqual(401, resp.status)

        # With invalid content-type:
        jwt_data = UploadFileJWTData(is_allowed_to_upload_file=False, role='client', exp=0, iss=0)
        headers[AUTHORIZATION] = BEARER_PREFIX + external_jwt_encode(jwt_data)
        headers[CONTENT_TYPE] = 'something invalid'
        resp = await self.client.request(method, url, data=payload, headers=headers)
        self.assertEqual(406, resp.status)

        # Without upload permission:
        jwt_data = UploadFileJWTData(is_allowed_to_upload_file=False, role='client', exp=0, iss=0)
        headers[AUTHORIZATION] = BEARER_PREFIX + external_jwt_encode(jwt_data)
        headers[CONTENT_TYPE] = 'multipart/form-data'
        resp = await self.client.request(method, url, data=payload, headers=headers)
        self.assertEqual(403, resp.status)

        # No field named `video` in payload:
        del headers[CONTENT_TYPE]
        jwt_data = UploadFileJWTData(is_allowed_to_upload_file=True, role='client', exp=0, iss=0)
        headers[AUTHORIZATION] = BEARER_PREFIX + external_jwt_encode(jwt_data)
        with open(self.test_video_file_path, 'rb') as f:
            # We pass an actual file in the payload to have a well-formed multipart constructed automatically
            payload = {'foo': f, 'bar': 'xyz'}
            resp = await self.client.request(method, url, data=payload, headers=headers)
        self.assertEqual(415, resp.status)

        # Faulty file extension upload:
        payload = FormData()
        payload.add_field('video', StringIO(),
                          filename='test_video.' + self.INVALID_EXT,
                          content_type='video/' + self.INVALID_EXT)
        resp = await self.client.request(method, url, data=payload, headers=headers)
        self.assertEqual(415, resp.status)

        # File too big:
        storage_settings.max_file_size_mb = self.test_vid_size_mb - 1
        with open(self.test_video_file_path, 'rb') as f:
            payload = FormData()
            payload.add_field('video', f,
                              filename=self.TEST_VIDEO_FILE_NAME,
                              content_type='video/' + self.TEST_VIDEO_FILE_EXT)
            resp = await self.client.request(method, url, data=payload, headers=headers)
        self.assertEqual(413, resp.status)

    @unittest_run_loop
    async def test_save_file(self):
        method, url, headers = 'GET', '/api/save/file/' + '0' * 64 + '.foo', {}

        # Without JWT:
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(401, resp.status)

        # With not enough privileges:
        jwt_data = SaveFileJWTData(is_allowed_to_save_file=True, role='client', exp=0, iss=0)
        headers[AUTHORIZATION] = BEARER_PREFIX + external_jwt_encode(jwt_data)
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(401, resp.status)

        # Without save permission:
        jwt_data = SaveFileJWTData(is_allowed_to_save_file=False, role='lms', exp=0, iss=0)
        headers[AUTHORIZATION] = BEARER_PREFIX + external_jwt_encode(jwt_data)
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(403, resp.status)

        # No such file:
        jwt_data = SaveFileJWTData(is_allowed_to_save_file=True, role='lms', exp=0, iss=0)
        headers[AUTHORIZATION] = BEARER_PREFIX + external_jwt_encode(jwt_data)
        resp = await self.client.request(method, url, headers=headers)
        resp_data = await resp.json()
        self.assertEqual(404, resp.status)
        self.assertEqual('error', resp_data['status'])

    @unittest_run_loop
    async def test_delete_file(self):
        method, url, headers = 'DELETE', '/api/file/' + '0' * 64 + '.foo', {}

        # Without JWT:
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(401, resp.status)

        # With not enough privileges:
        jwt_data = DeleteFileJWTData(is_allowed_to_delete_file=True, role='client', exp=0, iss=0)
        headers[AUTHORIZATION] = BEARER_PREFIX + external_jwt_encode(jwt_data)
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(401, resp.status)

        # Without delete permission:
        jwt_data = DeleteFileJWTData(is_allowed_to_delete_file=False, role='lms', exp=0, iss=0)
        headers[AUTHORIZATION] = BEARER_PREFIX + external_jwt_encode(jwt_data)
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(403, resp.status)


class RoutesIntegrationTestCaseCorrect(Base):

    @unittest_run_loop
    async def test_get_max_size(self):
        method, url = 'GET', '/api/upload/maxsize'
        expected_response_text = json.dumps({'max_size': storage_settings.max_file_size_mb})
        resp = await self.client.request(method, url)
        self.assertEqual(resp.status, 200)
        self.assertEqual(await resp.text(), expected_response_text)

    @unittest.skipUnless(Base.test_vid_exists, Base.skip_reason_no_test_vid)
    @unittest_run_loop
    async def test_real_file_cycle(self):
        temp_dir = Path(storage_settings.files_path, 'temp')
        perm_dir = Path(storage_settings.files_path, 'storage')

        # Upload:
        method, url = 'POST', '/api/upload/file'
        jwt_data = UploadFileJWTData(is_allowed_to_upload_file=True, role='client', exp=0, iss=0)
        headers = {AUTHORIZATION: BEARER_PREFIX + external_jwt_encode(jwt_data)}
        storage_settings.max_file_size_mb = self.test_vid_size_mb + 1
        with open(self.test_video_file_path, 'rb') as f:
            payload = FormData()
            payload.add_field('video', f,
                              filename=self.TEST_VIDEO_FILE_NAME,
                              content_type='video/' + self.TEST_VIDEO_FILE_EXT)
            resp = await self.client.request(method, url, data=payload, headers=headers)
        self.assertEqual(200, resp.status)
        resp_data = json.loads(await resp.text())
        data = external_jwt_decode(resp_data['jwt'])
        hashed_video_file_name = data['hash'] + data['file_ext']
        thumb_count = int(data['thumbnails_available'])
        # Check that video and thumbnail files were created:
        self.assertTrue(Path(temp_dir, hashed_video_file_name).is_file())
        for i in range(thumb_count):
            self.assertTrue(Path(temp_dir, data['hash'] + f'_{i}.{self.THUMBNAIL_EXT}').is_file())

        # Save:
        method, url = 'GET', '/api/save/file/' + hashed_video_file_name
        jwt_data = SaveFileJWTData(is_allowed_to_save_file=True, role='lms', exp=0, iss=0)
        headers = {AUTHORIZATION: BEARER_PREFIX + external_jwt_encode(jwt_data)}
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(200, resp.status)
        # Check that video and thumbnail files were moved:
        self.assertTrue(Path(perm_dir, rel_path(hashed_video_file_name)).is_file())
        self.assertFalse(Path(temp_dir, hashed_video_file_name).exists())
        for i in range(thumb_count):
            self.assertTrue(Path(perm_dir, rel_path(data['hash'] + f'_{i}.{self.THUMBNAIL_EXT}')).is_file())
            self.assertFalse(Path(temp_dir, data['hash'] + f'_{i}.{self.THUMBNAIL_EXT}').exists())

        # Download (without X-Accel):
        storage_settings.nginx_x_accel_location = ''
        method, url = 'GET', '/file'
        headers = self.get_request_file_headers(data['hash'], data['file_ext'])
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(200, resp.status)
        # Check integrity (compare hash of downloaded bytes with original test file):
        body = BytesIO()
        body.write(await resp.content.read())
        with open(self.test_video_file_path, 'rb') as f:
            self.assertEqual(md5(f.read()).digest(), md5(body.getvalue()).digest())

        # Download (with X-Accel):
        storage_settings.nginx_x_accel_location = mock_location = 'foo/bar'
        storage_settings.nginx_x_accel_limit_rate_mbit = test_limit_rate = 4.20
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(200, resp.status)
        expected_redirect = str(Path(mock_location, rel_path(hashed_video_file_name)))
        expected_limit_rate = str(int(test_limit_rate * MEGA / 8))
        self.assertEqual(expected_redirect, resp.headers['X-Accel-Redirect'])
        self.assertEqual(expected_limit_rate, resp.headers['X-Accel-Limit-Rate'])

        # Delete:
        method, url = 'DELETE', '/api/file/' + hashed_video_file_name
        jwt_data = DeleteFileJWTData(is_allowed_to_delete_file=True, role='lms', exp=0, iss=0)
        headers = {AUTHORIZATION: BEARER_PREFIX + external_jwt_encode(jwt_data)}
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(200, resp.status)
        # TODO: Find a more elegant way to prevent a race condition here.
        await asyncio.sleep(1)
        self.assertFalse(Path(perm_dir, rel_path(hashed_video_file_name)).exists())
        for i in range(thumb_count):
            self.assertFalse(Path(perm_dir, rel_path(data['hash'] + f'_{i}.{self.THUMBNAIL_EXT}')).exists())

        # Request again, expected 404:
        method, url = 'GET', '/file'
        headers = self.get_request_file_headers(data['hash'], data['file_ext'])
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(404, resp.status)
