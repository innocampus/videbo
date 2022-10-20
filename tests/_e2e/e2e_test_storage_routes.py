import asyncio
import json
import unittest
from io import StringIO, BytesIO
from pathlib import Path
from hashlib import md5

from aiohttp import FormData

from videbo import settings
from videbo.misc import MEGA
from videbo.misc.functions import rel_path
from videbo.models import Role, TokenIssuer
from videbo.storage.api.models import (
    UploadFileJWTData,
    SaveFileJWTData,
    DeleteFileJWTData,
    FileUploadedResponseJWT,
)
from videbo.storage.api.routes import get_expiration_time
from .base import BaseE2ETestCase


settings.distribution.static_node_base_urls = []  # prevent adding nodes (and sending status requests to them)

CONTENT_TYPE = 'Content-Type'


class RoutesIntegrationTestCaseFail(BaseE2ETestCase):
    INVALID_EXT = 'm4v'

    @unittest.skipUnless(BaseE2ETestCase.test_vid_exists, BaseE2ETestCase.SKIP_REASON_NO_TEST_VID)
    async def test_upload_file(self):
        method, url, headers, payload = 'POST', '/api/upload/file', {}, {}

        # Without JWT:
        resp = await self.client.request(method, url, data=payload, headers=headers)
        self.assertEqual(400, resp.status)

        # With invalid content-type:
        jwt_data = UploadFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.client,
            is_allowed_to_upload_file=True
        )
        headers |= self.get_auth_header(jwt_data.encode())
        headers[CONTENT_TYPE] = 'something invalid'
        resp = await self.client.request(method, url, data=payload, headers=headers)
        self.assertEqual(406, resp.status)

        # Without upload permission:
        jwt_data = UploadFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.client,
            is_allowed_to_upload_file=False
        )
        headers |= self.get_auth_header(jwt_data.encode())
        headers[CONTENT_TYPE] = 'multipart/form-data'
        resp = await self.client.request(method, url, data=payload, headers=headers)
        self.assertEqual(403, resp.status)

        # No field named `video` in payload:
        del headers[CONTENT_TYPE]
        jwt_data = UploadFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.client,
            is_allowed_to_upload_file=True
        )
        headers |= self.get_auth_header(jwt_data.encode())
        with open(settings.test_video_file_path, 'rb') as f:
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
        settings.max_file_size_mb = self.test_vid_size_mb - 1
        with open(settings.test_video_file_path, 'rb') as f:
            payload = FormData()
            payload.add_field('video', f,
                              filename=settings.test_video_file_path.name,
                              content_type='video/' + self.test_vid_file_ext)
            resp = await self.client.request(method, url, data=payload, headers=headers)
        self.assertEqual(413, resp.status)

    async def test_save_file(self):
        method, url, headers = 'GET', '/api/save/file/' + '0' * 64 + '.foo', {}

        # Without JWT:
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(400, resp.status)

        # With not enough privileges:
        jwt_data = SaveFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.client,
            is_allowed_to_save_file=True
        )
        headers |= self.get_auth_header(jwt_data.encode())
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(401, resp.status)

        # Without save permission:
        jwt_data = SaveFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.lms,
            is_allowed_to_save_file=False
        )
        headers |= self.get_auth_header(jwt_data.encode())
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(403, resp.status)

        # No such file:
        jwt_data = SaveFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.lms,
            is_allowed_to_save_file=True
        )
        headers |= self.get_auth_header(jwt_data.encode())
        resp = await self.client.request(method, url, headers=headers)
        resp_data = await resp.json()
        self.assertEqual(404, resp.status)
        self.assertEqual('error', resp_data['status'])

    async def test_delete_file(self):
        method, url, headers = 'DELETE', '/api/file/' + '0' * 64 + '.foo', {}

        # Without JWT:
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(400, resp.status)

        # With not enough privileges:
        jwt_data = DeleteFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.client,
            is_allowed_to_delete_file=True
        )
        headers |= self.get_auth_header(jwt_data.encode())
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(401, resp.status)

        # Without delete permission:
        jwt_data = DeleteFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.lms,
            is_allowed_to_delete_file=False
        )
        headers |= self.get_auth_header(jwt_data.encode())
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(403, resp.status)


class RoutesIntegrationTestCaseCorrect(BaseE2ETestCase):
    async def test_get_max_size(self):
        method, url = 'GET', '/api/upload/maxsize'
        expected_response_text = json.dumps({'max_size': settings.max_file_size_mb})
        resp = await self.client.request(method, url)
        self.assertEqual(resp.status, 200)
        self.assertEqual(await resp.text(), expected_response_text)

    @unittest.skipUnless(BaseE2ETestCase.test_vid_exists, BaseE2ETestCase.SKIP_REASON_NO_TEST_VID)
    async def test_real_file_cycle(self):
        temp_dir = Path(settings.files_path, 'temp')
        perm_dir = Path(settings.files_path, 'storage')

        # Upload:
        method, url = 'POST', '/api/upload/file'
        jwt_data = UploadFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.client,
            is_allowed_to_upload_file=True
        )
        headers = self.get_auth_header(jwt_data.encode())
        settings.max_file_size_mb = self.test_vid_size_mb + 1
        with open(settings.test_video_file_path, 'rb') as f:
            payload = FormData()
            payload.add_field('video', f,
                              filename=settings.test_video_file_path.name,
                              content_type='video/' + self.test_vid_file_ext)
            resp = await self.client.request(method, url, data=payload, headers=headers)
        self.assertEqual(200, resp.status)
        resp_data = json.loads(await resp.text())
        data = FileUploadedResponseJWT.decode(resp_data['jwt'])
        assert isinstance(data, FileUploadedResponseJWT)
        hashed_video_file_name = data.hash + data.file_ext
        thumb_count = int(data.thumbnails_available)
        # Check that video and thumbnail files were created:
        self.assertTrue(Path(temp_dir, hashed_video_file_name).is_file())
        for i in range(thumb_count):
            self.assertTrue(Path(temp_dir, data.hash + f'_{i}.{self.THUMBNAIL_EXT}').is_file())

        # Save:
        method, url = 'GET', '/api/save/file/' + hashed_video_file_name
        jwt_data = SaveFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.lms,
            is_allowed_to_save_file=True
        )
        headers = self.get_auth_header(jwt_data.encode())
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(200, resp.status)
        # Check that video and thumbnail files were moved:
        self.assertTrue(Path(perm_dir, rel_path(hashed_video_file_name)).is_file())
        self.assertFalse(Path(temp_dir, hashed_video_file_name).exists())
        for i in range(thumb_count):
            self.assertTrue(Path(perm_dir, rel_path(data.hash + f'_{i}.{self.THUMBNAIL_EXT}')).is_file())
            self.assertFalse(Path(temp_dir, data.hash + f'_{i}.{self.THUMBNAIL_EXT}').exists())

        # Download (without X-Accel):
        settings.webserver.x_accel_location = ''
        method, url = 'GET', '/file'
        jwt_data = self.get_request_file_jwt_data(data.hash, data.file_ext)
        headers = self.get_auth_header(jwt_data.encode())
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(200, resp.status)
        # Check integrity (compare hash of downloaded bytes with original test file):
        body = BytesIO()
        body.write(await resp.content.read())
        with open(settings.test_video_file_path, 'rb') as f:
            self.assertEqual(md5(f.read()).digest(), md5(body.getvalue()).digest())

        # Download (with X-Accel):
        settings.webserver.x_accel_location = mock_location = 'foo/bar'
        settings.webserver.x_accel_limit_rate_mbit = test_limit_rate = 4.20
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(200, resp.status)
        expected_redirect = str(Path(mock_location, rel_path(hashed_video_file_name)))
        expected_limit_rate = str(int(test_limit_rate * MEGA / 8))
        self.assertEqual(expected_redirect, resp.headers['X-Accel-Redirect'])
        self.assertEqual(expected_limit_rate, resp.headers['X-Accel-Limit-Rate'])

        # Delete:
        method, url = 'DELETE', '/api/file/' + hashed_video_file_name
        jwt_data = DeleteFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.lms,
            is_allowed_to_delete_file=True
        )
        headers = self.get_auth_header(jwt_data.encode())
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(200, resp.status)
        # TODO: Find a more elegant way to prevent a race condition here.
        await asyncio.sleep(1)
        self.assertFalse(Path(perm_dir, rel_path(hashed_video_file_name)).exists())
        for i in range(thumb_count):
            self.assertFalse(Path(perm_dir, rel_path(data.hash + f'_{i}.{self.THUMBNAIL_EXT}')).exists())

        # Request again, expected 404:
        method, url = 'GET', '/file'
        jwt_data = self.get_request_file_jwt_data(data.hash, data.file_ext)
        headers = self.get_auth_header(jwt_data.encode())
        resp = await self.client.request(method, url, headers=headers)
        self.assertEqual(404, resp.status)
