from asyncio.subprocess import create_subprocess_exec
from asyncio.tasks import sleep
from filecmp import cmp
from hashlib import md5
from pathlib import Path
from unittest import SkipTest

from aiohttp.client import ClientSession, ClientConnectionError
from aiohttp.formdata import FormData

from videbo.config import Settings, CONFIG_FILE_PATHS_PARAM
from videbo.misc.functions import rel_path
from videbo.models import Role, TokenIssuer
from videbo.storage.api.models import (
    DeleteFileJWTData,
    FileUploadedResponseJWT,
    SaveFileJWTData,
    UploadFileJWTData,
)
from videbo.storage.api.routes import get_expiration_time
from tests._e2e.base import BaseE2ETestCase

THIS_DIR = Path(__file__).parent
# Read specific test node configs from here:
TEST_CONFIG_STORAGE = Path(THIS_DIR, "config_storage.yaml")
TEST_CONFIG_DIST = Path(THIS_DIR, "config_dist.yaml")
# Temporarily pipe storage and distributor logs here:
LOG_PATH_STORAGE = Path("test_storage.log")
LOG_PATH_DIST = Path("test_dist.log")


class TwoNodesTestCase(BaseE2ETestCase):
    """
    Performs one monolithic test with two nodes running in subprocesses.

    Dedicated config files are loaded for the test nodes and equivalent
    settings objects are created within the test case.

    Output from both test nodes is piped into separate log files.
    If test is passed without problems, they are deleted.
    """

    def setUp(self) -> None:
        if not self.test_vid_exists:
            raise SkipTest(self.SKIP_REASON_NO_TEST_VID)

        # Same config files as used by the nodes:
        self.test_settings_storage = Settings(
            **{CONFIG_FILE_PATHS_PARAM: [TEST_CONFIG_STORAGE]}
        )
        self.test_settings_dist = Settings(
            **{CONFIG_FILE_PATHS_PARAM: [TEST_CONFIG_DIST]}
        )

        # To be safe, ensure the files paths do not yet exist:
        if self.test_settings_storage.files_path.exists():
            raise RuntimeError(
                f"Test storage files path already exists: "
                f"'{self.test_settings_storage.files_path.resolve()}'"
            )
        if self.test_settings_dist.files_path.exists():
            raise RuntimeError(
                f"Test distributor files path already exists: "
                f"'{self.test_settings_dist.files_path.resolve()}'"
            )

        # Calculate MD5 hash of the test video file in advance:
        with self.test_settings_storage.test_video_file_path.open("rb") as f:
            self.test_file_md5 = md5(f.read()).digest()

        # Open log files for subprocesses:
        self.fd_log_storage = LOG_PATH_STORAGE.open("w")
        self.fd_log_dist = LOG_PATH_DIST.open("w")

        super().setUp()

    async def asyncSetUp(self) -> None:
        # Start distributor node subprocess:
        args_dist = f"-m videbo -c {TEST_CONFIG_DIST.resolve()} distributor"
        self.proc_dist = await create_subprocess_exec(
            "python",
            *args_dist.split(),
            stdout=self.fd_log_dist,
            stderr=self.fd_log_dist,
        )
        await sleep(1)

        # Start storage node subprocess:
        args_storage = f"-m videbo -c {TEST_CONFIG_STORAGE.resolve()} storage"
        self.proc_storage = await create_subprocess_exec(
            "python",
            *args_storage.split(),
            stdout=self.fd_log_storage,
            stderr=self.fd_log_storage,
        )

        # Start HTTP client session:
        self.session = ClientSession()

        await super().asyncSetUp()

    async def asyncTearDown(self) -> None:
        # Close HTTP client session:
        await self.session.close()
        # Stop storage & distributor node subprocesses:
        self.proc_storage.terminate()
        self.proc_dist.terminate()

        await super().asyncTearDown()

    def tearDown(self) -> None:
        # Close log files:
        self.fd_log_storage.close()
        self.fd_log_dist.close()

        # Delete log files, if everything is fine
        if self.all_tests_passed():
            LOG_PATH_STORAGE.unlink()
            LOG_PATH_DIST.unlink()

        # Delete test storage/distributor files directories:
        for dir_path in (
                self.test_settings_storage.files_path,
                self.test_settings_dist.files_path,
        ):
            self._safe_rmtree(dir_path)

        super().tearDown()

    async def _get_max_size(
        self,
        attempts: int = 3,
        seconds_between: float = 1.0,
    ) -> float:
        """
        Attempts to fetch the `max_size` from the storage API.

        Retries on `ClientConnectionError` (assuming node is not up yet).
        If all attempts fail, re-raises the last error.
        """
        url = self.test_settings_storage.make_url("/api/upload/maxsize")
        i = 0
        while True:
            i += 1
            await sleep(seconds_between)
            try:
                async with self.session.get(url) as resp:
                    data = await resp.json()
                    return data["max_size"]
            except ClientConnectionError:
                if i >= attempts:
                    raise

    async def test_monolith(self) -> None:
        """
        Performs the most relevant interactions and verifies the outcomes.

        Actions:
        0. Try to fetch the maximum video file size.
        1. Upload test video to storage
        2. Request saving the uploaded video
        3.a) Download the video from storage
        3.b) Trigger copying from storage to distributor
        4. Request video from storage again to get redirected
        5. Download video from distributor
        6. Delete video on storage (and thereby distributor)
        7. Try to request deleted video
        """
        # If this returns without error, we can assume the nodes are running:
        max_size = await self._get_max_size()
        self.assertEqual(self.test_settings_storage.max_file_size_mb, max_size)

        temp_dir = Path(self.test_settings_storage.files_path, "temp")
        perm_dir = Path(self.test_settings_storage.files_path, "storage")
        dist_dir = Path(self.test_settings_dist.files_path)
        key = self.test_settings_storage.external_api_secret

        ##########
        # Upload #

        # Temp. dir should not contain files yet:
        self.assertFalse(any(p.is_file() for p in temp_dir.glob("**/*")))

        method = "POST"
        url = self.test_settings_storage.make_url("/api/upload/file")
        jwt_data = UploadFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.client,
            is_allowed_to_upload_file=True
        )
        headers = self.get_auth_header(jwt_data.encode(key=key))
        with self.test_settings_storage.test_video_file_path.open("rb") as f:
            # Construct file data:
            payload = FormData()
            payload.add_field(
                "video",
                f,
                content_type="video/" + self.test_vid_file_ext,
                filename=self.test_settings_storage.test_video_file_path.name,
            )
            # Perform the request to the storage node:
            async with self.session.request(
                    method,
                    url,
                    data=payload,
                    headers=headers,
            ) as response:
                self.assertEqual(200, response.status)
                # Load the response data:
                resp_data = await response.json()
        data = FileUploadedResponseJWT.decode(resp_data["jwt"], key=key)
        hashed_video_file_name = data.hash + data.file_ext
        thumbnail_file_names = [
            f"{data.hash}_{i}.{self.THUMBNAIL_EXT}"
            for i in range(data.thumbnails_available)
        ]
        # Check that the test video was uploaded to the `temp_dir`:
        self.assertTrue(cmp(
            self.test_settings_storage.test_video_file_path,
            Path(temp_dir, hashed_video_file_name),
        ))
        # Check that thumbnails were created in the `temp_dir`:
        for name in thumbnail_file_names:
            self.assertTrue(Path(temp_dir, name).is_file())

        ########
        # Save #

        method = "GET"
        url = self.test_settings_storage.make_url("/api/save/file/" + hashed_video_file_name)
        jwt_data = SaveFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.lms,
            is_allowed_to_save_file=True
        )
        headers = self.get_auth_header(jwt_data.encode(key=key))
        # Perform the request to the storage node:
        async with self.session.request(
                method,
                url,
                headers=headers,
        ) as response:
            self.assertEqual(200, response.status)
            self.assertDictEqual({"status": "ok"}, await response.json())
        # Check that the test video was put into the `perm_dir`:
        self.assertTrue(cmp(
            self.test_settings_storage.test_video_file_path,
            Path(perm_dir, rel_path(hashed_video_file_name)),
        ))
        # Check that it does no longer exist in the `temp_dir`:
        self.assertFalse(Path(temp_dir, hashed_video_file_name).exists())
        # Check the same for the thumbnails
        for name in thumbnail_file_names:
            self.assertTrue(Path(perm_dir, rel_path(name)).is_file())
            self.assertFalse(Path(temp_dir, name).is_file())

        ########################################
        # Download from Storage & copy to Dist #

        method = "GET"
        jwt_data = self.get_request_file_jwt_data(data.hash, data.file_ext)
        request_file_token = jwt_data.encode(key=key)
        url = self.test_settings_storage.make_url("/file?jwt=" + request_file_token)
        headers = self.get_auth_header(request_file_token)
        # Perform the request to the storage node and download content:
        async with self.session.request(
                method,
                url,
                headers=headers,
        ) as response:
            self.assertEqual(200, response.status)
            # Download bytes and check integrity:
            self.assertEqual(
                self.test_file_md5,
                md5(await response.content.read()).digest(),
            )
            # Check that no redirects occurred:
            self.assertEqual(0, len(response.history))

        # Give the copy task enough time to finish:
        await sleep(1)
        # Check that the test video was put into the `dist_dir`:
        self.assertTrue(cmp(
            self.test_settings_storage.test_video_file_path,
            Path(dist_dir, rel_path(hashed_video_file_name)),
        ))

        #############################################
        # Get Storage redirect & download from Dist #

        # Perform another request to the storage node to trigger redirect:
        async with self.session.request(
                method,
                url,
                headers=headers,
        ) as response:
            self.assertEqual(200, response.status)
            # Check that one redirect occurred:
            self.assertEqual(1, len(response.history))
            self.assertEqual(302, response.history[0].status)
            # Check that last response came from distributor node:
            self.assertEqual("localhost", response.url.host)
            self.assertEqual(self.test_settings_dist.listen_port, response.url.port)
            # Download bytes and check integrity:
            self.assertEqual(
                self.test_file_md5,
                md5(await response.content.read()).digest(),
            )

        ##########
        # Delete #

        method = "DELETE"
        url = self.test_settings_storage.make_url("/api/file/" + hashed_video_file_name)
        jwt_data = DeleteFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.lms,
            is_allowed_to_delete_file=True
        )
        headers = self.get_auth_header(jwt_data.encode(key=key))
        # Perform DELETE request to the storage node:
        async with self.session.request(
                method,
                url,
                headers=headers,
        ) as response:
            self.assertEqual(200, response.status)
            self.assertDictEqual({"status": "ok"}, await response.json())
        # Give the deletion tasks a moment:
        await sleep(1)
        # Check that the file was deleted everywhere
        self.assertFalse(
            Path(perm_dir, rel_path(hashed_video_file_name)).exists()
        )
        self.assertFalse(
            Path(dist_dir, rel_path(hashed_video_file_name)).exists()
        )
        # Check the same for the thumbnails
        for name in thumbnail_file_names:
            self.assertFalse(Path(perm_dir, rel_path(name)).exists())

        # Perform another request to the storage node to get 404:
        method = "GET"
        jwt_data = self.get_request_file_jwt_data(data.hash, data.file_ext)
        request_file_token = jwt_data.encode(key=key)
        url = self.test_settings_storage.make_url("/file?jwt=" + request_file_token)
        headers = self.get_auth_header(request_file_token)
        async with self.session.request(
                method,
                url,
                headers=headers,
        ) as response:
            self.assertEqual(404, response.status)
