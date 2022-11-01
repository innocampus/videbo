import sys
import warnings
from pathlib import Path
from shutil import rmtree
from typing import Optional

from aiohttp.test_utils import AioHTTPTestCase
from aiohttp.web import Application

from tests.silent_log import SilentLogMixin
from videbo import settings
from videbo.misc import MEGA
from videbo.models import Role, TokenIssuer
from videbo.storage.api.models import FileType, RequestFileJWTData
from videbo.storage.api.routes import get_expiration_time, routes
from videbo.web import get_application


AUTHORIZATION, BEARER_PREFIX = 'Authorization', 'Bearer '

test_vid_path = settings.test_video_file_path.resolve()


class BaseE2ETestCase(SilentLogMixin, AioHTTPTestCase):
    THUMBNAIL_EXT = 'jpg'

    test_vid_exists: bool = test_vid_path.is_file()
    test_vid_size_mb: Optional[float]
    test_vid_file_ext: Optional[str]
    SKIP_REASON_NO_TEST_VID: str = f"Test video '{test_vid_path}' not found"

    @staticmethod
    def _safe_rmtree(dir_path: Path) -> None:
        if any(p.is_file() for p in dir_path.glob("**/*")):
            warnings.warn(
                f"Test case is finished, but files were found inside "
                f"the directory '{dir_path}'; not deleting",
                category=RuntimeWarning,
            )
        else:
            rmtree(dir_path)

    @classmethod
    def setUpClass(cls) -> None:
        if not sys.warnoptions:
            warnings.simplefilter("always")

        cls.test_vid_size_mb = (
            test_vid_path.stat().st_size / MEGA
            if cls.test_vid_exists
            else None
        )
        cls.test_vid_file_ext = (
            test_vid_path.suffix[1:]
            if cls.test_vid_exists
            else None
        )

        settings.files_path.mkdir(parents=True, exist_ok=True)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._safe_rmtree(settings.files_path)
        super().tearDownClass()

    @staticmethod
    def get_request_file_jwt_data(file_hash: str, file_ext: str) -> RequestFileJWTData:
        return RequestFileJWTData(
            exp=get_expiration_time(),
            iss=TokenIssuer.external,
            role=Role.client,
            type=FileType.VIDEO,
            hash=file_hash,
            file_ext=file_ext,
            rid='1'
        )

    @staticmethod
    def get_auth_header(token: str) -> dict[str, str]:
        return {AUTHORIZATION: BEARER_PREFIX + token}

    async def get_application(self) -> Application:
        app = get_application()
        app.add_routes(routes)
        return app

    def all_tests_passed(self) -> bool:
        """
        Returns `True` if no errors/failures occurred at the time of calling.

        Source: https://stackoverflow.com/a/39606065/19770795
        """
        outcome = getattr(self, "_outcome")
        if hasattr(outcome, "errors"):
            # <=3.10
            result = self.defaultTestResult()
            getattr(self, "_feedErrorsToResult")(result, outcome.errors)
        else:
            # Python >=3.11
            result = outcome.result
        return all(test != self for test, _ in result.errors + result.failures)
