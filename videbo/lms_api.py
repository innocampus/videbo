from typing import List, Optional, Type, Tuple, Any

from videbo import storage_settings as settings
from videbo.exceptions import HTTPResponseError
from videbo.misc import ensure_url_does_not_end_with_slash
from videbo.models import JSONBaseModel
from videbo.web import HTTPClient


class LMSSitesCollection:
    def __init__(self) -> None:
        self.sites: List[MoodleAPI] = []

    @staticmethod
    def get_all() -> 'LMSSitesCollection':
        collection = LMSSitesCollection()
        for url in settings.lms_base_urls:
            if url:
                collection.sites.append(MoodleAPI(url))
        return collection


class MoodleAPI:
    def __init__(self, base_url: str) -> None:
        self.base_url = ensure_url_does_not_end_with_slash(base_url)
        self.api_url = self.base_url + "/mod/videoservice/api.php"

    async def _post_request(self, function: str, json_data: JSONBaseModel,
                            expected_return_type: Optional[Type[JSONBaseModel]] = None) -> Tuple[int, Any]:

        url = self.api_url + "?function=" + function
        jwt = HTTPClient.get_standard_jwt_with_role("node", external=True)
        return await HTTPClient.videbo_request("POST", url, jwt, json_data, expected_return_type, timeout=30,
                                               external=True)

    async def video_exists(self, hash: str, file_ext: str) -> bool:
        try:
            url = self.api_url + "?function=video_exists"
            params = VideoExistsParams(hash=hash, file_ext=file_ext)
            ret: VideoExistsResponse
            status, ret = await self._post_request("video_exists", params, VideoExistsResponse)
            if status == 200:
                return ret.exists
            else:
                raise HTTPResponseError()
        except HTTPResponseError:
            raise LMSAPIError()


class VideoExistsParams(JSONBaseModel):
    hash: str
    file_ext: str


class VideoExistsResponse(JSONBaseModel):
    exists: bool


class LMSAPIError(Exception):
    pass
