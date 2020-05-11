from typing import List
from .web import ensure_url_does_not_end_with_slash, HTTPClient, JSONBaseModel, HTTPResponseError
from . import settings


class LMSSitesCollection:
    def __init__(self):
        self.sites: List[MoodleAPI] = []

    @staticmethod
    def get_all() -> "LMSSitesCollection":
        collection = LMSSitesCollection()
        urls = settings.lms.moodle_base_urls
        for url in map(str.strip, urls.split(',')):
            collection.sites.append(MoodleAPI(url))

        return collection


class MoodleAPI:
    def __init__(self, base_url: str):
        self.base_url = ensure_url_does_not_end_with_slash(base_url)
        self.api_url = self.base_url + "/mod/videoservice/api.php"

    async def video_exists(self, hash: str, file_ext: str) -> bool:
        try:
            url = self.api_url + "?function=video_exists"
            params = VideoExistsParams(hash=hash, file_ext=file_ext)
            ret: VideoExistsResponse
            status, ret = await HTTPClient.internal_request_node("POST", url, params, VideoExistsResponse)
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