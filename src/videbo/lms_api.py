from __future__ import annotations
from collections.abc import Iterator
from typing import Any, Optional, Type
from urllib.parse import urlencode

from videbo.exceptions import HTTPResponseError, LMSInterfaceError
from videbo.models import JSONBaseModel, LMSRequestJWTData, VideoExistsRequest, VideoExistsResponse
from videbo.web import HTTPClient


__all__ = ['LMS']


class LMS:
    FUNCTION_QUERY_PARAMETER = "function"

    _collection: dict[str, LMS] = {}

    @classmethod
    def add(cls, *urls: str) -> None:
        for url in urls:
            cls(url)

    @classmethod
    def iter_all(cls) -> Iterator[LMS]:
        yield from iter(cls._collection.values())

    def __init__(self, api_url: str) -> None:
        self.api_url = api_url
        self.__class__._collection[api_url] = self

    async def _post_request(self, function: str, json_data: JSONBaseModel,
                            expected_return_type: Optional[Type[JSONBaseModel]] = None) -> tuple[int, Any]:
        query = urlencode({self.FUNCTION_QUERY_PARAMETER: function})
        url = f"{self.api_url}?{query}"
        jwt = LMSRequestJWTData.get_standard_token()
        return await HTTPClient.videbo_request("POST", url, jwt, json_data, expected_return_type, timeout=30,
                                               external=True)

    async def video_exists(self, file_hash: str, file_ext: str) -> bool:
        function = "video_exists"
        request_data = VideoExistsRequest(hash=file_hash, file_ext=file_ext)
        response_data: VideoExistsResponse
        try:
            http_code, response_data = await self._post_request(function, request_data, VideoExistsResponse)
        except HTTPResponseError as e:
            raise LMSInterfaceError(
                f"Error trying to check video existence on {self.api_url}"
            ) from e
        if http_code != 200:
            raise LMSInterfaceError(
                f"Got response code {http_code} while "
                f"attempting to check video existence on {self.api_url}"
            )
        return response_data.exists
