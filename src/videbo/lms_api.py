from __future__ import annotations
import logging
from collections.abc import Iterator
from typing import Any, Optional, Type, TYPE_CHECKING
from urllib.parse import urlencode

from videbo.exceptions import HTTPResponseError, LMSInterfaceError
from videbo.models import (BaseRequestModel, BaseResponseModel, LMSRequestJWTData,
                           VideoModel, VideosMissingRequest, VideosMissingResponse)
from videbo.web import HTTPClient
if TYPE_CHECKING:
    from videbo.storage.util import StoredHashedVideoFile as StoredFile


__all__ = ['LMS']

log = logging.getLogger(__name__)


class LMS:
    """Interface for sending requests to Learning Management Systems (LMS)"""
    FUNCTION_QUERY_PARAMETER = "function"
    VIDEOS_CHECK_MAX_BATCH_SIZE = 10_000

    _collection: dict[str, LMS] = {}

    @classmethod
    def add(cls, *urls: str) -> None:
        """Instantiates `LMS` objects with the provided `*urls`."""
        for url in urls:
            cls(url)

    @classmethod
    def iter_all(cls) -> Iterator[LMS]:
        """Returns an iterator over all `LMS` instances."""
        yield from iter(cls._collection.values())

    def __init__(self, api_url: str) -> None:
        """Adds the new instance to the class' internal collection."""
        self.api_url = api_url
        self.__class__._collection[api_url] = self

    async def _post_request(self, function: str, data: BaseRequestModel,
                            expected_return_type: Optional[Type[BaseResponseModel]] = None) -> tuple[int, Any]:
        """Sends a request with the provided `data` to the LMS' API with the specified `function`."""
        query = urlencode({self.FUNCTION_QUERY_PARAMETER: function})
        url = f"{self.api_url}?{query}"
        jwt = LMSRequestJWTData.get_standard_token()
        return await HTTPClient.videbo_request("POST", url, jwt, data, expected_return_type, timeout=30, external=True)

    async def videos_missing(self, *videos: VideoModel) -> VideosMissingResponse:
        """Checks if the provided videos are known to the LMS."""
        function = "videos_missing"
        request_data = VideosMissingRequest(videos=list(videos))
        response_data: VideosMissingResponse
        try:
            http_code, response_data = await self._post_request(function, request_data, VideosMissingResponse)
        except HTTPResponseError as e:
            raise LMSInterfaceError(
                f"Error trying to check video existence on {self.api_url}"
            ) from e
        if http_code != 200:
            raise LMSInterfaceError(
                f"Got response code {http_code} while "
                f"attempting to check video existence on {self.api_url}"
            )
        return response_data

    @classmethod
    async def filter_orphaned_videos(cls, *files: StoredFile, origin: Optional[str] = None) -> list[VideoModel]:
        """
        Checks LMS sites for their knowledge of the provided `*files`.

        Args:
            *files:
                Any number of `StoredHashedVideoFile` objects to check for their orphan status.
                They should all be files that are actually managed by the central `FileStorage`.
            origin (optional):
                If passed a string, any LMS with a matching URL is _not_ checked.

        Returns:
            List of `VideoModel` instances representing the files that are regarded as _orphaned_.
            If an `origin` was provided and that is the _only_ LMS that knows a file,
            that file will still be considered _orphaned_.

        Raises:
            `LMSInterfaceError` after logging it
        """
        video_batches: list[list[VideoModel]] = []
        for i in range(0, len(files), cls.VIDEOS_CHECK_MAX_BATCH_SIZE):
            indices = slice(i, i + cls.VIDEOS_CHECK_MAX_BATCH_SIZE)
            video_batches.append(
                [VideoModel.from_orm(file) for file in files[indices]]
            )
        for batch_idx, videos in enumerate(video_batches):
            for site in cls.iter_all():
                if origin and site.api_url.startswith(origin):
                    continue
                log.debug(f"Checking LMS {site.api_url} for {len(videos)} files.")
                try:
                    response = await site.videos_missing(*videos)
                except LMSInterfaceError as e:
                    log.warning(f"{e} occurred on {site.api_url}.")
                    raise
                # Continue only with videos unknown to previously checked LMS:
                videos = response.videos
            video_batches[batch_idx] = videos
        return [video for videos in video_batches for video in videos]
