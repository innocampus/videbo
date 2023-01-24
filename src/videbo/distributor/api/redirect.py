from logging import Logger
from urllib.parse import quote, urlencode

from aiohttp.web_exceptions import HTTPFound
from aiohttp.web_request import Request

from videbo.auth import extract_jwt_from_request
from videbo.storage.distribution import DistributionNodeInfo
from videbo.storage.util import StoredHashedVideoFile


class RedirectToDistributor(HTTPFound):
    def __init__(
        self,
        request: Request,
        node: DistributionNodeInfo,
        file: StoredHashedVideoFile,
        log: Logger,
    ) -> None:
        """
        Constructs the URL to redirect to and initializes with that.

        Can be called from a storage route to redirect to the specified
        distributor node for the specified file.

        Args:
            request:
                The `aiohttp.web_request.Request` instance; if the query has
                the `downloadas` parameter it is propagated in the redirect;
                the JWT needed for the url parameter is extracted from it.
            node:
                The `DistributionNodeInfo` object representing the distributor
                node; the file request path is appended to it, the `jwt` query
                parameter is added with the encoded JWT value extracted from
                the request, and if `downloadas` is present, that parameter is
                added as well.
            file:
                The `StoredHashedVideoFile` object representing the file;
                needed for logging purposes only
            log:
                The `Logger` object to use for logging the redirect
        """
        log.debug(f"Redirect user to {node.base_url} for video {file}")
        query_data = {"jwt": extract_jwt_from_request(request)}
        download_as = request.query.getone("downloadas", None)
        if download_as:
            query_data["downloadas"] = quote(download_as)
        super().__init__(f"{node.base_url}/file?{urlencode(query_data)}")
