from typing import Any

from aiohttp.web_exceptions import HTTPConflict, HTTPNotFound

from videbo.exceptions import VideboBaseException


class HashedFileInvalidExtensionError(VideboBaseException):
    pass


class UploadError(VideboBaseException):
    pass


class FormFieldMissing(UploadError):
    pass


class BadFileExtension(UploadError):
    pass


class FileTooBigError(UploadError):
    pass


class DistributionError(VideboBaseException):
    pass


class DistStatusUnknown(DistributionError):
    pass


class UnknownDistURL(HTTPNotFound, DistributionError):
    def __init__(self, url: str, **kwargs: Any) -> None:
        kwargs.setdefault("text", f"Unknown distributor node `{url}`")
        HTTPNotFound.__init__(self, **kwargs)


class DistNodeAlreadySet(HTTPConflict, DistributionError):
    def __init__(self, url: str, enabled: bool, **kwargs: Any) -> None:
        prefix = "en" if enabled else "dis"
        kwargs.setdefault("text", f"Already {prefix}abled `{url}`")
        HTTPConflict.__init__(self, **kwargs)


class DistNodeAlreadyEnabled(DistNodeAlreadySet):
    def __init__(self, url: str, **kwargs: Any) -> None:
        super().__init__(url, True, **kwargs)


class DistNodeAlreadyDisabled(DistNodeAlreadySet):
    def __init__(self, url: str, **kwargs: Any) -> None:
        super().__init__(url, False, **kwargs)
