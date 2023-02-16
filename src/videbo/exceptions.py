from collections.abc import Iterable as _Iterable
from typing import Optional as _Optional


class VideboBaseException(Exception):
    pass


class SubprocessError(VideboBaseException):
    """Base class for subprocess-related exceptions"""
    def __init__(
        self,
        timeout: bool = False,
        stderr: _Optional[str] = None,
    ) -> None:
        """If `stderr` is provided, the text is added to the error message."""
        self.timeout: bool = timeout
        self.stderr: _Optional[str] = stderr
        msg = f"{timeout=}"
        if stderr:
            msg += f"; stderr={stderr}"
        super().__init__(msg)


class FileCmdError(SubprocessError):
    pass


class FFMpegError(SubprocessError):
    pass


class FFProbeError(SubprocessError):
    pass


class VideoError(VideboBaseException):
    """Base class for video format/codec related exceptions"""
    pass


class VideoNotAllowed(VideoError):
    pass


class MimeTypeNotAllowed(VideoNotAllowed):
    def __init__(self, mime_type: str):
        self.mime_type = mime_type
        super().__init__(self.mime_type)


class ContainerFormatNotAllowed(VideoNotAllowed):
    def __init__(self, formats: _Iterable[str]) -> None:
        self.formats = list(formats)
        super().__init__(str(self.formats))


class CodecNotAllowed(VideoNotAllowed):
    def __init__(self, codec: _Optional[str]) -> None:
        self.codec = codec
        super().__init__(str(self.codec))


class VideoCodecNotAllowed(CodecNotAllowed):
    pass


class AudioCodecNotAllowed(CodecNotAllowed):
    pass


class FilesystemError(VideboBaseException):
    pass


class PendingWriteOperationError(FilesystemError):
    pass


class RoutingError(VideboBaseException):
    pass


class InvalidRouteSignature(RoutingError):
    pass


class HTTPClientError(VideboBaseException):
    pass


class AuthError(VideboBaseException):
    pass


class InvalidAuthData(AuthError):
    pass


class NotAuthorized(AuthError):
    pass


class NoRunningTask(VideboBaseException):
    pass


class NetworkInfoError(VideboBaseException):
    pass


class UnknownServerStatusFormatError(NetworkInfoError):
    pass


class LMSInterfaceError(VideboBaseException):
    pass


class SizeError(VideboBaseException):
    pass
