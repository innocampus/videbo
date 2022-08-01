from typing import Optional


class VideboBaseException(Exception):
    pass


class SubprocessError(VideboBaseException):
    def __init__(self, timeout: bool = False, stderr: Optional[str] = None) -> None:
        self.timeout: bool = timeout
        self.stderr: Optional[str] = stderr
        super().__init__()


class FileCmdError(SubprocessError):
    pass


class FFMpegError(SubprocessError):
    pass


class FFProbeError(SubprocessError):
    pass


class VideoError(VideboBaseException):
    pass


class InvalidMimeTypeError(VideoError):
    def __init__(self, mimetype: str):
        self.mime_type = mimetype
        super().__init__()


class InvalidVideoError(VideoError):
    def __init__(self, container: str = '', video_codec: str = '', audio_codec: str = '') -> None:
        self.container = container
        self.video_codec = video_codec
        self.audio_codec = audio_codec
        super().__init__()


class FilesystemError(VideboBaseException):
    pass


class PendingWriteOperationError(FilesystemError):
    pass


class CouldNotCreateDir(FilesystemError):
    pass


class RoutingError(VideboBaseException):
    pass


class InvalidRouteSignature(RoutingError):
    pass


class HTTPResponseError(RoutingError):
    pass


class AuthError(VideboBaseException):
    pass


class InvalidRoleIssued(AuthError):
    pass


class InvalidIssuerClaim(AuthError):
    pass


class NoJWTFound(AuthError):
    pass


class NoRunningTask(VideboBaseException):
    pass
