class FileCmdError(Exception):
    def __init__(self, timeout):
        self.timeout = timeout


class FFProbeError(Exception):
    def __init__(self, timeout, stderr=None):
        self.timeout = timeout
        self.stderr = stderr


class FFMpegError(Exception):
    def __init__(self, timeout, stderr=None):
        self.timeout = timeout
        self.stderr = stderr


class FileDoesNotExistError(Exception):
    pass


class InvalidMimeTypeError(Exception):
    def __init__(self, mimetype: str):
        self.mime_type = mimetype


class InvalidVideoError(Exception):
    def __init__(self, container: str = "", video_codec: str = "", audio_codec: str = ""):
        self.container = container
        self.video_codec = video_codec
        self.audio_codec = audio_codec


class CouldNotCreateDir(Exception):
    pass


class CouldNotCreateTempDir(CouldNotCreateDir):
    pass


class CouldNotCreateTempOutDir(CouldNotCreateTempDir):
    pass


class PendingWriteOperationError(Exception):
    pass


class HashedFileInvalidExtensionError(Exception):
    pass


class NoValidFileInRequestError(Exception):
    pass


class FileTooBigError(Exception):
    pass
